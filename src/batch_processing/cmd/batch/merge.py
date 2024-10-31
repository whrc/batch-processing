import shutil
import subprocess
import gcsfs
import xarray as xr
import glob
from collections import defaultdict
from pathlib import Path
from dask.distributed import Client

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import get_batch_number, get_dimensions, get_gcsfs, get_cluster


class BatchMergeCommand(BaseCommand):
    __MIN_CELL_COUNT_FOR_DASK = 40_000

    def __init__(self, args):
        super().__init__()
        self._args = args
        self.base_batch_dir = Path(self.exacloud_user_dir, args.batches)
        self.result_dir = self.base_batch_dir / "all_merged"

    def _merge_with_nco(self):
        batch_output_dirs = self.base_batch_dir.glob("*/output")

        # keys hold the file names
        # values hold the paths of that files
        grouped_files = defaultdict(list)

        for batch_dir in batch_output_dirs:
            for file in batch_dir.iterdir():
                grouped_files[file.name].append(file)

        print("sorting the files")
        for file_name in grouped_files:
            grouped_files[file_name].sort(key=get_batch_number)

        print("renaming and copying the files")
        for file_name in grouped_files:
            all_files = grouped_files[file_name]
            new_files = []
            for index, file in enumerate(all_files):
                temp_file = Path(file.parent / f"{file.stem}_{index}.nc")
                file_as_str = file.as_posix()
                if any(map(file_as_str.__contains__, ["restart", "run_status"])):
                    shutil.copy(file, temp_file)
                else:
                    subprocess.run(
                        [
                            "ncrename",
                            "-O",
                            "-h",
                            "-d",
                            "x,X",
                            "-d",
                            "y,Y",
                            file_as_str,
                            temp_file,
                        ]
                    )

                new_files.append(temp_file)

            grouped_files[file_name] = new_files

        for file_name in grouped_files:
            all_files = grouped_files[file_name]
            for file in all_files:
                subprocess.run(
                    [
                        "ncks",
                        "-O",
                        "-h",
                        "--mk_rec_dmn",
                        "Y",
                        file,
                        file,
                    ]
                )

        print("ncap2")
        for file_name in grouped_files:
            all_files = grouped_files[file_name]
            for index, file in enumerate(all_files):
                try:
                    _ = subprocess.run(
                        [
                            "ncap2",
                            "-O",
                            "-h",
                            "-s",
                            f"Y[$Y]={index}; X[$X]=array(0, 1, $X);",
                            file.as_posix(),
                            file.as_posix(),
                        ],
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                except subprocess.CalledProcessError as e:
                    print(f"Error occurred: {e}")
                    print(f"Output: {e.stdout}")
                    print(f"Error output: {e.stderr}")

        self.result_dir.mkdir(exist_ok=True)
        print("concatenating the files")
        for file_name in grouped_files:
            all_files = grouped_files[file_name]
            subprocess.run(
                [
                    "ncrcat",
                    "-O",
                    "-h",
                    *all_files,
                    self.result_dir / file_name,
                ]
            )
            # remove the intermediary files
            _ = [file.unlink() for file in all_files]

    # it took 50 minutes to merge this dataset
    def _merge(self, output_file, bucket_path):
        fs = get_gcsfs()
        path = self.base_batch_dir / "batch_*" / "output" / output_file
        # path = f"{self.base_batch_dir}/batch_*/output/{output_file}"
        files = sorted(glob.glob(path.as_posix()), key=get_batch_number)
        print(f"Reading {output_file}")
        ds = xr.open_mfdataset(files, engine="h5netcdf", combine="nested", concat_dim="y", parallel=True, data_vars="minimal", coords="minimal", compat="override", decode_cf=False, decode_times=False)
        if output_file == "run_status.nc" or output_file.startswith("restart"):
            ds = ds.chunk({"X": "auto", "Y": "auto"})
        else:
            ds = ds.chunk({"time": -1, "x": "auto", "y": "auto"})

        # gcsmap = gcsfs.mapping.GCSMap(f"gcp-slurm/reference_data/iem/merged/{output_file[:len(output_file)-3]}.zarr", gcs=fs, check=False, create=True)
        gcsmap = gcsfs.mapping.GCSMap(f"{bucket_path}/{output_file[:len(output_file)-3]}.zarr", gcs=fs, check=False, create=True)
        delayed_obj = ds.to_zarr(gcsmap, mode="w", compute=False)
        return delayed_obj

    def _merge_with_dask(self, bucket_path):
        cluster = get_cluster(n_workers=10)
        client = Client(cluster)
        client.wait_for_workers(5)
        print(f"Dashboard link: {client.dashboard_link}")

        path = self.base_batch_dir / "batch_0" / "output"
        # path = Path("/mnt/exacloud/dteber_woodwellclimate_org/full-iem/batch_0/output")
        output_files = [f.name for f in path.iterdir()]

        for f in output_files:
            obj = self._merge(f, bucket_path)
            print(f"Computing {f}")
            obj.compute()

        cluster.close()

    def execute(self):
        import ipdb

        ipdb.set_trace()
        file_path = self.base_batch_dir / "batch_0" / "output" / "run_status.nc"
        print(f"File path: {file_path}")
        x, y = get_dimensions(file_path)
        total_batch_count = len([p for p in self.base_batch_dir.iterdir() if "batch_" in p.as_posix()])

        assert y == 1
        y *= total_batch_count
        total_cell_count = x * y

        if total_cell_count < self.__MIN_CELL_COUNT_FOR_DASK:
            self._merge_with_nco()
        else:
            if self._args.bucket_path == "":
                raise ValueError(
                    f"--bucket-path is required for this dataset which has {total_cell_count}"
                )

            fs = get_gcsfs()
            bucket_name = f"{Path(self._args.bucket_path).parts[0]}/"
            if bucket_name not in fs.buckets:
                raise ValueError(
                    f"There is no bucket named {bucket_name}"
                )

            self._merge_with_dask(self._args.bucket_path)
