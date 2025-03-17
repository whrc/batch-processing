import gcsfs
import xarray as xr
import glob
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
        self.result_dir.mkdir(parents=True, exist_ok=True)

    def _merge_small_dataset(self, output_file, output_path):
        path = self.base_batch_dir / "batch_*" / "output" / output_file
        files = sorted(glob.glob(path.as_posix()), key=get_batch_number)
        print(f"Reading {output_file}")
        ds = xr.open_mfdataset(files, engine="h5netcdf", combine="nested", concat_dim="y", data_vars="minimal", coords="minimal", compat="override", decode_cf=False, decode_times=False)
        ds.to_netcdf(f"{output_path}/{output_file}")

    def _merge(self, output_file, bucket_path):
        fs = get_gcsfs()
        path = self.base_batch_dir / "batch_*" / "output" / output_file
        files = sorted(glob.glob(path.as_posix()), key=get_batch_number)
        print(f"Reading {output_file}")
        ds = xr.open_mfdataset(files, engine="h5netcdf", combine="nested", concat_dim="y", parallel=True, data_vars="minimal", coords="minimal", compat="override", decode_cf=False, decode_times=False)
        if output_file == "run_status.nc" or output_file.startswith("restart"):
            ds = ds.chunk({"X": "auto", "Y": "auto"})
        else:
            ds = ds.chunk({"time": -1, "x": "auto", "y": "auto"})

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
        file_path = self.base_batch_dir / "batch_0" / "output" / "run_status.nc"
        print(f"File path: {file_path}")
        x, y = get_dimensions(file_path)
        total_batch_count = len([p for p in self.base_batch_dir.iterdir() if "batch_" in p.as_posix()])
        print("Total batch count is ", total_batch_count)

        assert y == 1
        y *= total_batch_count
        total_cell_count = x * y

        if total_cell_count < self.__MIN_CELL_COUNT_FOR_DASK:
            path = self.base_batch_dir / "batch_0" / "output"
            output_files = [f.name for f in path.iterdir()]
            for f in output_files:
                self._merge_small_dataset(f, self.result_dir)
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
