import gcsfs
import xarray as xr
import glob
from pathlib import Path
import numpy as np
from dask.distributed import Client

from batch_processing.cmd.base import BaseCommand
from batch_processing.cmd.batch.check import BatchCheckCommand
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
        concat_dim = "y"
        if output_file.startswith("restart") or output_file == "run_status.nc":
            concat_dim = "Y"

        print(f"Reading {output_file}")
        ds = xr.open_mfdataset(files, engine="h5netcdf", combine="nested", concat_dim=concat_dim, data_vars="minimal", coords="minimal", compat="override", decode_cf=False, decode_times=False)
        ds.to_netcdf(f"{output_path}/{output_file}")

    def _merge(self, output_file, bucket_path):
        fs = get_gcsfs()
        path = self.base_batch_dir / "batch_*" / "output" / output_file
        files = sorted(glob.glob(path.as_posix()), key=get_batch_number)
        print(f"Reading {output_file}")

        concat_dim = "y"
        chunk_size = {"time": -1, "x": "auto", "y": "auto"}
        if output_file.startswith("restart") or output_file == "run_status.nc":
            concat_dim = "Y"
            chunk_size = {"X": "auto", "Y": "auto"}
        ds = xr.open_mfdataset(files, engine="h5netcdf", combine="nested", concat_dim=concat_dim, parallel=True, data_vars="minimal", coords="minimal", compat="override", decode_cf=False, decode_times=False)
        ds = ds.chunk(chunk_size)

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

    def _check_status(self):
        run_status_file_pattern = f"{self.base_batch_dir.as_posix()}/batch_*/output/run_status.nc"

        ds = xr.open_mfdataset(run_status_file_pattern, engine="h5netcdf", concat_dim="Y", combine="nested")
        status_unique, status_counts = np.unique(ds.run_status.values, return_counts=True)
        merged = dict(zip(status_unique, status_counts))

        if len(status_unique) and status_unique[0] == 100:
            print("All status codes are 100! Continuing to merge")
            return True
        else:
            print("status code : count")
            print(merged)
            while True:
                choice = input("Status codes different than 100 were found. Do you want to continue merging (y/n) ? ")
                choice = choice.lower()
                if choice in ['y', 'n']:
                    return choice == 'y'
                print("Please enter 'y' or 'n'.")

    def execute(self):
        internal_check_command = BatchCheckCommand(self._args)
        internal_check_command.execute()

        if not self._check_status():
            print("Cancelled.")
            return

        file_path = self.base_batch_dir / "batch_0" / "output" / "run_status.nc"
        print(f"File path: {file_path}")
        x, y = get_dimensions(file_path)
        total_batch_count = len([p for p in self.base_batch_dir.iterdir() if "batch_" in p.as_posix()])
        print("Total batch count is", total_batch_count)

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

        # print average cell run time
        run_status_file = self.result_dir / "run_status.nc"
        if not run_status_file.exists():
            print(f"Couldn't find {run_status_file.as_posix()}. Aborting!")
            return

        ds = xr.open_dataset(run_status_file.as_posix(), engine="h5netcdf")

        # convert timedelta64 to seconds
        runtimes_in_seconds = [td / np.timedelta64(1, 's') for td in ds.total_runtime]
        average_cell_runtime = np.nanmean(runtimes_in_seconds)
        print(f"The average cell run time is {average_cell_runtime} seconds ({round(average_cell_runtime / 60, 2)} min)")
