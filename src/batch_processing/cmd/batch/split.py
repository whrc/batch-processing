import dask
import os
import re
import shutil
import subprocess
import dask.distributed
import xarray as xr
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
from pathlib import Path
from typing import List

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import (
    create_slurm_script,
    interpret_path,
    update_config,
    get_gcsfs,
    get_cluster,
)

# todo: this list doesn't include co2.nc and projected-co2.c files
# give a better name and refactor
INPUT_FILES = [
    "drainage.nc",
    "fri-fire.nc",
    "run-mask.nc",
    "soil-texture.nc",
    "topo.nc",
    "vegetation.nc",
    "historic-explicit-fire.nc",
    "projected-explicit-fire.nc",
    "projected-climate.nc",
    "historic-climate.nc",
]
INPUT_FILES_TO_SPLIT = [
    "drainage.zarr",
    "fri-fire.zarr",
    "run-mask.zarr",
    "soil-texture.zarr",
    "topo.zarr",
    "vegetation.zarr",
    "historic-explicit-fire.zarr",
    "projected-explicit-fire.zarr",
    "projected-climate.zarr",
    "historic-climate.zarr",
]
BATCH_DIRS: List[Path] = []
BATCH_INPUT_DIRS: List[Path] = []
SETUP_SCRIPTS_PATH = os.path.join("/opt/apps", "dvm-dos-tem/scripts/util")


class BatchSplitCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        # todo: remove self._args and create class variables for every argument
        self._args = args
        self.base_batch_dir = Path(self.exacloud_user_dir, args.batches)
        self.log_path = Path(self.base_batch_dir, "logs")

        self.log_path.mkdir(exist_ok=True, parents=True)

        self.input_path = args.input_path

    def _run_utils(self, batch_dir, batch_input_dir):
        # todo: instead of running this file, implement what this file does
        # inside bp.
        # later, delete the last portion of the execute() code which removes
        # duplicated input files.
        # doing that should save us some time.
        subprocess.run(
            [
                os.path.join(SETUP_SCRIPTS_PATH, "setup_working_directory.py"),
                batch_dir,
                "--input-data-path",
                batch_input_dir,
                "--copy-inputs",
            ]
        )

    def _configure(self, index: int, batch_dir: Path) -> None:
        config_file = batch_dir / "config" / "config.js"
        update_config(path=config_file.as_posix(), prefix_value=batch_dir)

        substitution_values = {
            "job_name": f"{self.base_batch_dir.name}-batch-{index}",
            "partition": self._args.slurm_partition,
            "dvmdostem_binary": self.dvmdostem_bin_path,
            "log_file_path": self.log_path / f"batch-{index}",
            "log_level": self._args.log_level,
            "config_path": config_file,
            "p": self._args.p,
            "e": self._args.e,
            "s": self._args.s,
            "t": self._args.t,
            "n": self._args.n,
        }

        script_path = batch_dir / "slurm_runner.sh"
        create_slurm_script(
            script_path.as_posix(), "slurm_runner.sh", substitution_values
        )

    def _split_with_nco(
        self, start_index: int, end_index: int, input_path: Path, split_dimension: str
    ) -> None:
        for input_file in INPUT_FILES:
            src_input_path = input_path / input_file
            print("splitting ", src_input_path)
            for index in range(start_index, end_index):
                path = os.path.join(BATCH_INPUT_DIRS[index], input_file)
                subprocess.run(
                    [
                        "ncks",
                        "-O",
                        "-h",
                        "-d",
                        f"{split_dimension},{index}",
                        src_input_path,
                        path,
                    ]
                )
            print("done splitting ", input_file)

    def _split_with_dask(self, bucket_path):
        cluster = get_cluster(n_workers=100)
        client = dask.distributed.Client(cluster)
        client.wait_for_workers(50)
        print(f"Dashboard link: {client.dashboard_link}")
        fs = get_gcsfs()
        for input_file in INPUT_FILES_TO_SPLIT:
            print(f"Processing {input_file}")
            bucket_mapping = fs.get_mapper(
                os.path.join(bucket_path, input_file), check=True
            )
            ds = xr.open_zarr(bucket_mapping, decode_times=False)
            if input_file in [
                "historic-climate.zarr",
                "historic-explicit-fire.zarr",
                "projected-climate.zarr",
                "projected-explicit-fire.zarr",
            ]:
                chunk_dict = {"Y": 1, "X": -1, "time": -1}
            else:
                chunk_dict = {"Y": 1, "X": -1}

            ds = ds.chunk(chunk_dict)
            y_dim = ds.Y.size

            # I know this is ugly but passing `ds` as an argument makes things painfully slow
            @dask.delayed
            def _process_data(col_index, output_path):
                subset = ds.isel({"Y": col_index}).expand_dims("Y")
                obj = subset.to_netcdf(output_path, engine="h5netcdf")
                return obj

            delayed_objs = [
                _process_data(
                    i,
                    os.path.join(
                        self.base_batch_dir,
                        f"batch_{i}",
                        "input",
                        f"{input_file[:len(input_file)-5]}.nc",
                    ),
                )
                for i in range(y_dim)
            ]
            batch_size = 125
            for i in range(0, y_dim, batch_size):
                print(f"Computing batch number {(i // batch_size) + 1}")
                batch = delayed_objs[i : i + batch_size]
                dask.compute(*batch)

            ds.close()

        cluster.close()

    def execute(self):
        reading_remote_data = False
        if self.input_path.startswith("gcs://"):
            self.input_path = self.input_path.replace("gcs://", "")
            reading_remote_data = True
        else:
            self.input_path = Path(interpret_path(self.input_path))

        fs = get_gcsfs()

        if reading_remote_data:
            path = fs.get_mapper(
                os.path.join(self.input_path, "run-mask.zarr"), check=True
            )
            ds = xr.open_zarr(path)
        else:
            ds = xr.open_dataset(self.input_path / "run-mask.nc", engine="h5netcdf")

        X, Y = ds.X.size, ds.Y.size
        print("Dimension size of X:", X)
        print("Dimension size of Y:", Y)

        # always split across y dimension
        SPLIT_DIMENSION, DIMENSION_SIZE = "Y", Y

        print(f"\nSplitting accros {SPLIT_DIMENSION} dimension")
        print("Dimension size:", DIMENSION_SIZE)

        ds.close()

        print("Cleaning up the existing directories")
        if self.base_batch_dir.exists():
            pattern = re.compile(r"^batch_\d+$")
            to_be_removed = [
                d
                for d in self.base_batch_dir.iterdir()
                if d.is_dir() and pattern.match(d.name)
            ]

            with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
                executor.map(lambda elem: shutil.rmtree(elem), to_be_removed)

        print("Set up batch directories")
        self.base_batch_dir.mkdir(exist_ok=True)
        self.log_path.mkdir(exist_ok=True)
        for index in range(DIMENSION_SIZE):
            path = self.base_batch_dir / f"batch_{index}"
            BATCH_DIRS.append(path)

            path = path / "input"
            BATCH_INPUT_DIRS.append(path)

        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
            executor.map(lambda elem: os.makedirs(elem), BATCH_INPUT_DIRS)

        co2_files = ["co2.zarr/", "projected-co2.zarr/"]
        if reading_remote_data:
            for co2_file in co2_files:
                src = os.path.join(self.input_path, co2_file)
                dst = os.path.join(self.base_batch_dir, co2_file)
                fs.get(src, dst, recursive=True)

                ds = xr.open_zarr(dst)
                co2_file = Path(co2_file)
                ds.to_netcdf(
                    os.path.join(self.base_batch_dir, f"{co2_file.stem}.nc"),
                    engine="h5netcdf",
                )
                ds.close()
                shutil.rmtree(dst)

        # co2.nc and projected-co2.nc doesn't have X and Y dimensions. So, we copy
        # them instead of splitting.
        print("Copy co2.nc and projected-co2.nc files")
        co2_dest = self.input_path
        if reading_remote_data:
            co2_dest = self.base_batch_dir

        for batch_dir in BATCH_INPUT_DIRS:
            src_co2 = co2_dest / "co2.nc"
            dst_co2 = batch_dir / "co2.nc"
            shutil.copy(src_co2, dst_co2)

            src_projected_co2 = co2_dest / "projected-co2.nc"
            dst_projected_co2 = batch_dir / "projected-co2.nc"
            shutil.copy(src_projected_co2, dst_projected_co2)

        if reading_remote_data:
            os.remove(os.path.join(co2_dest, "co2.nc"))
            os.remove(os.path.join(co2_dest, "projected-co2.nc"))

        print("Split input files")
        if reading_remote_data:
            self._split_with_dask(self.input_path)
        else:
            self._split_with_nco(0, DIMENSION_SIZE, self.input_path, SPLIT_DIMENSION)

        print("Set up the batch simulation")
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
            executor.map(
                lambda args: self._run_utils(*args), zip(BATCH_DIRS, BATCH_INPUT_DIRS)
            )

        print("Configure each batch")
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
            executor.map(lambda args: self._configure(*args), enumerate(BATCH_DIRS))

        # we have to do this otherwise there would be two inputs folders:
        # input/ and inputs/
        #
        # inputs/ folder is created because we are calling setup_working_directory.py
        print("Delete duplicated inputs files")
        duplicated_input_paths = self.base_batch_dir.glob("*/inputs")
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
            executor.map(lambda elem: shutil.rmtree(elem), duplicated_input_paths)
