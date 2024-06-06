import json
import os
import re
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from itertools import product
from multiprocessing import Pool
from string import Template

import netCDF4

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import (
    clean_and_load_json,
    create_chunks,
    get_project_root,
)

OUTPUT_DIR = "/mnt/exacloud/dteber_woodwellclimate_org/output/"
BATCH_DIRS = []
BATCH_INPUT_DIRS = []
INPUT_FILES = [
    "co2.nc",
    "projected-co2.nc",
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
INPUT_DIR = "/mnt/exacloud/dteber_woodwellclimate_org/sliced-inputs-six"
SETUP_SCRIPTS_PATH = os.path.join(os.environ["HOME"], "dvm-dos-tem/scripts/util")


class BatchNewSplitCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args

    def run_utils(self, batch_dir, batch_input_dir):
        subprocess.run(
            [
                os.path.join(SETUP_SCRIPTS_PATH, "setup_working_directory.py"),
                batch_dir,
                "--input-data-path",
                batch_input_dir,
                "--copy-inputs",
            ]
        )

    def configure(self, index, batch_dir):
        config_file = os.path.join(batch_dir, "config/config.js")
        with open(config_file) as f:
            config_data = json.load(f)

        config_data["IO"]["parameter_dir"] = f"{batch_dir}/parameters/"
        config_data["IO"]["output_dir"] = f"{batch_dir}/output/"
        config_data["IO"]["output_spec_file"] = f"{batch_dir}/config/output_spec.csv"
        config_data["IO"]["runmask_file"] = f"{batch_dir}/input/run-mask.nc"

        config_data["IO"][
            "hist_climate_file"
        ] = f"{batch_dir}/input/historic-climate.nc"
        config_data["IO"][
            "proj_climate_file"
        ] = f"{batch_dir}/input/projected-climate.nc"
        config_data["IO"]["veg_class_file"] = f"{batch_dir}/input/vegetation.nc"
        config_data["IO"]["drainage_file"] = f"{batch_dir}/input/drainage.nc"
        config_data["IO"]["soil_texture_file"] = f"{batch_dir}/input/soil-texture.nc"
        config_data["IO"]["co2_file"] = f"{batch_dir}/input/co2.nc"
        config_data["IO"]["proj_co2_file"] = f"{batch_dir}/input/projected-co2.nc"
        config_data["IO"]["topo_file"] = f"{batch_dir}/input/topo.nc"
        config_data["IO"]["fri_fire_file"] = f"{batch_dir}/input/fri-fire.nc"
        config_data["IO"][
            "hist_exp_fire_file"
        ] = f"{batch_dir}/input/historic-explicit-fire.nc"
        config_data["IO"][
            "proj_exp_fire_file"
        ] = f"{batch_dir}/input/projected-explicit-fire.nc"

        with open(config_file, "w") as f:
            json.dump(config_data, f, indent=4)

        with open(f"{get_project_root()}/templates/slurm_runner.sh") as file:
            template = Template(file.read())

        slurm_runner = template.substitute(
            {
                "index": index,
                "partition": self._args.slurm_partition,
                "user": self.user,
                "dvmdostem_binary": self.dvmdostem_bin_path,
                "log_level": self._args.log_level,
                "config_path": config_file,
                "p": self._args.p,
                "e": self._args.e,
                "s": self._args.s,
                "t": self._args.t,
                "n": self._args.n,
            }
        )

        with open(f"{batch_dir}/slurm_runner.sh", "w") as file:
            file.write(slurm_runner)

    def remove_directory(self, batch_dir):
        shutil.rmtree(batch_dir)

    def execute(self):
        with open(self.config_path) as f:
            configuration = f.read()

        configuration = clean_and_load_json(configuration)
        runmask_file_path = configuration["IO"]["runmask_file"]
        with netCDF4.Dataset(runmask_file_path, "r") as dataset:
            X = dataset.dimensions["X"].size
            Y = dataset.dimensions["Y"].size

        print("Dimension size of X:", X)
        print("Dimension size of Y:", Y)

        # Choose the dimension to split along
        SPLIT_DIMENSION, DIMENSION_SIZE = ("X", X) if Y > X else ("Y", Y)

        print(f"\nSplitting accros {SPLIT_DIMENSION} dimension")
        print("Dimension size:", DIMENSION_SIZE)

        print("Cleaning up the existing directories")
        pattern = re.compile(r"^batch_\d+$")
        batch_directories = [
            os.path.join(self.output_dir, d)
            for d in os.listdir(self.output_dir)
            if os.path.isdir(os.path.join(self.output_dir, d)) and pattern.match(d)
        ]

        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
            executor.map(self.remove_directory, batch_directories)

        print("Set up batch directories")
        os.makedirs(self.output_dir, exist_ok=True)
        for index in range(DIMENSION_SIZE):
            path = os.path.join(self.output_dir, f"batch_{index}")
            BATCH_DIRS.append(path)

            path = os.path.join(path, "input")
            BATCH_INPUT_DIRS.append(path)

        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
            _ = [
                executor.submit(
                    lambda batch_input_dir=batch_input_dir: os.makedirs(batch_input_dir)
                )
                for batch_input_dir in BATCH_INPUT_DIRS
            ]

        print("Split input files")
        tasks = []
        chunk_size = 0
        # Iterate over sliced input directories and input files
        for sliced_dir, input_file in product(os.listdir(INPUT_DIR), INPUT_FILES):
            input_file_path = os.path.join(INPUT_DIR, sliced_dir, input_file)
            if chunk_size == 0:
                chunk_range = sliced_dir.split("-")[-1]
                start, end = (int(val) for val in chunk_range.split("_"))
                chunk_size = end - start
                print("chunk_size is", chunk_size)

            # Create tasks for each combination of input file and chunk
            for start_index, end_index in create_chunks(chunk_size, os.cpu_count()):
                tasks.append(
                    (
                        start_index,
                        end_index,
                        input_file_path,
                        input_file,
                        SPLIT_DIMENSION,
                    )
                )

        with Pool(processes=os.cpu_count()) as pool:
            pool.starmap(split_file_chunk, tasks)

        print("Set up the batch simulation")
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
            _ = [
                executor.submit(self.run_utils, batch_dir, batch_input_dir)
                for batch_dir, batch_input_dir in zip(BATCH_DIRS, BATCH_INPUT_DIRS)
            ]

        print("Configure each batch")
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
            _ = [
                executor.submit(self.configure, index, batch_dir)
                for index, batch_dir in enumerate(BATCH_DIRS)
            ]


def split_file_chunk(start_index, end_index, input_path, input_file, split_dimension):
    print("splitting ", input_path)
    for index in range(start_index, end_index):
        path = os.path.join(BATCH_INPUT_DIRS[index], input_file)
        if input_file in ["co2.nc", "projected-co2.nc"]:
            shutil.copy(input_path, path)
        else:
            subprocess.run(
                [
                    "ncks",
                    "-O",
                    "-h",
                    "-d",
                    f"{split_dimension},{index}",
                    input_path,
                    path,
                ]
            )

    print("done splitting ", input_file)
