import json
import os
import re
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from itertools import product
from multiprocessing import Pool
from pathlib import Path
from typing import Union

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import (
    INPUT_FILES,
    IO_PATHS,
    get_dimensions,
    render_slurm_job_script,
)

BATCH_DIRS = []
BATCH_INPUT_DIRS = []
SETUP_SCRIPTS_PATH = os.path.join(os.environ["HOME"], "dvm-dos-tem/scripts/util")


class BatchSplitCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args
        self.output_dir = Path(self.output_dir)
        self.base_batch_dir = Path(self.exacloud_user_dir, args.batches)
        self.log_path = Path(self.base_batch_dir, "logs")

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

    def _configure(self, index, batch_dir):
        config_file = os.path.join(batch_dir, "config/config.js")
        with open(config_file) as f:
            config_data = json.load(f)

        for key, val in IO_PATHS.items():
            config_data["IO"][key] = f"{batch_dir}/{val}"

        with open(config_file, "w") as f:
            json.dump(config_data, f, indent=4)

        substitution_values = {
            "job_name": f"batch-{index}",
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
        slurm_runner = render_slurm_job_script("slurm_runner.sh", substitution_values)

        with open(f"{batch_dir}/slurm_runner.sh", "w") as file:
            file.write(slurm_runner)

    def _get_chunk_size(self, sliced_dir):
        chunk_range = sliced_dir.split("-")[-1]
        start, end = (int(val) for val in chunk_range.split("_"))
        return end - start

    def _calculate_dimensions(self):
        INPUT_FILE = "run-mask.nc"
        input_path = Path(self._args.input_path)
        items = [item for item in input_path.iterdir()]

        # unsliced, normal input dataset
        if all([item.is_file() for item in items]):
            X, Y = get_dimensions(input_path / INPUT_FILE)
            return X, Y, False

        if all([item.is_dir() for item in items]):
            X = Y = 0
            for item in items:
                path = item / INPUT_FILE
                temp_x, temp_y = get_dimensions(path)
                X += temp_x
                Y += temp_y

            if X > Y:
                X = X // len(items)
            else:
                Y = Y // len(items)

            return X, Y, True

        raise ValueError(
            "The provided path to the input data contains malformed information. "
            "Either check the path or fix the input data."
        )

    def create_chunks(self, dim_size, chunk_count):
        """Create chunk boundaries for slicing the dataset."""
        chunks = []
        chunk_size = dim_size // chunk_count
        remainder = dim_size % chunk_count

        for i in range(chunk_count):
            start = i * chunk_size + min(i, remainder)
            end = start + chunk_size + (1 if i < remainder else 0)
            chunks.append((start, end))

        return chunks

    def _submit_job(self) -> Union[str, str]:
        substitution_values = {
            "job_name": "split_job",
            "partition": self._args.slurm_partition,
            "log_path": self.exacloud_user_dir,
            "p": self._args.p,
            "e": self._args.e,
            "s": self._args.s,
            "t": self._args.t,
            "n": self._args.n,
            "input_path": self._args.input_path,
            "batches": self._args.batches,
            "log_level": self._args.log_level,
        }
        job_script = render_slurm_job_script("split_job.sh", substitution_values)
        result = subprocess.run(
            ["sbatch"], input=job_script, text=True, capture_output=True
        )
        return result.stdout, result.stderr

    def execute(self):
        X, Y, use_parallel = self._calculate_dimensions()

        if use_parallel and not self._args.launch_as_job:
            stdout, stderr = self._submit_job()
            if stderr == "":
                print("The split job is successfully submitted.")
                print(stdout.strip())
            else:
                print("Something went wrong when submitting the job")

            return

        print("Dimension size of X:", X)
        print("Dimension size of Y:", Y)

        # Choose the dimension to split along
        SPLIT_DIMENSION, DIMENSION_SIZE = ("X", X) if Y > X else ("Y", Y)

        print(f"\nSplitting accros {SPLIT_DIMENSION} dimension")
        print("Dimension size:", DIMENSION_SIZE)

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

            path = os.path.join(path, "input")
            BATCH_INPUT_DIRS.append(path)

        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
            executor.map(lambda elem: os.makedirs(elem), BATCH_INPUT_DIRS)

        print("Split input files")
        if use_parallel:
            tasks = []
            sliced_dirs = os.listdir(self._args.input_path)

            for sliced_dir, input_file in product(sliced_dirs, INPUT_FILES):
                # todo: change this hard-coded value
                chunks = self.create_chunks(185, os.cpu_count())
                input_file_path = os.path.join(
                    self._args.input_path, sliced_dir, input_file
                )
                for start_chunk, end_chunk in chunks:
                    tasks.append(
                        (
                            start_chunk,
                            end_chunk,
                            input_file_path,
                            input_file,
                            SPLIT_DIMENSION,
                        )
                    )

            with Pool(processes=os.cpu_count()) as pool:
                pool.starmap(split_file_chunk, tasks)
        else:
            split_file(0, DIMENSION_SIZE, self._args.input_path, SPLIT_DIMENSION)

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


def split_file_chunk(start_index, end_index, input_path, input_file, split_dimension):
    print("splitting ", input_path)
    folders = input_path.split("/")
    chunk_folder = folders[-2]
    intervals = chunk_folder.split("-")[-1]
    chunk_start, chunk_end = (int(elem) for elem in intervals.split("_"))
    for index in range(start_index, end_index):
        path = os.path.join(BATCH_INPUT_DIRS[chunk_start + index], input_file)
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


def split_file(start_index, end_index, input_path, split_dimension):
    for input_file in INPUT_FILES:
        src_input_path = os.path.join(input_path, input_file)
        print("splitting ", src_input_path)
        for index in range(start_index, end_index):
            path = os.path.join(BATCH_INPUT_DIRS[index], input_file)
            if input_file in ["co2.nc", "projected-co2.nc"]:
                shutil.copy(src_input_path, path)
            else:
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
