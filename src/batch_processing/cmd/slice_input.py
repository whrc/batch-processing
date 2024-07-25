import multiprocessing as mp
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from string import Template
from typing import List, Union

import xarray as xr

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import (
    INPUT_FILES,
    INPUT_FILES_TO_COPY,
    Chunk,
    create_chunks,
    get_dimension_sizes,
    get_project_root,
    interpret_path,
)

MIN_CELL_COUNT = 500_000
SLICE_COUNT = 10


@dataclass
class ChunkTask:
    chunk: Chunk
    src_path: str
    dest_path: str


def slice_and_save(chunk_task: ChunkTask) -> None:
    """Slice a chunk of the dataset and save it to a new NetCDF file."""
    try:
        print(f"Processing {chunk_task.src_path}...")
        with xr.open_dataset(
            chunk_task.src_path, engine="netcdf4", decode_times=False
        ) as dataset:
            subset = dataset.isel(Y=slice(chunk_task.chunk.start, chunk_task.chunk.end))
            subset.to_netcdf(
                chunk_task.dest_path,
                encoding={
                    "albers_conical_equal_area": {"dtype": "str"},
                    "lat": {"_FillValue": None},
                    "lon": {"_FillValue": None},
                },
            )
        print(f"Done processing {chunk_task.src_path}!")
    except Exception as e:
        print(
            f"Error processing chunk {chunk_task.src_path} for "
            f"{chunk_task.src_path}: {e}"
        )


class SliceInputCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args

        self.input_path = Path(interpret_path(args.input_path))
        self.output_path = Path(interpret_path(args.output_path))

    def _check_cell_count(self, input_file_path: str) -> Union[bool, int]:
        """
        Check if the given input file satisfies the minimum cell count.

        Returns:
            bool: True if the minimum cell count is satisfied. False, otherwise
            int: Cell count in the Y dimension. If the minimum cell count is not
                satisfied, 0 is returned.
        """
        X, Y = get_dimension_sizes(input_file_path)

        current_cell_count = X * Y
        if current_cell_count < MIN_CELL_COUNT:
            print(
                f"The given dataset has {current_cell_count} cells. "
                f"It has to have at least {MIN_CELL_COUNT} cells."
            )
            return True, 0

        return False, Y

    def _prepare_tasks_from_chunks(self, chunks: List[Chunk]) -> List[ChunkTask]:
        tasks = []
        for input_file in INPUT_FILES:
            print(f"Processing {input_file}...")
            input_file_path = self.input_path / input_file

            for chunk in chunks:
                chunk_directory_path = self.output_path / f"{chunk.start}_{chunk.end}"
                chunk_directory_path.mkdir(parents=True, exist_ok=True)

                chunk_file_path = chunk_directory_path / input_file
                if input_file in INPUT_FILES_TO_COPY:
                    shutil.copy(input_file_path, chunk_file_path)
                else:
                    t = ChunkTask(chunk, input_file_path, chunk_file_path)
                    tasks.append(t)

        return tasks

    def _get_slurm_job(self) -> str:
        template_path = Path(f"{get_project_root()}/templates/slice_input_job.sh")
        with open(template_path) as file:
            template = Template(file.read())

        template = template.substitute(
            {
                "job_name": "slice input job",
                "partition": "process",
                "log_path": f"{self.exacloud_user_dir}/slice_input.log",
                "input_path": self._args.input_path,
                "output_path": self._args.output_path,
            }
        )

        return template

    def _submit_job(self) -> Union[str, str]:
        job_script = self._get_slurm_job()
        result = subprocess.run(
            ["sbatch"], input=job_script, text=True, capture_output=True
        )
        return result.stdout, result.stderr

    def execute(self):
        if not self._args.force and self.output_path.exists():
            print(
                "The given output path exists. "
                "If you want to override it, pass -f or --force flags. Exiting..."
            )
            exit(1)

        if self._args.force and self.output_path.exists():
            print("The given output path exists. Removing it...")
            shutil.rmtree(self.output_path)

        should_terminate, DIMENSION_SIZE = self._check_cell_count(
            self.input_path / "vegetation.nc"
        )
        if should_terminate:
            exit(1)

        if self._args.launch_as_job:
            chunks = create_chunks(DIMENSION_SIZE, SLICE_COUNT)
            tasks = self._prepare_tasks_from_chunks(chunks)

            with mp.Pool(processes=mp.cpu_count()) as pool:
                pool.map(slice_and_save, tasks)
        else:
            stdout, stderr = self._submit_job()
            if stderr == "":
                print("Job is successfully submited.")
                print(stdout)
