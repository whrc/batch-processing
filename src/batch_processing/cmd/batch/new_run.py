import os
import subprocess

from rich.progress import track

from batch_processing.cmd.base import BaseCommand
from batch_processing.cmd.elapsed import ElapsedCommand


class BatchNewRunCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args

    def execute(self):
        full_paths = [
            os.path.join(self.output_dir, item) for item in os.listdir(self.output_dir)
        ]
        # -1 is for removing batch-run/ dir
        total_batches = sum(1 for path in full_paths if os.path.isdir(path)) - 1

        for index in track(
            range(total_batches), description="Submitting batches", total=total_batches
        ):
            slurm_script_path = f"{self.output_dir}/batch_{index}/slurm_runner.sh"
            subprocess.check_output(["sbatch", slurm_script_path])

        ElapsedCommand(self._args).execute()
