import os
import subprocess
import time

from rich.progress import track

from batch_processing.cmd.base import BaseCommand


class BatchRunCommand(BaseCommand):
    BATCH_INTERVAL = 20
    SLEEP_TIME = 5

    def __init__(self, args):
        super().__init__()
        self._args = args

    def execute(self):
        full_paths = [
            os.path.join(self.batch_dir, item) for item in os.listdir(self.batch_dir)
        ]
        total_batches = sum(1 for path in full_paths if os.path.isdir(path))

        for index in track(
            range(total_batches), description="Submitting batches", total=total_batches
        ):
            slurm_script_path = f"{self.batch_dir}/batch-{index}/slurm_runner.sh"
            subprocess.check_output(["sbatch", slurm_script_path])

            if (index + 1) % self.BATCH_INTERVAL == 0:
                time.sleep(self.SLEEP_TIME)
