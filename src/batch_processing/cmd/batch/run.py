import os
import subprocess

from progress.bar import Bar

from batch_processing.cmd.base import BaseCommand


class BatchRunCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args

    def execute(self):
        full_paths = [
            os.path.join(self.batch_dir, item) for item in os.listdir(self.batch_dir)
        ]
        total_batches = sum(1 for path in full_paths if os.path.isdir(path))

        bar = Bar("Submitting batches", max=total_batches)
        for index in range(0, total_batches):
            slurm_script_path = f"{self.batch_dir}/batch-{index}/slurm_runner.sh"
            subprocess.check_output(["sbatch", slurm_script_path])
            bar.next()

        bar.finish()
