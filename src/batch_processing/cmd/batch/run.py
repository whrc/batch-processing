import os

from progress.bar import Bar

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import run_command


class BatchRunCommand(BaseCommand):
    def __init__(self, args):
        self._args = args

    def execute(self):
        full_paths = [
            os.path.join(self.batch_dir, item) for item in os.listdir(self.batch_dir)
        ]
        total_batches = sum(1 for path in full_paths if os.path.isdir(path))

        # fix the outputting error
        bar = Bar("Submitting batches", max=total_batches)
        for index in range(0, total_batches):
            slurm_script_path = f"{self.batch_dir}/batch-{index}/slurm_runner.sh"
            run_command(["sbatch", slurm_script_path, ">", "/dev/null"])
            bar.next()

        bar.finish()
