import os

from progress.bar import Bar

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import run_command


class BatchRunCommand(BaseCommand):
    def __init__(self, args):
        self._args = args

    def execute(self):
        USER = os.getenv("USER")
        BATCH_DIR = f"/mnt/exacloud/{USER}/output/batch-run"
        full_paths = [os.path.join(BATCH_DIR, item) for item in os.listdir(BATCH_DIR)]
        total_batches = sum(1 for path in full_paths if os.path.isdir(path))

        bar = Bar("Submitting batches", max=total_batches)
        for index in range(0, total_batches):
            slurm_script_path = f"{BATCH_DIR}/batch-{index}/slurm_runner.sh"
            run_command(["sbatch", slurm_script_path, ">", "/dev/null"])
            bar.next()

        bar.finish()
