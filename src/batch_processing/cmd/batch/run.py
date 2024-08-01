from pathlib import Path

from rich.progress import track

from batch_processing.cmd.base import BaseCommand
from batch_processing.cmd.elapsed import ElapsedCommand
from batch_processing.utils.utils import submit_job


class BatchRunCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args
        self.base_batch_dir = Path(self.exacloud_user_dir, args.batches)
        self._args.base_batch_dir = self.base_batch_dir

    def execute(self):
        full_paths = list(self.base_batch_dir.glob("*/slurm_runner.sh"))
        if len(full_paths) == 0:
            print(
                "Couldn't find any slurm_runner scripts. ",
                f"Is {self._args.batches} the correct path?",
            )
            exit(1)

        for path in track(
            full_paths, description="Submitting batches", total=len(full_paths)
        ):
            submit_job(path.as_posix())

        ElapsedCommand(self._args).execute()
