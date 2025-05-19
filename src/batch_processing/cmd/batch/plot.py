from pathlib import Path

from batch_processing.cmd.base import BaseCommand


class BatchPlotCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args
        self.base_batch_dir = Path(self.exacloud_user_dir, args.batches)
        self.result_dir = self.base_batch_dir / "all_merged"

        if not self.result_dir.exists():
            raise FileNotFoundError(f"{self.result_dir} doesn't exist")

    def execute(self):
        pass