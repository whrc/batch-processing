import subprocess
from pathlib import Path

from rich.progress import track

from batch_processing.cmd.base import BaseCommand
from batch_processing.cmd.elapsed import ElapsedCommand


class BatchNewRunCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args
        self.output_dir = Path(self.output_dir)

    def execute(self):
        full_paths = self.output_dir.glob("*/slurm_runner.sh")

        for path in track(
            (p for p in full_paths), 
            description="Submitting batches",
            total=len(full_paths)
        ):
            subprocess.run([
                "sbatch",
                path.as_posix(),
            ])

        ElapsedCommand(self._args).execute()
