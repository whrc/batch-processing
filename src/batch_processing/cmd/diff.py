from pathlib import Path
import subprocess

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import interpret_path


class DiffCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()

        args.path_one = Path(interpret_path(args.path_one))
        args.path_two = Path(interpret_path(args.path_two))
        self._args = args

    def execute(self):
        if not self._args.path_one.is_dir():
            raise Exception(f"The given path is not a directory: {self._args.path_one}")

        if not self._args.path_two.is_dir():
            raise Exception(f"The given path is not a directory: {self._args.path_two}")

        files_one = sorted(self._args.path_one.glob("*.nc"))
        files_two = sorted(self._args.path_two.glob("*.nc"))

        if len(files_one) != len(files_two):
            raise Exception(
                "The lengths of the folders are not the same. "
                f"len(path_one): {len(files_one)}, len(path_two): {len(files_two)}"
            )

        for file_one, file_two in zip(files_one, files_two):
            output = subprocess.run([
                "cdo",
                "diffv",
                file_one,
                file_two,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            )

            if output.returncode != 0:
                print(f"{file_one} and {file_two} are not the same.")
                print(output.stdout.decode("utf-8"))
                print(output.stderr.decode("utf-8"))
                return

        print("No difference is found. The two folders are identical.")
