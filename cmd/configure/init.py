import os
from cmd.base import BaseCommand
from pathlib import Path

from batch_processing.constants import (
    BUCKET_OUTPUT_SPEC,
    CONFIG_PATH,
    DVMDOSTEM_BIN_PATH,
    EXACLOUD_USER_DIR,
    OUTPUT_PATH,
    OUTPUT_SPEC_PATH,
    SLURM_LOG_PATH,
    USER,
)
from batch_processing.utils import download_directory, run_command


class ConfigureInitCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args
        self._input_dir = f"/mnt/exacloud/{os.getenv('USER')}/input"

    def execute(self):
        if self.user == "root":
            raise ValueError("Do not run as root or with sudo.")

        data = self._args.input_data
        if data:
            Path(self._input_dir).mkdir(exist_ok=True)
            # todo: take `bucket_name` and `prefix` as arguments
            # download_directory(args.bucket_name, args.prefix)
            run_command(["gsutil", "-m", "cp", "-r", data, self._input_dir])
            print(
                f"The input data is successfully copied from Google Bucket to {self._input_dir}"
            )

        # Copy necessary files from the cloud
        download_directory("slurm-homefs", "dvm-dos-tem/")
        print(f"dvm-dos-tem is copied to {os.getenv('HOME')}")
        # run_command(["gsutil", "-m", "cp", "-r", BUCKET_DVMDOSTEM, HOME])
        run_command(["chmod", "+x", DVMDOSTEM_BIN_PATH])
        run_command(
            [
                "gsutil",
                "-m",
                "cp",
                BUCKET_OUTPUT_SPEC,
                OUTPUT_SPEC_PATH,
            ]
        )
        print(f"output_spec.csv is copied to {OUTPUT_SPEC_PATH}")

        run_command(["sudo", "-H", "mkdir", "-p", EXACLOUD_USER_DIR])
        run_command(["sudo", "-H", "chown", "-R", f"{USER}:{USER}", EXACLOUD_USER_DIR])
        print(
            f"A new directory is created for the current user, {os.getenv('USER')} in /mnt/exacloud"
        )

        Path(SLURM_LOG_PATH).mkdir(exist_ok=True)
        # run_command(["sudo", "mkdir", "-p", SLURM_LOG_PATH])

        run_command(["lfs", "setstripe", "-S", "0.25M", EXACLOUD_USER_DIR])

        # Modify `output_dir` field in the configuration file
        # todo: move this operation to cmd/input.py file
        run_command(
            [
                "sed",
                "-i",
                f's|"output_dir":\s*"output/",|"output_dir": "{OUTPUT_PATH}",|',
                CONFIG_PATH,
            ]
        )

        print("The initialization is successfully completed.")
        print(f"Check /home/{os.getenv('USER')} and /mnt/exacloud/{os.getenv('USER')} for the changes.")
