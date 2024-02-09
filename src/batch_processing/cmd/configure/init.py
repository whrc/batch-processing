import os
from pathlib import Path

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.constants import (
    DVMDOSTEM_BIN_PATH,
    EXACLOUD_USER_DIR,
    OUTPUT_SPEC_PATH,
    USER,
)
from batch_processing.utils.utils import download_directory, download_file, run_command


class ConfigureInitCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args
        self._input_dir = f"/mnt/exacloud/{os.getenv('USER')}/input"

    def execute(self):
        if self.user == "root":
            raise ValueError("Do not run as root or with sudo.")

        # Copy necessary files from the cloud
        print("Copying dvm-dos-tem to the home directory...")
        download_directory("slurm-homefs", "dvm-dos-tem/")
        print(f"dvm-dos-tem is copied to {os.getenv('HOME')}")
        # run_command(["gsutil", "-m", "cp", "-r", BUCKET_DVMDOSTEM, HOME])
        run_command(["chmod", "+x", DVMDOSTEM_BIN_PATH])

        download_file(
            "four-basins",
            "all-merged/config/output_spec.csv",
            f"{os.getenv('HOME')}/dvm-dos-tem/config/output_spec.csv",
        )
        print(f"output_spec.csv is copied to {OUTPUT_SPEC_PATH}")

        run_command(["sudo", "-H", "mkdir", "-p", EXACLOUD_USER_DIR])
        run_command(["sudo", "-H", "chown", "-R", f"{USER}:{USER}", EXACLOUD_USER_DIR])
        print(
            "A new directory is created for the current user, "
            f"{os.getenv('USER')} in /mnt/exacloud"
        )

        data = self._args.input_data
        if data:
            Path(self._input_dir).mkdir(exist_ok=True)
            # todo: take `bucket_name` and `prefix` as arguments
            # download_directory(args.bucket_name, args.prefix)
            run_command(["gsutil", "-m", "cp", "-r", data, self._input_dir])
            print(
                "The input data is successfully copied "
                f"from Google Bucket to {self._input_dir}"
            )

        Path(f"/mnt/exacloud/{os.getenv('USER')}/slurm-logs").mkdir(exist_ok=True)
        print(
            f"slurm-logs directory is created under /mnt/exacloud/{os.getenv('USER')}"
        )

        run_command(["lfs", "setstripe", "-S", "0.25M", EXACLOUD_USER_DIR])

        print("\nThe initialization is successfully completed.")
        print(
            f"Check /home/{os.getenv('USER')} and "
            f"/mnt/exacloud/{os.getenv('USER')} for the changes.\n"
        )
