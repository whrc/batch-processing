from pathlib import Path

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import download_directory, download_file, run_command


class InitCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args
        self._input_dir = f"/mnt/exacloud/{self.user}/input"

    def execute(self):
        if self.user == "root":
            raise ValueError("Do not run as root or with sudo.")

        # Copy necessary files from the cloud
        print("Copying dvm-dos-tem to the home directory...")
        download_directory("slurm-homefs", "dvm-dos-tem/")
        print(f"dvm-dos-tem is copied to {self.home_dir}")
        run_command(["chmod", "+x", self.dvmdostem_bin_path])

        download_file(
            "four-basins",
            "all-merged/config/output_spec.csv",
            self.output_spec_path,
        )
        print(f"output_spec.csv is copied to {self.output_spec_path}")

        run_command(["sudo", "-H", "mkdir", "-p", self.exacloud_user_dir])
        run_command(
            [
                "sudo",
                "-H",
                "chown",
                "-R",
                f"{self.user}:{self.user}",
                self.exacloud_user_dir,
            ]
        )
        print(
            "A new directory is created for the current user, "
            f"{self.user} in /mnt/exacloud"
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

        Path(f"/mnt/exacloud/{self.user}/slurm-logs").mkdir(exist_ok=True)
        print(f"slurm-logs directory is created under /mnt/exacloud/{self.user}")

        run_command(["lfs", "setstripe", "-S", "0.25M", self.exacloud_user_dir])

        print("\nThe initialization is successfully completed.")
        print(
            f"Check /home/{self.user} and "
            f"/mnt/exacloud/{self.user} for the changes.\n"
        )
