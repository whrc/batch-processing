from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import download_directory, download_file, run_command


class InitCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args

    def execute(self):
        if self.user == "root":
            raise ValueError("Do not run as root or with sudo.")

        self.exacloud_user_dir.mkdir(exist_ok=True)
        self.slurm_log_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)

        # Copy necessary files from the cloud
        # dvm-dos-tem version v0.7.0 - 2023-06-14
        # Note: dvmdostem binary is compiled with USEMPI=true flag
        print("Copying dvm-dos-tem to the home directory...")

        download_directory("gcp-slurm", "dvm-dos-tem/", self.home_dir)
        download_file(
            "gcp-slurm",
            "output_spec.csv",
            self.output_spec_path,
        )

        run_command(["chmod", "+x", self.dvmdostem_bin_path])
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
        run_command(["lfs", "setstripe", "-S", "0.25M", self.exacloud_user_dir])

        print("The initialization is successfully completed.")
        print(f"Check {self.home_dir} and {self.exacloud_user_dir} for the changes.")
