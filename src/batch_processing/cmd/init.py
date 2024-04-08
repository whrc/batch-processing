import subprocess
from pathlib import Path

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import download_file, run_command


class InitCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args

    def execute(self):
        if self.user == "root":
            raise ValueError("Do not run as root or with sudo.")

        # Copy necessary files from the cloud
        print("Copying dvm-dos-tem to the home directory...")
        # download_directory("gcp-slurm", "dvm-dos-tem/", self.home_dir)
        subprocess.run(
            f"git clone https://github.com/uaf-arctic-eco-modeling/dvm-dos-tem.git {self.home_dir}/dvm-dos-tem",
            shell=True,
            check=True,
            executable="/bin/bash",
        )
        print(f"dvm-dos-tem is copied to {self.home_dir}")

        print("Compile dvmdostem binary...")
        command = """
        cd dvm-dos-tem && \
        export DOWNLOADPATH=/dependencies && \
        . $DOWNLOADPATH/setup-env.sh && \
        module load openmpi && \
        make USEMPI=true
        """

        subprocess.run(command, shell=True, check=True, executable="/bin/bash")
        # run_command(["chmod", "+x", self.dvmdostem_bin_path])
        print("dvmdostem binary is successfully compiled.")

        download_file(
            "gcp-slurm",
            "output_spec.csv",
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
            f"{self.exacloud_user_dir}"
        )

        Path(f"{self.slurm_log_dir}").mkdir(exist_ok=True)
        print(f"slurm-logs directory is created under {self.exacloud_user_dir}")

        run_command(["lfs", "setstripe", "-S", "0.25M", self.exacloud_user_dir])

        print("\nThe initialization is successfully completed.")
        print(
            f"Check {self.home_dir} and " f"{self.exacloud_user_dir} for the changes.\n"
        )
