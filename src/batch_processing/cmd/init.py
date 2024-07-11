from pathlib import Path

from rich import print

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import download_directory, download_file, run_command


class InitCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args

    def execute(self):
        if self.user == "root":
            raise ValueError("Do not run as root or with sudo.")

        # Copy necessary files from the cloud
        # dvm-dos-tem version v0.7.0 - 2023-06-14
        # Note: dvmdostem binary is compiled with USEMPI=true flag
        print("[bold blue]Copying dvm-dos-tem to the home directory...[/bold blue]")
        download_directory("gcp-slurm", "dvm-dos-tem/", self.home_dir)
        # subprocess.run(
        #     f"git clone https://github.com/uaf-arctic-eco-modeling/dvm-dos-tem.git {self.home_dir}/dvm-dos-tem",
        #     shell=True,
        #     check=True,
        #     executable="/bin/bash",
        # )
        print(f"[bold green]dvm-dos-tem is copied to {self.home_dir}[/bold green]")

        # print("[bold blue]Compile dvmdostem binary...[/bold blue]")
        # command = f"""
        # cd {self.home_dir}/dvm-dos-tem && \
        # export DOWNLOADPATH=/dependencies && \
        # . $DOWNLOADPATH/setup-env.sh && \
        # module load openmpi && \
        # make USEMPI=true
        # """

        # subprocess.run(command, shell=True, check=True, executable="/bin/bash")
        # print("[bold green]dvmdostem binary is successfully compiled.[/bold green]")
        run_command(["chmod", "+x", f"{self.dvmdostem_scripts_path}/*"])
        run_command(["chmod", "+x", ])

        download_file(
            "gcp-slurm",
            "output_spec.csv",
            self.output_spec_path,
        )
        print(
            f"[bold blue]output_spec.csv is copied to {self.output_spec_path}[/bold blue]"
        )

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
            "[bold green]A new directory is created for the current user, "
            f"{self.exacloud_user_dir}[/bold green]"
        )

        Path(f"{self.output_dir}").mkdir(exist_ok=True)
        Path(f"{self.slurm_log_dir}").mkdir(exist_ok=True)
        print(
            f"[bold blue]slurm-logs directory is created under {self.exacloud_user_dir}[/bold blue]"
        )

        run_command(["lfs", "setstripe", "-S", "0.25M", self.exacloud_user_dir])

        print(
            "\n[bold green]The initialization is successfully completed.[/bold green]"
        )
        print(
            f"[bold green]Check {self.home_dir} and {self.exacloud_user_dir} for the changes.[/bold green]\n"
        )
