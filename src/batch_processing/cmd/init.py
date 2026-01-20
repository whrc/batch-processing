import json
import subprocess
from pathlib import Path

from rich import print

from batch_processing.cmd.base import BaseCommand, CONFIG_FILE_PATH, DEFAULT_BASEDIR
from batch_processing.utils.utils import download_directory, download_file, run_command


class InitCommand(BaseCommand):
    def __init__(self, args):
        basedir = getattr(args, "basedir", DEFAULT_BASEDIR)
        super().__init__(basedir=basedir)
        self._args = args
        self._compile = getattr(args, "compile", False)

    def execute(self):
        if self.user == "root":
            raise ValueError("Do not run as root or with sudo.")

        # Copy necessary files from the cloud
        # dvm-dos-tem version v0.7.0 - 2023-06-14
        # Note: dvmdostem binary is compiled with USEMPI=true flag
        if self.dvmdostem_path.exists():
            print("[bold yellow]dvm-dos-tem already exists, using current installation...[/bold yellow]")
        else:
            if self._compile:
                # Clone from GitHub and compile
                print(f"[bold blue]Cloning dvm-dos-tem to {self.dvmdostem_path} directory...[/bold blue]")
                subprocess.run(
                    f"git clone https://github.com/uaf-arctic-eco-modeling/dvm-dos-tem.git {self.dvmdostem_path}",
                    shell=True,
                    check=True,
                    executable="/bin/bash",
                )
                print(f"[bold green]dvm-dos-tem is cloned to {self.dvmdostem_path}[/bold green]")

                print("[bold blue]Compiling dvmdostem binary...[/bold blue]")
                command = f"""
                cd {self.dvmdostem_path} && \
                export DOWNLOADPATH=/dependencies && \
                . $DOWNLOADPATH/setup-env.sh && \
                module load openmpi && \
                make USEMPI=true
                """

                subprocess.run(command, shell=True, check=True, executable="/bin/bash")
                print("[bold green]dvmdostem binary is successfully compiled.[/bold green]")
            else:
                # Copy pre-built version from bucket (default)
                basedir = str(self.dvmdostem_path.parent)
                print(f"[bold blue]Copying dvm-dos-tem to {self.dvmdostem_path} directory...[/bold blue]")
                download_directory("gcp-slurm", "dvm-dos-tem/", basedir)
                print(f"[bold green]dvm-dos-tem is copied to {self.dvmdostem_path}[/bold green]")

            subprocess.run([f"chmod +x {self.dvmdostem_bin_path}"], shell=True, check=True)
            subprocess.run(
                f"chmod +x {self.dvmdostem_scripts_path}/*", shell=True, check=True
            )

        if Path(self.output_spec_path).exists():
            print("[bold yellow]output_spec.csv already exists, using current file...[/bold yellow]")
        else:
            download_file(
                "gcp-slurm",
                "output_spec.csv",
                self.output_spec_path,
            )
            print(
                f"[bold blue]output_spec.csv is copied to {self.output_spec_path}[/bold blue]"
            )

        run_command(["sudo", "-H", "mkdir", "-p", self.exacloud_user_dir])
        # run_command(
        #     [
        #         "sudo",
        #         "-H",
        #         "chown",
        #         "-R",
        #         f"{self.user}:{self.user}",
        #         self.exacloud_user_dir,
        #     ]
        # )
        print(
            "[bold green]A new directory is created for the current user, "
            f"{self.exacloud_user_dir}[/bold green]"
        )

        # run_command(
        #     [
        #         "lfs",
        #         "setstripe",
        #         "-E",
        #         "64M",
        #         "-c",
        #         "2",
        #         "-E",
        #         "512M",
        #         "-c",
        #         "8",
        #         "-E",
        #         "-1",
        #         "-c",
        #         "16",
        #         self.exacloud_user_dir,
        #     ]
        # )

        # Save configuration to config file
        config = {"basedir": str(self.dvmdostem_path)}
        with open(CONFIG_FILE_PATH, "w") as f:
            json.dump(config, f, indent=2)
        print(f"[bold green]Configuration saved to {CONFIG_FILE_PATH}[/bold green]")

        print(
            "\n[bold green]The initialization is successfully completed.[/bold green]"
        )
        print(
            f"[bold green]Check {self.home_dir} and {self.exacloud_user_dir} for the changes.[/bold green]\n"
        )
