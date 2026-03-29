import sys
import shlex
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
        self._branch = getattr(args, "branch", None)

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
                branch_opt = f"-b {shlex.quote(self._branch)} " if self._branch else ""
                subprocess.run(
                    f"git clone {branch_opt}https://github.com/uaf-arctic-eco-modeling/dvm-dos-tem.git {shlex.quote(str(self.dvmdostem_path))}",
                    shell=True,
                    check=True,
                    executable="/bin/bash",
                )
                print(f"[bold green]dvm-dos-tem is cloned to {self.dvmdostem_path}[/bold green]")
                subprocess.run("which python",shell=True, check=True, executable="/bin/bash")
                #subprocess.run(f"pipx install {self.dvmdostem_path}/pyddt",shell=True, check=True, executable="/bin/bash")

                print("[bold blue]Compiling dvmdostem binary...[/bold blue]")
                command = f"""
                cd {self.dvmdostem_path} && \
                export DOWNLOADPATH=/dependencies && \
                if [ -f "$DOWNLOADPATH/setup-env.sh" ]; then \
                    . $DOWNLOADPATH/setup-env.sh && \
                    module load openmpi; \
                fi && \
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
                if self._branch:
                    print("[bold yellow]--branch is ignored unless --compile is specified.[/bold yellow]")


            subprocess.run([f"chmod +x {self.dvmdostem_bin_path}"], shell=True, check=True)
            # Make all Python scripts in scripts directory executable (recursively)
            #subprocess.run(
            #    f"find {self.dvmdostem_scripts_path} -name '*.py' -exec chmod +x {{}} \\;",
            #    shell=True,
            #    check=True
            #)

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

        run_command(["mkdir", "-p", self.exacloud_user_dir])
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

        # Save configuration to config file (save the parent directory, not the full path)
        config = {"basedir": str(self.dvmdostem_path.parent)}
        with open(CONFIG_FILE_PATH, "w") as f:
            json.dump(config, f, indent=2)
        print(f"[bold green]Configuration saved to {CONFIG_FILE_PATH}[/bold green]")

        print(
            "\n[bold green]The initialization is successfully completed.[/bold green]"
        )
        print(
            f"[bold green]Check {self.home_dir} and {self.exacloud_user_dir} for the changes.[/bold green]\n"
        )
