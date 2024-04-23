import logging
import os
import shutil
import subprocess

import netCDF4 as nc
from rich import print

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import get_progress_bar


class BatchMergeCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args
        self.merged_config = os.path.join(self.result_dir, "config")

        self.logger = logging.getLogger(__name__)
        file_handler = logging.FileHandler(f"{self.exacloud_user_dir}/merge.log")
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(file_formatter)

        self.logger.addHandler(file_handler)

    def execute(self):
        if not self.validate_run_status():
            self.logger.error(
                "[bold red]There are unexecuted/failed cells. "
                "Merging is aborted![/bold red]"
            )

        STAGES = ["eq", "sp", "tr", "sc"]
        RES_STAGES = ["pr", "eq", "sp", "tr", "sc"]
        TIMESTEPS = ["daily", "monthly", "yearly"]

        os.makedirs(self.result_dir, exist_ok=True)

        with open(self.output_spec_path) as file:
            content = file.readlines()

        variables = []
        for line in content:
            variable = line.split(",")[0]
            variables.append(variable)

        # Name isn't a variable. It's a name. So, we skip that.
        variables.remove("Name")

        with get_progress_bar() as progress_bar:
            task = progress_bar.add_task(
                "[cyan]Merging variable files...",
                total=len(variables) * len(STAGES) * len(TIMESTEPS),
            )
            # First handle all the normal outputs.
            for variable in variables:
                self.logger.debug(f"Processing variable: {variable.strip()}")
                for stage in STAGES:
                    self.logger.debug(f"  --> stage: {stage}")

                    for timestep in TIMESTEPS:
                        self.logger.debug(f"  --> timestep: {timestep}")

                        # Determine the file name of the outputs variable
                        # for the specific run mode and time step
                        filename = f"{variable.strip()}_{timestep}_{stage}.nc"
                        self.logger.debug(f"  --> find: {filename}")

                        # List all the output files for the variable in question
                        # in every output sub-directory
                        # (one directory = one sub-regional run)
                        filelist = subprocess.getoutput(
                            f"find {self.batch_dir} -maxdepth 4 -type f -name '{filename}'"
                        )

                        if filelist:
                            # Concatenate all these files together
                            self.logger.debug("merge files")

                            # Something is messed up with my quoting, as this only
                            # works with the filelist variable **unquoted** which
                            # I think is bad practice.
                            subprocess.run(
                                ["ncea", "-O", "-h", "-y", "avg"]
                                + filelist.split()
                                + [f"{self.result_dir}/{filename}"]
                            )
                        else:
                            self.logger.debug("  --> nothing to do; no files found...")

                        progress_bar.advance(task)

        print("[cyan]Merging restart files...[/cyan]")
        # Next handle the restart files
        for stage in RES_STAGES:
            filename = f"restart-{stage}.nc"
            self.logger.debug(f"  --> stage: {stage}")

            filelist = subprocess.getoutput(
                f"find {self.batch_dir} -maxdepth 4 -type f -name '{filename}'"
            )
            self.logger.debug(f"THE FILE LIST IS: {filelist}")

            if filelist:
                subprocess.run(
                    ["ncea", "-O", "-h", "-y", "avg"]
                    + filelist.split()
                    + [f"{self.result_dir}/{filename}"]
                )

            else:
                self.logger.debug(
                    f"nothing to do - no restart files for stage {stage} found?"
                )

        print("[cyan]Merging run_status files...[/cyan]")
        # Next handle the run_status file
        filelist = subprocess.getoutput(
            f"find {self.batch_dir} -maxdepth 4 -type f -name 'run_status.nc'"
        )
        self.logger.debug(f"THE FILE LIST IS: {filelist}")
        if filelist:
            # NOTE: for some reason the 'avg' operator does not work with this file!!
            subprocess.run(
                ["ncea", "-O", "-h", "-y", "max"]
                + filelist.split()
                + [f"{self.result_dir}/run_status.nc"]
            )

        else:
            self.logger.debug("nothing to do - no run_status.nc files found?")

        print("[cyan]Merging fail_log files...[/cyan]")
        # Finally, handle the fail log
        filelist = subprocess.getoutput(
            f"find {self.batch_dir} -maxdepth 4 -type f -name 'fail_log.txt'"
        )
        self.logger.debug(f"THE FILE LIST IS: {filelist}")
        if filelist:
            for f in filelist.split():
                with open(f) as f_read:
                    with open(f"{self.result_dir}/fail_log.txt", "a") as f_write:
                        f_write.write(f_read.read())

            print("[green]Successfully merged fail_log files![/green]")
        else:
            self.logger.debug("nothing to do - no fail_log.txt files found?")

        self.copy_config_files()

    def copy_config_files(self):
        """Copies `config.js` and `slurm_runner.sh` to `all-merged/config`.

        `slurm_runner.sh` is the first batch's runner script. Since the others
        are almost the same, except the batch number, the first one is taken
        as an example.

        The reason why we are copying these files is that we may need to investigate
        them later when a run is completed with or without an error.
        """
        os.makedirs(os.path.join(self.merged_config), exist_ok=True)

        config_dest = os.path.join(self.merged_config, "config.js")
        shutil.copyfile(self.config_path, config_dest)

        batch_directories = os.listdir(self.batch_dir)

        if not batch_directories:
            return

        # Find the first directory in the batch directory
        first_batch_dir = next(
            (
                d
                for d in batch_directories
                if os.path.isdir(os.path.join(self.batch_dir, d))
            ),
            None,
        )

        if not first_batch_dir:
            return

        batch_runner_src = os.path.join(
            self.batch_dir, first_batch_dir, "slurm_runner.sh"
        )
        batch_runner_dest = os.path.join(self.merged_config, "slurm_runner.sh")
        shutil.copyfile(batch_runner_src, batch_runner_dest)

        print(
            f"[green bold]A new directory, {self.merged_config}, is created with "
            "config.js and slurm_runner.sh for a further reference. [/green bold]"
        )

    def validate_run_status(self) -> bool:
        """Reads every `run_status.nc` file in every batch and checks if a
        cell is failed.

        An example file looks like this:

        ```

        netcdf run_status {
        dimensions:
                Y = 10 ;
                X = 10 ;
        variables:
                int run_status(Y, X) ;
                        run_status:_FillValue = -9999 ;
                int total_runtime(Y, X) ;
                        total_runtime:_FillValue = -9999 ;
                        total_runtime:units = "seconds" ;
        data:

        run_status =
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        100, 100, 100, -23, 100, 100, -12, 100, 100, 100,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0 ;

        total_runtime =
        _, _, _, _, _, _, _, _, _, _,
        16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
        _, _, _, _, _, _, _, _, _, _,
        _, _, _, _, _, _, _, _, _, _,
        _, _, _, _, _, _, _, _, _, _,
        _, _, _, _, _, _, _, _, _, _,
        _, _, _, _, _, _, _, _, _, _,
        _, _, _, _, _, _, _, _, _, _,
        _, _, _, _, _, _, _, _, _, _,
        _, _, _, _, _, _, _, _, _, _ ;
        }

        This method gets `run_status` variable and makes sure each cell is run
        without any error.

        A positive number stands for success
        _ stands for cell not being run
        A negative number stands for failure
        ```
        """
        flag = True
        error_messages = []

        batch_folders = [
            folder
            for folder in os.listdir(self.batch_dir)
            if os.path.isdir(os.path.join(self.batch_dir, folder))
        ]

        progress_bar = get_progress_bar()
        with progress_bar as bar:
            for batch_folder in bar.track(batch_folders):
                batch_number = int(batch_folder.split("-")[-1])
                run_status_file_path = self.run_status_path.format(batch_folder)
                dataset = nc.Dataset(run_status_file_path)
                data = dataset.variables["run_status"][:]
                dataset.close()

                row = data[batch_number]
                if "_" in row or any(elem < 0 for elem in row):
                    error_messages.append(
                        "[bold red]Some cells didn't run successfully "
                        f"in {batch_folder}[/bold red]\n\n",
                    )

                    error_messages.append(
                        "[blue]Run [/blue][bold blue]ncdump "
                        f"{run_status_file_path}[/bold blue]"
                        "[blue] to further investigate.[/blue]"
                    )
                    flag = False

        if flag:
            print(
                "[bold green]Each cell is run and successfully completed![/bold green]"
            )
        else:
            for err_msg in error_messages:
                print(err_msg)

        return flag
