import os

import netCDF4 as nc
from rich import print

from src.batch_processing.utils.utils import get_progress_bar

from .base import BaseCommand


class RunStatusCheck(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args

    def execute(self):
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
                    print(
                        "[bold red]Some cells didn't run successfully "
                        f"in {batch_folder}[/bold red]",
                        end="\n\n",
                    )
                    print(
                        "[blue]Run [/blue][bold blue]ncdump "
                        f"{run_status_file_path}[/bold blue]"
                        "[blue] to further investigate.[/blue]"
                    )

        print("[bold green]Each cell is run and successfully completed![/bold green]")
