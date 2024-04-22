import os

import netCDF4 as nc
from rich import print
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
)

from .base import BaseCommand


class RunStatusCheck(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args

    def execute(self):
        progress_bar = Progress(
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeElapsedColumn(),
        )
        batch_folders = [
            folder
            for folder in os.listdir(self.batch_dir)
            if os.path.isdir(os.path.join(self.batch_dir, folder))
        ]
        with progress_bar as bar:
            for batch_folder in bar.track(batch_folders):
                batch_number = int(batch_folder.split("-")[-1])
                run_status_file_path = (
                    f"{self.batch_dir}/{batch_folder}/output/run_status.nc"
                )
                dataset = nc.Dataset(run_status_file_path)
                data = dataset.variables["run_status"][:]
                dataset.close()

                row = data[batch_number]
                for elem in row:
                    if elem == "_" or elem < 0:
                        print(
                            f"[bold red]Some cells didn't run successfully in {batch_folder}[/bold red]",
                            end="\n\n",
                        )
                        print(
                            f"[blue]Run [/blue][bold blue]ncdump {run_status_file_path}[/bold blue]"
                            "[blue] to further investigate.[/blue]"
                        )

        print("[bold green]Each cell is run and successfully completed![/bold green]")
