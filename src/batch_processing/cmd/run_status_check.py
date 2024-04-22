import os

import netCDF4 as nc

from .base import BaseCommand


class RunStatusCheck(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args

    def execute(self):
        batch_folders = [
            folder
            for folder in os.listdir(self.batch_dir)
            if os.path.isdir(os.path.join(self.batch_dir, folder))
        ]
        for batch_folder in batch_folders:
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
                    print(f"something's wrong with batch-{batch_number}")
