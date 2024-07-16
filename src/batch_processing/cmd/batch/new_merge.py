from collections import defaultdict
import subprocess
from pathlib import Path
import shutil
import re

from batch_processing.cmd.base import BaseCommand


class BatchNewMergeCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args
        self.output_dir = Path(self.output_dir)

    def execute(self):
        batch_output_dirs = self.output_dir.glob("*/output")

        # keys hold the file names
        # values hold the paths of that files
        grouped_files = defaultdict(list)

        for batch_dir in batch_output_dirs:
            for file in batch_dir.iterdir():
                grouped_files[file.name].append(file)

        print("sorting the files")
        for file_name in grouped_files:
            grouped_files[file_name].sort(key=get_batch_number)

        print("renaming and copying the files")
        for file_name in grouped_files:
            all_files = grouped_files[file_name]
            new_files = []
            for index, file in enumerate(all_files):
                temp_file = Path(file.parent / f"{file.stem}_{index}.nc")
                file_as_str = file.as_posix()
                if any(map(file_as_str.__contains__, ["restart", "run_status"])):
                    shutil.copy(file, temp_file)
                else:
                    subprocess.run([
                        "ncrename",
                        "-O",
                        "-h",
                        "-d",
                        "x,X",
                        "-d",
                        "y,Y",
                        file_as_str,
                        temp_file,
                    ])

                new_files.append(temp_file)

            grouped_files[file_name] = new_files

        for file_name in grouped_files:
            all_files = grouped_files[file_name]
            for file in all_files:
                subprocess.run([
                    "ncks",
                    "-O",
                    "-h",
                    "--mk_rec_dmn",
                    "Y",
                    file,
                    file,
                ])

        print("ncap2")
        for file_name in grouped_files:
            all_files = grouped_files[file_name]
            for index, file in enumerate(all_files):
                print(file)
                subprocess.run([
                    'ncap2',
                    '-O',
                    '-h',
                    f"-s'Y[$Y]={index}; X[$X]=array(0, 1, $X);'",
                    file,
                    file,
                ])

        print("concatenating the files")
        for file_name in grouped_files:
            all_files = grouped_files[file_name]
            subprocess.run([
                "ncrcat",
                "-O",
                "-h",
                *all_files,
                f"merged_{file_name}",
            ])


def get_batch_number(path: Path) -> int:
    """Returns the batch number from the given path.
    
    An example argument would be like this:

    /mnt/exacloud/dteber_woodwellclimate_org/output/batch_0/output/restart-eq.nc

    The return value for the above path is 0.
    """
    match_found = re.search(r'batch_(\d+)', str(path))
    return int(match_found.group(1)) if match_found else -1
