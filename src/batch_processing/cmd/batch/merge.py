from collections import defaultdict
import subprocess
from pathlib import Path
import shutil

from batch_processing.utils.utils import get_batch_number
from batch_processing.cmd.base import BaseCommand


class BatchMergeCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args
        self.output_dir = Path(self.output_dir)
        self.result_dir = Path(self.result_dir)

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
                try:
                    completed_process = subprocess.run([
                        'ncap2',
                        '-O',
                        '-h',
                        '-s', f'Y[$Y]={index}; X[$X]=array(0, 1, $X);',
                        file.as_posix(),
                        file.as_posix(),
                    ], check=True, capture_output=True, text=True)
                except subprocess.CalledProcessError as e:
                    print(f"Error occurred: {e}")
                    print(f"Output: {e.stdout}")
                    print(f"Error output: {e.stderr}")

        self.result_dir.mkdir(exist_ok=True)
        print("concatenating the files")
        for file_name in grouped_files:
            all_files = grouped_files[file_name]
            subprocess.run([
                "ncrcat",
                "-O",
                "-h",
                *all_files,
                self.result_dir / file_name,
            ])
