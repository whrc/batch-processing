from pathlib import Path
import subprocess
import shutil

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import interpret_path

INPUT_FILES = [
    "co2.nc",
    "projected-co2.nc",
    "drainage.nc",
    "fri-fire.nc",
    "run-mask.nc",
    "soil-texture.nc",
    "topo.nc",
    "vegetation.nc",
    "historic-explicit-fire.nc",
    "projected-explicit-fire.nc",
    "projected-climate.nc",
    "historic-climate.nc",
]

class ExtractCellCommand(BaseCommand):
    def __init__(self, args):
        self._args = args

    def execute(self):
        output_path = Path(interpret_path(self._args.output_path))
        output_path.mkdir(exist_ok=True)

        input_path = Path(interpret_path(self._args.input_path))
        input_files = [input_path / file for file in INPUT_FILES]
        for input_file in input_files:
            if input_file.name in ["co2.nc", "projected-co2.nc"]:
                shutil.copy(input_file, output_path / input_file.name)
            else:
                subprocess.run([
                    "ncks",
                    "-O",
                    "-h",
                    "-d",
                    f"X,{self._args.X}",
                    "-d",
                    f"Y,{self._args.Y}",
                    input_file,
                    output_path / input_file.name,
                ])

# todo: copy calibration and parameter directories
# todo: copy and configure config.js files
# todo: add slurm_runner.sh script
