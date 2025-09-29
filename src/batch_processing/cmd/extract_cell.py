import json
import shutil
import subprocess
from pathlib import Path
from string import Template

import netCDF4

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import (
    INPUT_FILES,
    IO_PATHS,
    clean_and_load_json,
    generate_random_string,
    get_project_root,
    interpret_path,
)


class ExtractCellCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        args.input_path = Path(interpret_path(args.input_path))
        args.output_path = Path(interpret_path(args.output_path))
        self._args = args

    def _do_coords_in_range(self, input_path, x, y):
        file = netCDF4.Dataset(input_path)
        return y <= file.dimensions["Y"].size and x <= file.dimensions["X"].size

    def _copy_folders(self):
        calib_dest_path = self._args.output_path / "calibration"
        calib_dest_path.mkdir(exist_ok=True)
        calib_src_path = self.dvmdostem_path / "calibration"
        shutil.copy(calib_src_path / "calibration_targets.py", calib_dest_path)

        param_dst_path = self._args.output_path / "parameters"
        param_dst_path.mkdir(exist_ok=True)
        param_src_path = self.dvmdostem_path / "parameters"
        shutil.copytree(param_src_path, param_dst_path, dirs_exist_ok=True)

        config_dst_path = self._args.output_path / "config"
        config_dst_path.mkdir(exist_ok=True)
        config_src_path = self.dvmdostem_path / "config"
        shutil.copytree(config_src_path, config_dst_path, dirs_exist_ok=True)

    def _copy_input_files(self):
        dest_path = self._args.output_path / "input"
        dest_path.mkdir(exist_ok=True, parents=True)

        input_files = [self._args.input_path / file for file in INPUT_FILES]
        for input_file in input_files:
            if input_file.name in ["co2.nc", "projected-co2.nc"]:
                shutil.copy(input_file, dest_path / input_file.name)
            else:
                subprocess.run(
                    [
                        "ncks",
                        "-O",
                        "-h",
                        "-d",
                        f"X,{self._args.X}",
                        "-d",
                        f"Y,{self._args.Y}",
                        input_file,
                        dest_path / input_file.name,
                    ]
                )

    def _write_slurm_runner(self):
        with open(get_project_root() / "templates" / "slurm_runner.sh") as file:
            template = Template(file.read())

        job_name = generate_random_string()
        slurm_runner = template.substitute(
            {
                "job_name": job_name,
                "partition": self._args.slurm_partition,
                "dvmdostem_binary": self.dvmdostem_bin_path,
                "log_file_path": self._args.output_path / f"{job_name}.log",
                "log_level": self._args.log_level,
                "config_path": Path(self._args.output_path / "config" / "config.js"),
                "p": self._args.p,
                "e": self._args.e,
                "s": self._args.s,
                "t": self._args.t,
                "n": self._args.n,
            }
        )

        with open(self._args.output_path / "slurm_runner.sh", "w") as file:
            file.write(slurm_runner)

    def _configure(self):
        config_file_path = Path(self._args.output_path / "config" / "config.js")
        with open(config_file_path) as f:
            config_data_str = f.read()

        config_data = clean_and_load_json(config_data_str)
        for key, val in IO_PATHS.items():
            config_data["IO"][key] = f"{self._args.output_path}/{val}"

        with open(config_file_path, "w") as f:
            json.dump(config_data, f, indent=4)

    def execute(self):
        if not self.dvmdostem_path.exists():
            raise Exception(
                "dvm-dos-tem folder needs to exist in /opt/apps directory. "
                f"Couldn't found in {self.dvmdostem_path}"
            )

        if not self._do_coords_in_range(
            Path(self._args.input_path / "drainage.nc"), self._args.X, self._args.Y
        ):
            raise Exception(
                "The given coordinates are out of bounds for the given dataset. Provided values are: "
                f"\nX: {self._args.X}"
                f"\nY: {self._args.Y}"
                f"\nInput Path: {self._args.input_path}"
            )

        self._copy_input_files()
        self._copy_folders()
        self._write_slurm_runner()
        self._configure()

        print("The given cell is successfully extracted.")
