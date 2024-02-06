import json
import os
import re
from cmd.base import BaseCommand

from batch_processing.constants import CONFIG_PATH


class ConfigureInputCommand(BaseCommand):
    IO_FILE_KEYS = [
        "hist_climate_file",
        "proj_climate_file",
        "veg_class_file",
        "drainage_file",
        "soil_texture_file",
        "co2_file",
        "proj_co2_file",
        "runmask_file",
        "topo_file",
        "fri_fire_file",
        "hist_exp_fire_file",
        "proj_exp_fire_file",
        "topo_file",
    ]

    def __init__(self, args):
        # dvmdostem cannot interpret ~ (tilde) as the home directory
        if "~" in args.input_path:
            args.input_path = args.input_path.replace("~", os.getenv("HOME"))

        self._args = args

    def execute(self):
        with open(CONFIG_PATH) as file:
            file_content = file.read()

        config = json.loads(re.sub("//.*\n", "\n", file_content))

        io_json = config["IO"]
        for key in self.IO_FILE_KEYS:
            value = io_json[key]
            file_name = value.split("/")[-1]
            if not self._args.input_path.endswith("/"):
                self._args.input_path += "/"
            io_json[key] = self._args.input_path + file_name

        io_json["parameter_dir"] = f"{os.getenv('HOME')}/dvm-dos-tem/parameters/"
        io_json["output_dir"] = f"/mnt/exacloud/{os.getenv('USER')}/output"
        io_json[
            "output_spec_file"
        ] = f"{os.getenv('HOME')}/dvm-dos-tem/config/output_spec.csv"

        with open(CONFIG_PATH, "w") as file:
            json.dump(config, file, indent=2)

        print("config.js is updated according to the provided input file.")
        print(f"You can check the file via: cat {CONFIG_PATH}")
