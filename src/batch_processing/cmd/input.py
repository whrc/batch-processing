import json

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import clean_and_load_json


class InputCommand(BaseCommand):
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
        super().__init__()
        # dvmdostem cannot interpret ~ (tilde) as the home directory
        if "~" in args.input_path:
            args.input_path = args.input_path.replace("~", self.home_dir)

        self._args = args

    def execute(self):
        with open(self.config_path) as file:
            file_content = file.read()

        # the config file contains comments which are not valid
        # therefore, we are removing them before parsing
        config = clean_and_load_json(file_content)

        io_json = config["IO"]
        for key in self.IO_FILE_KEYS:
            value = io_json[key]
            file_name = value.split("/")[-1]
            if not self._args.input_path.endswith("/"):
                self._args.input_path += "/"
            io_json[key] = self._args.input_path + file_name

        io_json["parameter_dir"] = self.parameters_path
        io_json["output_dir"] = self.output_dir
        io_json["output_spec_file"] = self.output_spec_path

        with open(self.config_path, "w") as file:
            json.dump(config, file, indent=2)

        print("config.js is updated according to the provided input file.")
        print(f"You can check the file via: cat {self.config_path}")
