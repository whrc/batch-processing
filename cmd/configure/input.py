import os
import json

TAB_SIZE = 8
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


def modify_config(file_path: str) -> None:
    """
    Modifies the given config file by modifiying the lines that contains invalid JSON

    `config.js` contains comments which is not allowed in the JSON syntax. In order to
    manipulate this file as JSON, one needs to clean it up. This function is a utility
    function that cleans `config.js` up for JSON processing.
    """
    with open(file_path, "r") as file:
        lines = file.readlines()

        modified_lines = []
        for line in lines:
            # todo: replace this with regex
            index = line.find("//")
            # Generally, the lines that contain comment in 0th or 8th column
            # is a full-line comment. That's why we are ignoring them.
            if index >= 0 and index < TAB_SIZE:
                continue
            elif index > TAB_SIZE:
                line = line[:index] + "\n"

            modified_lines.append(line)

    with open(file_path, "w") as file:
        file.writelines(modified_lines)


def map_input_files(config_file_path: str, input_path: str) -> None:
    """Maps the input files to the desired directory."""
    with open(config_file_path, "r") as file:
        config = json.load(file)

        io_json = config["IO"]
        for key in IO_FILE_KEYS:
            value = io_json[key]
            file_name = value.split("/")[-1]
            if not input_path.endswith("/"):
                input_path += "/"
            io_json[key] = input_path + file_name

    with open(config_file_path, "w") as file:
        json.dump(config, file, indent=2)


def handle_configure(args):
    home = os.getenv("HOME")
    # dvmdostem cannot interpret ~ (tilde) as the home directory
    if "~" in args.input_path:
        args.input_path = args.input_path.replace("~", home)

    config_file_path = f"{home}/dvm-dos-tem/config/config.js"
    modify_config(config_file_path)
    map_input_files(config_file_path, args.input_path)
