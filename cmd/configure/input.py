import argparse
import json
import re

from batch_processing.constants import CONFIG_PATH, HOME, IO_FILE_KEYS


def _modify_config(file_path: str) -> None:
    """
    Modifies the given config file by removing lines or parts of lines that
    contain comments. This function cleans `config.js` for JSON processing
    by removing JavaScript-style comments.
    """
    with open(file_path) as file:
        lines = file.readlines()

    # Regex pattern to match comments. It matches '//' and everything after it,
    # but does not match '//' that are within a string.
    # This regex assumes that you don't have strings containing '//' that
    # you want to keep.
    pattern = re.compile(r'(?<!: )"//.*|//.*')

    modified_lines = [pattern.sub("", line) for line in lines]

    # Optionally, remove any fully empty lines left after removing comments
    modified_lines = [line for line in modified_lines if line.strip()]

    with open(file_path, "w") as file:
        file.writelines(modified_lines)


def _map_input_files(config_file_path: str, input_path: str) -> None:
    """Maps the input files to the desired directory."""
    with open(config_file_path) as file:
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


# todo: update parameter_dir and output_spec_file as well in this function
def handle_configure(args: argparse.Namespace) -> None:
    # dvmdostem cannot interpret ~ (tilde) as the home directory
    if "~" in args.input_path:
        args.input_path = args.input_path.replace("~", HOME)

    _modify_config(CONFIG_PATH)
    _map_input_files(CONFIG_PATH, args.input_path)
