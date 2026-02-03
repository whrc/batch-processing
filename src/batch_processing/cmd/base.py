import json
import os
from abc import ABC, abstractmethod
from pathlib import Path

CONFIG_FILE_PATH = Path.home() / ".bpconfig"
DEFAULT_BASEDIR = "/opt/apps"
DVMDOSTEM_FOLDER = "dvm-dos-tem"


def get_basedir_from_config() -> str:
    """Read basedir from config file if it exists."""
    if CONFIG_FILE_PATH.exists():
        try:
            with open(CONFIG_FILE_PATH, "r") as f:
                config = json.load(f)
                return config.get("basedir", DEFAULT_BASEDIR)
        except (json.JSONDecodeError, IOError):
            pass
    return DEFAULT_BASEDIR


class BaseCommand(ABC):
    def __init__(self, basedir: str = None):
        self.user = os.getenv("USER")
        self.home_dir = os.getenv("HOME")

        # Use provided basedir, or read from config, or use default
        if basedir is None:
            basedir = get_basedir_from_config()
        self.dvmdostem_path = Path(basedir) / DVMDOSTEM_FOLDER
        self.dvmdostem_bin_path = f"{self.dvmdostem_path}/dvmdostem"

        # For versions of dvmdostem >= v0.8.3-85-g465d6165
        # Previous versions had the scripts folder in a different location.
        self.dvmdostem_scripts_path = f"{self.dvmdostem_path}/pyddt/src/pyddt/"

        # You might notice that the only variable which has a trailing
        # slash is in the below one. config.js file has it this way.
        # Therefore, I didn't remove it. The trailing slash can be
        # removed after making sure that `dvmdostem` is working
        # without a problem.
        self.parameters_path = f"{self.dvmdostem_path}/parameters/"
        self.config_path = f"{self.dvmdostem_path}/config/config.js"
        self.output_spec_path = f"{self.dvmdostem_path}/config/output_spec.csv"

        self.exacloud_user_dir = f"/mnt/exacloud/{self.user}"
        self.output_dir = f"{self.exacloud_user_dir}/output"
        self.slurm_log_dir = f"{self.exacloud_user_dir}/slurm-logs"

        self.batch_dir = f"{self.output_dir}/batch-run"
        self.result_dir = f"{self.exacloud_user_dir}/all-merged"

        self.run_status_path = f"{self.batch_dir}/{{}}/output/run_status.nc"

    @abstractmethod
    def execute(self):
        pass

    def get_batch_folders(self) -> list:
        return os.listdir(self.batch_dir)

    def get_total_batch_count(self) -> int:
        return len(self.get_batch_folders())
