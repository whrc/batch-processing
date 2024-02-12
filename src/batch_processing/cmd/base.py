import os
from abc import ABC, abstractmethod


class BaseCommand(ABC):
    def __init__(self):
        self.home_dir = os.getenv("HOME")
        self.user = os.getenv("USER")
        self.dvmdostem_bin_path = f"{self.home_dir}/dvm-dos-tem/dvmdostem"
        self.output_spec_path = f"{self.home_dir}/dvm-dos-tem/config/output_spec.csv"
        self.exacloud_user_dir = f"/mnt/exacloud/{self.user}"
        self.config_path = f"{self.home_dir}/dvm-dos-tem/config/config.js"
        self.input_dir = f"/mnt/exacloud/{self.user}/input"
        self.output_dir = f"/mnt/exacloud/{self.user}/output"
        self.slurm_log_dir = f"{self.exacloud_user_dir}/slurm-logs"

    @abstractmethod
    def execute(self):
        pass
