import os
from abc import ABC, abstractmethod


class BaseCommand(ABC):
    def __init__(self):
        self.user = os.getenv("USER")
        self.home_dir = os.getenv("HOME")

        self.dvmdostem_path = f"{self.home_dir}/dvm-dos-tem"
        self.dvmdostem_bin_path = f"{self.dvmdostem_path}/dvmdostem"

        # You might notice that the only variable which has a trailing
        # slash is in the below one. config.js file has it this way.
        # Therefore, I didn't remove it. The trailing slash can be
        # removed after making sure that `dvmdostem` is working
        # without a problem.
        self.parameters_path = f"{self.dvmdostem_path}/parameters/"
        self.config_path = f"{self.dvmdostem_path}/config/config.js"
        self.output_spec_path = f"{self.dvmdostem_path}/config/output_spec.csv"

        self.exacloud_user_dir = f"/mnt/exacloud/{self.user}"
        self.input_dir = f"{self.exacloud_user_dir}/input"
        self.output_dir = f"{self.exacloud_user_dir}/output"
        self.slurm_log_dir = f"{self.exacloud_user_dir}/slurm-logs"
        self.result_dir = f"{self.exacloud_user_dir}/all-merged"

        self.batch_dir = f"{self.output_dir}/batch-run"

    @abstractmethod
    def execute(self):
        pass
