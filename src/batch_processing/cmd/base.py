import logging
import os
from abc import ABC, abstractmethod
from logging.handlers import FileHandler

from rich.logging import RichHandler


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
        self.output_dir = f"{self.exacloud_user_dir}/output"
        self.slurm_log_dir = f"{self.exacloud_user_dir}/slurm-logs"

        self.batch_dir = f"{self.output_dir}/batch-run"
        self.result_dir = f"{self.exacloud_user_dir}/all-merged"

        self.run_status_path = f"{self.batch_dir}/{{}}/output/run_status.nc"

        self.log_level = logging.DEBUG
        self.log_format = "%(message)s"
        self.log_file_path = f"{self.exacloud_user_dir}/batch-processing.log"
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(self.log_level)
        file_handler = FileHandler(self.log_file_path)
        file_handler.setFormatter(logging.Formatter(self.log_format))

        logging.basicConfig(
            format=self.log_format,
            level=self.log_level,
            handlers=[RichHandler(rich_tracebacks=True, markup=True), file_handler],
        )

    @abstractmethod
    def execute(self):
        pass

    def get_batch_folders(self) -> list:
        return os.listdir(self.batch_dir)

    def get_total_batch_count(self) -> int:
        return len(self.get_batch_folders())
