import logging
import os
import shutil
import subprocess
import sys
import time

from rich import print

from .base import BaseCommand


class RunCheckCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self.log_file_path = os.path.join(self.exacloud_user_dir, "run_check.out")

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        file_handler = logging.FileHandler(self.log_file_path)
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(file_formatter)

        self.logger.addHandler(file_handler)

        self._args = args
        self.keywords = ["error", "non-exit", "aborted"]

        self.resubmission_map = {}
        for log_file in self.get_log_files():
            batch_name, _ = os.path.splitext(log_file)
            self.resubmission_map[batch_name] = 3

    def get_log_files(self):
        files = os.listdir(self.slurm_log_dir)
        return [file for file in files if file.endswith(".out")]

    def contains_error(self, path_to_log_file):
        with open(path_to_log_file) as file:
            content = file.read()
            for target_word in self.keywords:
                if target_word in content:
                    return True

        return False

    def remove_output_for_batch(self, batch_name):
        output_path = os.path.join(self.batch_dir, batch_name, "output")
        try:
            shutil.rmtree(output_path)
            self.logger.debug(f"Removed the output/ folder in {batch_name}")
        except FileNotFoundError:
            self.logger.debug(
                f"The output/ folder in {batch_name} doesn't exist, nothing to remove."
            )

    def resubmit_batch(self, batch_name):
        remaining_resubmission_attempt = self.resubmission_map.get(batch_name)
        if remaining_resubmission_attempt == 0:
            self.logger.debug(
                f"{batch_name} is already re-submitted three times. "
                "4th resubmission will not happen."
            )
            return

        slurm_script_path = os.path.join(self.batch_dir, batch_name, "slurm_runner.sh")
        stdout = subprocess.check_output(["sbatch", slurm_script_path]).decode("utf-8")
        self.logger.debug(f"Re-submitted {batch_name}")
        self.logger.debug(f"'sbatch {slurm_script_path}' said: {stdout}")

        remaining_resubmission_attempt -= 1
        self.resubmission_map[batch_name] = remaining_resubmission_attempt
        self.logger.debug(
            f"The remaining resubmission attempt for {batch_name} is "
            f"{remaining_resubmission_attempt}"
        )

    def check(self):
        for log_file in self.get_log_files():
            file_path = os.path.join(self.slurm_log_dir, log_file)
            if not self.contains_error(file_path):
                continue

            batch_name, _ = os.path.splitext(log_file)
            self.logger.debug(f"Found something wrong with {batch_name}")
            self.remove_output_for_batch(batch_name)
            self.resubmit_batch(batch_name)

    def execute(self):
        try:
            pid = os.fork()
            # exit the main process
            if pid > 0:
                sys.exit(0)
        except OSError as e:
            self.logger.error(
                f"The fork operation is failed. Couldn't created the child process: {e}"
            )
            sys.exit(e)

        print(
            "[blue]The background job is started. Check[/blue] "
            f"[blue bold]{self.log_file_path}[/blue] [blue]for the logs.[/blue]"
        )
        self.logger.debug("run_check.py is initiated.")
        while True:
            time.sleep(1 * 60)
            self.check()
