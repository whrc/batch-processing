import logging
import os
import shutil
import subprocess
import sys
import time

from rich.logging import RichHandler

from .base import BaseCommand

logger = logging.getLogger(__name__)

fmt_handler = "%(message)s"
handler_formatter = logging.Formatter(fmt_handler)

handler = RichHandler()
handler.setFormatter(handler_formatter)

logger.addHandler(handler)


class RunCheckCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        logging.basicConfig(
            level=logging.NOTSET,
            filename=f"{os.path.join(self.exacloud_user_dir, "run_check.py")}",
        )

        self._args = args
        self.keywords = ["error", "non-exit", "aborted"]

        self.resubmission_map = {}
        files = os.listdir(self.slurm_log_dir)
        log_files = [file for file in files if file.endswith(".out")]
        for log_file in log_files:
            batch_name, _ = os.path.splitext(log_file)
            self.resubmission_map[batch_name] = 3

    def contains_error(self, path_to_log_file):
        with open(path_to_log_file) as file:
            content = file.read()
            for target_word in self.keywords:
                if target_word in content:
                    return True

        return False

    def remove_output_for_batch(self, batch_name):
        output_path = os.path.join(self.batch_dir, batch_name, "output")
        shutil.rmtree(output_path)
        logger.info(f"Removed the output/ folder in {batch_name}")

    def resubmit_batch(self, batch_name):
        remaining_resubmission_attempt = self.resubmission_map.get(batch_name)
        if remaining_resubmission_attempt == 0:
            logger.info(
                f"{batch_name} is already re-submitted three times. "
                "4th resubmission will not happen."
            )
            return

        slurm_script_path = os.path.join(self.batch_dir, batch_name, "slurm_runner.sh")
        stdout = subprocess.check_output(["sbatch", slurm_script_path])
        logger.info(f"Re-submitted {batch_name}")
        logger.info(f"'sbatch {slurm_script_path}' said: {stdout}")

        remaining_resubmission_attempt -= 1
        self.resubmission_map[batch_name] = remaining_resubmission_attempt
        logger.info(
            f"The remaining resubmission attempt for {batch_name} is "
            f"{remaining_resubmission_attempt}"
        )

    def check(self):
        files = os.listdir(self.slurm_log_dir)
        log_files = [file for file in files if file.endswith(".out")]
        for log_file in log_files:
            file_path = os.path.join(self.slurm_log_dir, log_file)
            if not self.contains_error(file_path):
                continue

            batch_name, _ = os.path.splitext(log_file)
            logger.info(f"Found something wrong with {batch_name}")
            self.remove_output_for_batch(batch_name)
            self.resubmit_batch(batch_name)

    def execute(self):
        try:
            pid = os.fork()
            # exit the main process
            if pid > 0:
                sys.exit(0)
        except OSError as e:
            logger.error(
                f"The fork operation is failed. Couldn't created the child process: {e}"
            )
            sys.exit(e)

        logger.info("run_check.py is initiated.")
        while True:
            self.check()
            time.sleep(1 * 60)
