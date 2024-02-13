import os
import sys
import time
from datetime import datetime

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import get_slurm_queue


class ElapsedCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args
        self._file_path = f"{self.exacloud_user_dir}/elapsed_time.txt"
        self._sleep_time = 10

    def get_now_and_write(self, s: str):
        now = datetime.now().ctime()
        with open(self._file_path, "a") as file:
            file.write(s + now + "\n")

    def execute(self):
        try:
            pid = os.fork()
            # exit the main process
            if pid > 0:
                sys.exit(0)
        except OSError as e:
            print(
                f"The fork operation is failed. Couldn't created the child process: {e}"
            )
            sys.exit(e)

        # continue the execution from the child process
        self.get_now_and_write("start datetime: ")
        print(f"Timer has started. Check {self._file_path} for the results.")
        while True:
            queue = get_slurm_queue()
            if not queue:
                self.get_now_and_write("end datetime: ")
                break

            time.sleep(self._sleep_time)

        print(f"The timer has stopped. Check {self._file_path} for the results.")
