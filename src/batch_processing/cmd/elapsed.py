import os
import subprocess
import sys
import time
from datetime import datetime

from batch_processing.cmd.base import BaseCommand


class ElapsedCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args
        # move this to exacloud file system
        self._file_path = "/home/dteber_woodwellclimate_org/elapsed_time.txt"
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
            # todo: log the error
            sys.exit(e)

        # continue the execution from the child process
        self.get_now_and_write("start datetime: ")
        print(f"Timer has started. Check {self._file_path} for the results.")
        while True:
            queue = subprocess.run(
                ["squeue", "--me", "--noheader"], stdout=subprocess.PIPE
            ).stdout.decode("utf-8")
            if not queue:
                self.get_now_and_write("end datetime: ")
                break

            time.sleep(self._sleep_time)

        print(f"The timer has stopped. Check {self._file_path} for the results.")
