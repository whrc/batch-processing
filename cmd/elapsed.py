import subprocess
import time
from cmd.base import BaseCommand
from datetime import datetime


class ElapsedCommand(BaseCommand):
    def __init__(self, args):
        self._args = args
        self._file_path = "/home/dteber_woodwellclimate_org/elapsed_time.txt"
        self._sleep_time = 10

    def get_now_and_write(self, s: str):
        now = datetime.now().ctime()
        with open(self._file_path, "a") as file:
            file.write(s + now + "\n")

    def execute(self):
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
