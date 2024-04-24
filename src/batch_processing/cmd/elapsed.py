import os
import sys
import time
from datetime import datetime

from rich import print

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import get_slurm_queue


class ElapsedCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args
        self._file_path = f"{self.exacloud_user_dir}/elapsed_time.txt"
        self._sleep_time = 60

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
                "[red bold]The fork operation is failed. "
                f"Couldn't created the child process: {e}[/red bold]"
            )
            sys.exit(e)

        # continue the execution from the child process
        start_time = datetime.now()
        with open(self._file_path, "a") as file:
            file.write("start datetime: " + start_time.ctime() + "\n")

        print(
            "[blue bold]Timer has started. "
            f"Check {self._file_path} for the results.[/blue bold]"
        )
        while True:
            queue = get_slurm_queue()
            if not queue:
                end_time = datetime.now()
                time_difference = end_time - start_time
                days = time_difference.days
                hours, remainder = divmod(time_difference.seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                formatted_time = (
                    f"{days} days, {hours} hours, {minutes} minutes, {seconds} seconds"
                )

                with open(self._file_path, "a") as file:
                    file.write("end datetime: " + end_time.ctime() + "\n")
                    file.write("elapsed time: " + formatted_time + "\n")

                break

            time.sleep(self._sleep_time)

        print(
            "[green bold]The timer has stopped. "
            f"Check {self._file_path} for the results.[/green bold]"
        )
