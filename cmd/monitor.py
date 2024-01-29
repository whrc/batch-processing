from __future__ import annotations

import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

from google.cloud import compute_v1

from .base import BaseCommand

logging.basicConfig(
    filename="/home/dteber_woodwellclimate_org/monitor.log",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger()


MAX_INSTANCE_COUNT = 16
TERMINATED = "TERMINATED"
NOT_FOUND = "NOT FOUND"


def get_queue():
    return subprocess.check_output(["squeue", "--json", "--noheader"])


def get_batch_number(instance_name):
    queue = get_queue()
    queue = json.loads(queue)
    jobs = queue.get("jobs")
    batch_number = None
    for job in jobs:
        if job.get("nodes") == instance_name:
            # state = job.get("job_state")
            name = job.get("name")
            batch_number = name.split("-")[-1]

    return batch_number


# todo: move this to utils.py
def delete_output_folder(path):
    logger.debug(f"Deleting the output dir for {path}")
    shutil.rmtree(path)


class MonitorCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args
        self._file_name = "monitor_pid"
        self._dir_path = f"{self.home_dir}/.batch-processing"

        self._instance_status_mapping = {}
        # we might get this value from a config file or from cli
        self._max_machine_count = 16
        self._machine_names = [
            f"slurmlustr-spot-ghpc-{i}" for i in range(0, self._max_machine_count)
        ]

    def execute(self):
        if self._args.start:
            # todo: print a message saying that the monitoring has started
            # and it can be followed from ... log file
            self._start_monitoring()
        elif self._args.stop:
            self._stop_monitoring()

    def _start_monitoring(self):
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError as e:
            logger.error(
                f"fork failed: {e.errno} ({e.strerror})" % (e.errno, e.strerror)
            )
            sys.exit(1)

        Path(self._dir_path).mkdir(exist_ok=True)
        # we might not need to change directory here
        os.chdir(self._dir_path)

        with open(self._file_name, "w") as file:
            file.write(str(os.getpid()))

        os.setsid()
        os.umask(0)

        self._monitor()

    def _stop_monitoring(self):
        file_path = f"{self._dir_path}/{self._file_name}"
        with open(file_path) as file:
            pid = int(file.read())
            os.kill(pid, signal.SIGTERM)

    def _monitor(self):
        while True:
            logger.debug("Checking the instances...")
            self._check_instances()
            time.sleep(5)

    def _check_instances(self):
        self._update_instance_mapping()
        for name, status in self._instance_status_mapping.items():
            if status == TERMINATED:
                logger.debug(f"{name} is terminated.")
                batch_number = get_batch_number(name)
                user = os.getenv("USER")
                output_folder_path = (
                    f"/mnt/exacloud/{user}/output/batch-run/batch-{batch_number}"
                )
                delete_output_folder(output_folder_path)
                self._instance_status_mapping[name] = ""

        logger.debug(self._instance_status_mapping)

    def _update_instance_mapping(
        self,
        project_id: str = "spherical-berm-323321",
        zone: str = "us-central1-c",
    ) -> None:
        instance_client = compute_v1.InstancesClient()
        instance_list = instance_client.list(project=project_id, zone=zone)

        logger.debug(f"Instances found in zone {zone}:")
        for instance in instance_list:
            # skip the VMs we are not interested in
            if instance.name not in self._machine_names:
                continue

            self._instance_status_mapping[instance.name] = instance.status

        logger.debug(
            f"Instance status mapping is updated: {self._instance_status_mapping}"
        )
