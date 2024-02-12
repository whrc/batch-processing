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

logger = logging.getLogger(__name__)


class MonitorCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args

        # the below three variables are responsible for storing the process id
        # of the child process after the fork operation
        self._file_name = "monitor_pid"
        self._dir_path = f"{self.home_dir}/.batch-processing"
        self._file_path = f"{self._dir_path}/{self._file_name}"

        self._log_file_path = f"{self.exacloud_user_dir}/monitor.log"

        self._instance_status_mapping = {}
        self._max_machine_count = args.instance_count
        self._machine_names = [
            f"slurmlustr-spot-ghpc-{i}" for i in range(0, self._max_machine_count)
        ]

        logging.basicConfig(
            filename=self._log_file_path,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

    # todo: we can automatically check the queue and stop the monitoring
    # that way we'd eliminate the flags and file write operations
    def execute(self):
        if self._args.start:
            print(
                "Monitoring has started. "
                f"You can check {self._log_file_path} for the logs"
            )
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

        with open(f"{self._dir_path}/{self._file_name}", "w") as file:
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
            if status == "TERMINATED":
                logger.debug(f"{name} is terminated.")
                batch_number = self._get_batch_number(name)
                output_folder_path = (
                    f"/mnt/exacloud/{self.user}/output/batch-run/batch-{batch_number}"
                )

                # remove the output folder for this batch because the machine
                # that is running this batch is pre-maturely terminated
                shutil.rmtree(output_folder_path)
                self._instance_status_mapping[name] = ""
                logger.debug(f"{name}'s output folder is deleted.")
                logger.debug("_instance_status_mapping looks like this\n")
                logger.debug(self._instance_status_mapping)

        logger.debug(self._instance_status_mapping)

    def _update_instance_mapping(
        self,
        project_id: str = "spherical-berm-323321",
        zone: str = "us-central1-c",
    ) -> None:
        instance_client = compute_v1.InstancesClient()
        instance_list = instance_client.list(project=project_id, zone=zone)

        logger.debug("Updating _instance_status_mapping")
        for instance in instance_list:
            # skip the VMs we are not interested in
            if instance.name not in self._machine_names:
                continue

            self._instance_status_mapping[instance.name] = instance.status

        logger.debug(
            f"_instance_status_mapping is updated\n\n{self._instance_status_mapping}"
        )

    def _get_queue(self):
        return subprocess.check_output(["squeue", "--me", "--json", "--noheader"])

    def _get_batch_number(self, instance_name):
        queue = self._get_queue()
        queue = json.loads(queue)
        jobs = queue.get("jobs")
        batch_number = None
        for job in jobs:
            if job.get("nodes") == instance_name:
                # state = job.get("job_state")
                name = job.get("name")
                batch_number = name.split("-")[-1]

        return batch_number
