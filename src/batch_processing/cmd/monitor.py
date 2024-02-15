from __future__ import annotations

import json
import logging
import os
import shutil
import sys
import time

from google.cloud import compute_v1

from batch_processing.utils.utils import get_slurm_queue

from .base import BaseCommand

logger = logging.getLogger(__name__)


class MonitorCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args

        self._instance_status_mapping = {}
        self._max_machine_count = args.instance_count
        self._machine_names = [
            f"slurmlustr-spot-ghpc-{i}" for i in range(0, self._max_machine_count)
        ]
        self._sleep_time = 5

        self._log_file_path = f"{self.exacloud_user_dir}/monitor.log"
        logging.basicConfig(
            filename=self._log_file_path,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        logger.setLevel(logging.DEBUG)

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

        self._start_monitoring()

    def _start_monitoring(self):
        print(
            "Monitoring has started. "
            f"You can check {self._log_file_path} for the logs"
        )
        while True:
            logger.debug("Checking the queue...")
            queue = get_slurm_queue()
            if not queue:
                logger.debug("The queue is empty. Monitoring is finished.")
                break

            logger.debug("Checking the instances...")
            self._check_instances()

            time.sleep(self._sleep_time)

    def _check_instances(self):
        self._update_instance_mapping()
        for name, status in self._instance_status_mapping.items():
            if status == "TERMINATED":
                logger.debug(f"{name} is terminated.")
                batch_number = self._get_batch_number(name)
                logger.debug(f"The batch number of the executed job is {batch_number}")
                # this line is broken into two lines due to it's being
                # too long but it is actually a one full line.
                output_folder_path = (
                    f"/mnt/exacloud/{self.user}/output"
                    f"/batch-run/batch-{batch_number}/output"
                )

                try:
                    # remove the output folder for this batch because the machine
                    # that is running this batch is pre-maturely terminated
                    shutil.rmtree(output_folder_path)

                # the file is already deleted, so we can continue
                # we can hit this line if we check the instances too often
                except FileNotFoundError:
                    pass

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

    def _get_batch_number(self, instance_name):
        queue = get_slurm_queue(["--json"])
        queue = json.loads(queue)
        jobs = queue.get("jobs")
        batch_number = None
        for job in jobs:
            if job.get("nodes") == instance_name:
                # state = job.get("job_state")
                name = job.get("name")
                batch_number = name.split("-")[-1]

        return batch_number
