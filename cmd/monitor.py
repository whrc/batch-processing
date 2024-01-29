from __future__ import annotations

import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import Iterable
from pathlib import Path

from google.cloud import compute_v1

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


def delete_output_folder(path):
    logger.debug(f"Deleting the output dir for {path}")
    shutil.rmtree(path)


def get_instance_mapping(
    instance_names: list,
    project_id: str = "spherical-berm-323321",
    zone: str = "us-central1-c",
) -> Iterable[compute_v1.Instance]:
    """List all instances in the given zone in the specified project.

    Args:
        project_id: project ID or project number of the Cloud project you want to use.
        zone: name of the zone you want to use.
    Returns:
        An iterable collection of Instance objects.
    """
    instance_client = compute_v1.InstancesClient()
    instance_list = instance_client.list(project=project_id, zone=zone)

    logger.debug(f"Instances found in zone {zone}:")
    instance_mapping = {}
    for instance in instance_list:
        # skip the VMs we are not interested in
        if instance.name not in instance_names:
            continue

        instance_mapping[instance.name] = instance.status

    logger.debug(f"The latest mapping: {instance_mapping}")
    return instance_mapping


def check_instances(instances):
    instance_status_mapping = get_instance_mapping(instances)
    for name, status in instance_status_mapping.items():
        if status == TERMINATED:
            logger.debug(f"{name} is terminated.")
            batch_number = get_batch_number(name)
            user = os.getenv("USER")
            output_folder_path = (
                f"/mnt/exacloud/{user}/output/batch-run/batch-{batch_number}"
            )
            delete_output_folder(output_folder_path)
            instance_status_mapping[name] = ""

    logger.debug(instance_status_mapping)


def monitor():
    instance_names = []
    for i in range(0, MAX_INSTANCE_COUNT):
        instance_names.append(f"slurmlustr-spot-ghpc-{i}")

    while True:
        logger.debug("Checking the instances...")
        check_instances(instance_names)
        time.sleep(5)


def handle_monitoring(args):
    if args.start:
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError as e:
            print(f"fork failed: {e.errno} ({e.strerror})" % (e.errno, e.strerror))
            sys.exit(1)

        Path(f"{os.getenv('HOME')}/.batch-processing").mkdir(exist_ok=True)
        os.chdir(f"{os.getenv('HOME')}/.batch-processing")

        with open("monitor_pid", "w") as file:
            file.write(str(os.getpid()))

        os.setsid()
        os.umask(0)

        monitor()

    elif args.stop:
        with open(f"{os.getenv('HOME')}/.batch-processing/monitor_pid") as file:
            pid = int(file.read())
            os.kill(pid, signal.SIGTERM)
