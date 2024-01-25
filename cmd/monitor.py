import os
import json
import sys
import time
import shutil
import subprocess
import logging
import signal
from pathlib import Path


logging.basicConfig(
    filename="/home/dteber_woodwellclimate_org/monitor.log",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger()


MAX_INSTANCE_COUNT = 16
TERMINATED = "TERMINATED"

instance_mapping = {}
instance_names = []
for i in range(0, MAX_INSTANCE_COUNT):
    instance_names.append(f"slurmlustr-spot-ghpc-{i}")


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


def descibe_instance(instance_name):
    try:
        return subprocess.check_output(
            [
                "gcloud",
                "compute",
                "instances",
                "describe",
                instance_name,
                "--format=json(status)",
                "--zone=us-central1-c",
            ]
        )
    except subprocess.CalledProcessError as e:
        logger.error(f"Something went wrong when running the command: {e}")
        return None


def get_instance_status(output):
    data = json.loads(output)
    return data.get("status")


def check_instances():
    for name in instance_names:
        output = descibe_instance(name)
        if output:
            status = get_instance_status(output)
            instance_mapping[name] = status
            if status == TERMINATED:
                logger.debug(f"{name} is terminated.")
                batch_number = get_batch_number(name)
                user = os.getenv("USER")
                output_folder_path = (
                    f"/mnt/exacloud/{user}/output/batch-run/batch-{batch_number}"
                )
                delete_output_folder(output_folder_path)
                instance_mapping[name] = ""


def monitor():
    while True:
        check_instances()
        logger.debug(instance_mapping)
        time.sleep(5)


def handle_monitoring(args):
    if args.start:
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError as e:
            print >> sys.stderr, "fork failed: %d (%s)" % (e.errno, e.strerror)
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
