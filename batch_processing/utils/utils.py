import os
import subprocess
import errno
from pathlib import Path

from google.cloud import storage


def run_command(command):
    """Executes a shell command."""
    subprocess.run(command, check=True)


def mkdir_p(path):
    """Provides similar functionality to bash mkdir -p"""
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def download_directory(bucket_name: str, prefix: str) -> None:
    """Downloads a directory from Google Cloud Storage.

    Args:
        bucket_name (str): Bucket name
        prefix (str): The full path of the desired directory

    Example:
        Consider the below `gsutil URI`:

        gs://wcrc-tfstate-9486302/slurm-lustre-dvmdostem-v5/slurm-lustre-dvmdostem-v5/primary

        In the above URI, `wcrc-tfstate-9486302` is the bucket name and
        `slurm-lustre-dvmdostem-v5/slurm-lustre-dvmdostem-v5/primary` is the prefix.
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        if blob.name.endswith("/"):
            continue
        file_split = blob.name.split("/")
        directory = "/".join(file_split[0:-1])
        absolute_directory = f"{os.getenv('HOME')}/{directory}"
        Path(absolute_directory).mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(f"{os.getenv('HOME')}/{blob.name}")
