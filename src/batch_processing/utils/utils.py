import errno
import json
import os
import re
import subprocess
from pathlib import Path

from google.cloud import storage


def run_command(command: list) -> None:
    """Executes a shell command."""
    subprocess.run(command, check=True)


def mkdir_p(path: str) -> None:
    """Provides similar functionality to bash mkdir -p"""
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def download_directory(bucket_name: str, blob_name: str) -> None:
    """Downloads a directory from Google Cloud Storage.

    Args:
        bucket_name (str): Bucket name
        blob_name (str): The full path of the desired directory

    Example:
        Consider the below `gsutil URI`:

        gs://wcrc-tfstate-9486302/slurm-lustre-dvmdostem-v5/slurm-lustre-dvmdostem-v5/primary

        In the above URI, `wcrc-tfstate-9486302` is the bucket name and
        `slurm-lustre-dvmdostem-v5/slurm-lustre-dvmdostem-v5/primary` is the blob_name.
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=blob_name)
    for blob in blobs:
        if blob.name.endswith("/"):
            continue
        file_split = blob.name.split("/")
        directory = "/".join(file_split[0:-1])
        absolute_directory = f"{os.getenv('HOME')}/{directory}"
        Path(absolute_directory).mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(f"{os.getenv('HOME')}/{blob.name}")


def download_file(bucket_name: str, blob_name: str, output_file_name: str) -> None:
    """
    Downloads a file from a Google Cloud Storage bucket to a local file.

    This function retrieves a blob from the specified bucket in Google Cloud Storage
    and downloads it to a local file. The local file is saved with the specified output
    file name.

    Parameters:
    - bucket_name (str): The name of the Google Cloud Storage bucket from which to
    download the file.
    - blob_name (str): The name of the blob (file) within the bucket to download.
    - output_file_name (str): The name (including path) under which the file should be
    saved locally.
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(blob_name)
    blob.download_to_filename(output_file_name)


def clean_and_load_json(input: str) -> dict:
    """
    Cleans comments from JSON-formatted string and loads it into a Python object.

    Args:
        input (str): Input JSON-formatted string possibly containing comments.

    Returns:
        dict: Python dictionary representing the JSON data.
    """
    cleaned_str = re.sub("//.*\n", "\n", input)
    json_data = json.loads(cleaned_str)
    return json_data
