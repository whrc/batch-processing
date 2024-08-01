import errno
import json
import os
import random
import re
import string
import subprocess
from dataclasses import dataclass
from pathlib import Path
from string import Template
from subprocess import CompletedProcess
from typing import List, Union

import cftime
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from google.cloud import storage
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
)

INPUT_FILES = [
    "co2.nc",
    "projected-co2.nc",
    "drainage.nc",
    "fri-fire.nc",
    "run-mask.nc",
    "soil-texture.nc",
    "topo.nc",
    "vegetation.nc",
    "historic-explicit-fire.nc",
    "projected-explicit-fire.nc",
    "projected-climate.nc",
    "historic-climate.nc",
]

INPUT_FILES_TO_COPY = ["co2.nc", "projected-co2.nc"]

IO_PATHS = {
    "parameter_dir": "parameters/",
    "output_dir": "output/",
    "output_spec_file": "config/output_spec.csv",
    "runmask_file": "input/run-mask.nc",
    "hist_climate_file": "input/historic-climate.nc",
    "proj_climate_file": "input/projected-climate.nc",
    "veg_class_file": "input/vegetation.nc",
    "drainage_file": "input/drainage.nc",
    "soil_texture_file": "input/soil-texture.nc",
    "co2_file": "input/co2.nc",
    "proj_co2_file": "input/projected-co2.nc",
    "topo_file": "input/topo.nc",
    "fri_fire_file": "input/fri-fire.nc",
    "hist_exp_fire_file": "input/historic-explicit-fire.nc",
    "proj_exp_fire_file": "input/projected-explicit-fire.nc",
}


@dataclass
class Chunk:
    id: int
    start: int
    end: int


def create_chunks(total_size: int, num_chunks: int) -> List[Chunk]:
    """
    Create chunk boundaries for slicing the dataset.

    Parameters:
    total_size (int): The total size of the dimension to be chunked.
    num_chunks (int): The number of chunks to create.

    Returns:
    List[Chunk]: A list of Chunk instances, each containing the chunk index,
        start index, and end index.
    """
    if num_chunks <= 0:
        raise ValueError("num_chunks must be a positive integer")

    chunk_size = total_size // num_chunks
    chunks = []

    for i in range(num_chunks):
        start = i * chunk_size
        end = start + chunk_size if i < num_chunks - 1 else total_size
        chunks.append(Chunk(i, start, end))

    return chunks


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


def remove_file(file: Union[str, list]):
    """Remove the specified file or list of files.

    Parameters:
        file (str or list): File path or list of file paths to be removed.

    Returns:
        None
    """
    if isinstance(file, str):
        os.remove(file)
        return

    if isinstance(file, list):
        _ = [os.remove(f) for f in file]


def download_directory(bucket_name: str, blob_name: str, output_path: str) -> None:
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
        absolute_directory = f"{output_path}/{directory}"
        Path(absolute_directory).mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(f"{output_path}/{blob.name}")


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


def get_slurm_queue(params: list = []) -> str:
    command = ["squeue", "--me", "--noheader"]
    command.extend(params)

    return subprocess.check_output(command).decode("utf-8")


def static_map(monthly_GPP_tr, monthly_GPP_sc, output, file_name):
    # Calculate the GPP means for 2000-2020
    a = (
        monthly_GPP_tr.sel(time=slice("2000", "2015"))
        .resample(time="YS")
        .sum(dim="time")
    )
    b = (
        monthly_GPP_sc.sel(time=slice("2016", "2020"))
        .resample(time="YS")
        .sum(dim="time")
    )
    gpp_mean_2000_2020 = xr.concat([a, b], dim="time")
    gpp_mean_2000_2020 = gpp_mean_2000_2020.mean(dim="time", keepdims=True)

    # Calculate the GPP means for 2040-2060 and 2080-2100
    gpp_mean_2040_2060 = (
        monthly_GPP_sc.sel(time=slice("2040", "2060"))
        .resample(time="YS")
        .sum(dim="time")
        .mean(dim="time")
    )
    gpp_mean_2080_2100 = (
        monthly_GPP_sc.sel(time=slice("2080", "2100"))
        .resample(time="YS")
        .sum(dim="time")
        .mean(dim="time")
    )

    # Create a plot with 3 subplots with uniform colorbars
    fig, axes = plt.subplots(ncols=3, figsize=(12, 4), constrained_layout=True)
    vmin = np.min(
        [gpp_mean_2000_2020.min(), gpp_mean_2040_2060.min(), gpp_mean_2080_2100.min()]
    )
    vmax = np.max(
        [gpp_mean_2000_2020.max(), gpp_mean_2040_2060.max(), gpp_mean_2080_2100.max()]
    )

    # Plot the mean GPP value for each time period
    colormap = "YlGn"
    gpp_mean_2000_2020.plot(
        ax=axes[0], cmap=colormap, add_colorbar=False, vmin=vmin, vmax=vmax
    )
    gpp_mean_2040_2060.plot(
        ax=axes[1], cmap=colormap, add_colorbar=False, vmin=vmin, vmax=vmax
    )
    gpp_mean_2080_2100.plot(
        ax=axes[2], cmap=colormap, add_colorbar=False, vmin=vmin, vmax=vmax
    )

    # Add titles and labels to the subplots
    axes[0].set_title("2000-2020")
    axes[1].set_title("2040-2060")
    axes[2].set_title("2080-2100")
    # axes[0].set_ylabel('Y')
    # axes[1].set_ylabel('Y')
    # axes[2].set_ylabel('Y')
    # axes[0].set_xlabel('X')
    # axes[1].set_xlabel('X')
    # axes[2].set_xlabel('X')

    # Add a colorbar to the figure
    fig.colorbar(
        axes[2].collections[0],
        ax=axes,
        orientation="horizontal",
        label="Average Yearly Spatial " + output,
    )
    fig.suptitle(("Mean " + output), fontsize=20)

    plt.savefig(file_name)


def static_timeseries(data_tr, data_sc, output, type_var, type_spread, file_name):
    """
    output = 'GPP' or other variable of interest contained in dataframe
    type_var = 'mean' or 'sum'
    type_spread = 'std' or 'var'
    """
    plt.style.use("bmh")
    if type_spread == "std":
        spreadtext = "Standard Deviation"
    else:
        spreadtext = "Variance"

    # Convert the time coordinate to a regular datetime format
    data_tr["time"] = [
        cftime.datetime(t.year, t.month, t.day) for t in data_tr.time.values
    ]
    data_sc["time"] = [
        cftime.datetime(t.year, t.month, t.day) for t in data_sc.time.values
    ]

    # Group the data by year and compute the mean for each year
    a = data_tr.sel(time=slice("2000", "2015")).groupby("time.year").mean(dim="time")
    b = data_sc.groupby("time.year").mean(dim="time")
    annual_means = xr.concat([a, b], dim="time")
    # annual_means = monthly_GPP_sc.groupby('time.year').mean(dim='time')

    df = annual_means.to_dataframe().reset_index()

    # Group the data by year and calculate the sum and variance of GPP
    df_grouped = df.groupby("year").agg({output: [type_var, type_spread]}).reset_index()

    # Extract the sum and variance columns
    gpp_sum = df_grouped[output][
        type_var
    ]  # this is mean of gpp over all locations - do we want sum or mean??
    gpp_std = df_grouped[output][
        type_spread
    ]  # this is std of each year over all locations

    # Create the plot
    fig, ax = plt.subplots()

    # Add the shaded region for the variance
    y1 = gpp_sum - (gpp_std)
    y2 = gpp_sum + (gpp_std)
    ax.fill_between(
        df_grouped["year"],
        y1,
        y2,
        color="#fcaa0f",
        alpha=0.25,
        interpolate=True,
        label=spreadtext,
    )
    ax.plot(
        df_grouped["year"],
        gpp_sum,
        color="#9f2a63",
        label="Mean " + output + " over all locations/year",
    )
    # ax.plot(time, gpp_var, label="Standard Deviation")

    # Set the axis labels and title
    # ax.set_yscale('log')
    ax.set_xlabel("Time")
    ax.set_ylabel("Averaged " + output)
    ax.set_title(output + " over Time with " + spreadtext)
    ax.legend()
    plt.savefig(file_name)


def get_progress_bar():
    return Progress(
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("•"),
        TimeElapsedColumn(),
    )


def get_project_root() -> Path:
    """Returns the project root."""
    return Path(__file__).parent.parent


def interpret_path(path: str) -> str:
    """Converts any given relative path to an absolute path."""
    path = os.path.expanduser(path)

    return os.path.abspath(path)


def generate_random_string(N=5):
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=N))


def get_dimensions(file_name: str) -> Union[int, int]:
    """Retrieve the dimensions sizes from the given NetCDF file."""
    with xr.open_dataset(file_name) as dataset:
        x = dataset.dims["X"]
        y = dataset.dims["Y"]
    return x, y


def get_batch_number(path: Union[Path, str]) -> int:
    """Returns the batch number from the given path.

    An example argument would be like this:

    /mnt/exacloud/dteber_woodwellclimate_org/output/batch_0/output/restart-eq.nc

    The return value for the above path is 0.
    """
    match_found = re.search(r"batch_(\d+)", str(path))
    return int(match_found.group(1)) if match_found else -1


def render_slurm_job_script(template_name: str, values: dict) -> str:
    """Reads the specified template file and populates it with the given values.

    Args:
        template_name (str): Name of the template file located in the templates folder
                             at the root of the project.
        values (dict): A dictionary of key-value pairs for substitution in the template.
                       Keys represent placeholders in the template, and values are the
                       corresponding substitution values.

    Returns:
        str: The populated job script ready to be submitted to Slurm.

    Raises:
        FileNotFoundError: If the specified template file does not exist.

    """
    template_path = get_project_root() / "templates" / template_name
    if not template_path.exists():
        raise FileNotFoundError(f"{template_path} doesn't exist.")

    with open(template_path) as file:
        template = Template(file.read())

    return template.substitute(values)


def read_text_file(path: str) -> str:
    """Reads and returns the content of a text file.

    Args:
        path (str): The file system path to the text file to be read.

    Returns:
        str: The content of the file as a string.

    Raises:
        FileNotFoundError: If the specified file does not exist.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"The given file is not found: {path}")

    with open(path) as file:
        content = file.read()

    return content


def read_json_file(path: str) -> dict:
    """Reads and returns the content of a JSON file.

    Args:
        path (str): The file system path to the JSON file to be read.

    Returns:
        dict: The content of the file as a dictionary.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        json.JSONDecodeError: If the file content is not valid JSON.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"The given file is not found: {path}")

    with open(path) as file:
        content = json.load(file)

    return content


def write_text_file(path: str, content: str) -> None:
    """A self-explanatory function

    Args:
        path (str): The file system path where the content should be written.
        content (str): The content to write to the file.

    Returns:
        None
    """
    with open(path, "w") as file:
        file.write(content)


def write_json_file(path: str, content: dict, indent: int = 4) -> None:
    """Writes a dictionary to a file in JSON format with specified indentation.

    Args:
        path (str): The file system path where the JSON content should be written.
        content (dict): A dictionary representing the JSON data to be written
            to the file.
        indent (int, optional): The number of spaces to use as indentation in the
            JSON file. Defaults to 4.

    Returns:
        None
    """
    with open(path, "w") as file:
        json.dump(content, file, indent=indent)


def submit_job(path: str) -> CompletedProcess:
    """Submits a job script to the Slurm workload manager using the `sbatch` command.

    Args:
        path (str): The file system path to the job script to be submitted.

    Returns:
        CompletedProcess: An object representing the completed process, containing
                          information about the execution of the `sbatch` command,
                          including stdout, stderr, and the return code.

    Raises:
        FileNotFoundError: If the specified job script file does not exist.
        subprocess.CalledProcessError: If the `sbatch` command fails.
    """
    command = ["sbatch", path]
    return subprocess.run(command, text=True, capture_output=True)


def update_config(path: str, prefix_value: str) -> None:
    """Updates the 'IO' section of config.js with new paths.

    This function reads the JSON configuration file, modifies the 'IO' section
    by updating the paths with a new prefix, and then writes the updated
    configuration back to the file.

    Args:
        path (str): The file system path to the JSON configuration file to be updated.
        prefix_value (str): The new prefix to be added to the paths in the 'IO' section.

    Returns:
        None
    """
    config_data = read_json_file(path)
    for key, val in IO_PATHS.items():
        config_data["IO"][key] = f"{prefix_value}/{val}"

    write_json_file(path, config_data)


def create_slurm_script(
    path: str, template_name: str, substitution_values: dict
) -> None:
    """Creates a Slurm job script by rendering a template and writing it to a file.

    This function uses a template and a set of substitution values to generate a
    Slurm job script, and then writes the resulting script to the specified path.

    Args:
        path (str): The file system path where the Slurm job script should be saved.
        template_name (str): The name of the template file located in the templates
            folder at the root of the project.
        substitution_values (dict): A dictionary of key-value pairs for substituting
            placeholders in the template.

    Returns:
        None
    """
    slurm_runner = render_slurm_job_script(template_name, substitution_values)
    write_text_file(path, slurm_runner)
