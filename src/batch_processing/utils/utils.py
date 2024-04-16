import errno
import json
import os
import re
import subprocess
from pathlib import Path

import cftime
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
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


def remove_file(file: str | list):
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
