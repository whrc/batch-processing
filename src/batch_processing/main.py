import logging
import os
import typer
from typing import Optional
from enum import Enum
import textwrap

# Force h5netcdf to use pyfive backend to avoid H5DSget_num_scales errors with
# NetCDF4 files that have complex dimension scale metadata (e.g. from DVMDOSTEM)
os.environ.setdefault("H5NETCDF_READ_BACKEND", "pyfive")

# Suppress pyfive's verbose INFO-level logging (file access messages)
logging.getLogger("pyfive").setLevel(logging.WARNING)

import lazy_import

from batch_processing.utils.utils import get_email_from_username
from batch_processing.cmd.base import get_basedir_from_config, DVMDOSTEM_FOLDER

InitCommand = lazy_import.lazy_class("batch_processing.cmd.init.InitCommand")
BatchSplitCommand = lazy_import.lazy_class(
    "batch_processing.cmd.batch.split.BatchSplitCommand"
)
BatchRunCommand = lazy_import.lazy_class(
    "batch_processing.cmd.batch.run.BatchRunCommand"
)
BatchMergeCommand = lazy_import.lazy_class(
    "batch_processing.cmd.batch.merge.BatchMergeCommand"
)
BatchPlotCommand = lazy_import.lazy_class(
    "batch_processing.cmd.batch.plot.BatchPlotCommand"
)
BatchPostprocessCommand = lazy_import.lazy_class(
    "batch_processing.cmd.batch.postprocess.BatchPostprocessCommand"
)
DiffCommand = lazy_import.lazy_class("batch_processing.cmd.diff.DiffCommand")
ExtractCellCommand = lazy_import.lazy_class(
    "batch_processing.cmd.extract_cell.ExtractCellCommand"
)
MapCommand = lazy_import.lazy_class("batch_processing.cmd.map.MapCommand")
SliceInputCommand = lazy_import.lazy_class(
    "batch_processing.cmd.slice_input.SliceInputCommand"
)
MonitorCommand = lazy_import.lazy_class("batch_processing.cmd.monitor.MonitorCommand")


class LogLevel(str, Enum):
    debug = "debug"
    info = "info"
    note = "note"
    warn = "warn"
    err = "err"
    fatal = "fatal"
    disabled = "disabled"


class SlurmPartition(str, Enum):
    spot = "spot"
    dask = "dask"
    compute = "compute"


app = typer.Typer(
    help=textwrap.dedent(
        """
        bp (or batch-processing) is a specialized internal tool designed for
        scientists at the Woodwell Climate Research Center.

        Optimized for execution in the GCP (Google Cloud Platform) cluster,
        this tool streamlines the process of setting up and managing Slurm-based
        computational environments. It simplifies tasks such as configuring run
        parameters, partitioning input data into manageable batches, and executing
        these batches efficiently.

        Its primary aim is to enhance productivity and reduce manual setup
        overhead in complex data processing workflows, specifically tailored
        to the needs of climate research and analysis."""
    )
)

batch_app = typer.Typer(help="Batch related operations")
app.add_typer(batch_app, name="batch")


@app.callback()
def callback(
    version: Optional[bool] = typer.Option(
        None, "--version", "-v", help="Show the version and exit.", is_flag=True
    )
):
    if version:
        typer.echo("bp 1.1.0")
        raise typer.Exit()


@app.command("init")
def init(
    basedir: str = typer.Option(
        "/opt/apps",
        "--basedir",
        help="Parent directory where dvm-dos-tem will be installed",
    ),
    compile: bool = typer.Option(
        False,
        "--compile",
        help="Clone dvm-dos-tem from GitHub and compile it instead of copying pre-built version from bucket",
    ),
        branch: Optional[str] = typer.Option(
        None,
        "--branch",
        help="Git branch of dvm-dos-tem to clone (used only with --compile)",
    ),
):
    """Initialize the environment for running the simulation."""
    args = type("Args", (), {"basedir": basedir, "compile": compile, "branch": branch})()
    InitCommand(args).execute()


@app.command("tem")
def tem():
    """Show the current dvm-dos-tem installation path."""
    from pathlib import Path
    basedir = get_basedir_from_config()
    dvmdostem_path = Path(basedir) / DVMDOSTEM_FOLDER
    typer.echo(dvmdostem_path)


@batch_app.command("postprocess")
def batch_postprocess(
    light: bool = typer.Option(
        False, "--light", help="Perform light post-processing"
    ),
    heavy: bool = typer.Option(
        False, "--heavy", help="Perform heavy post-processing"
    ),
):
    """Post-process the merged files and creates pre-define graphs."""
    if not light and not heavy:
        typer.echo("Error: Either --light or --heavy must be specified")
        raise typer.Exit(1)
    
    args = type("Args", (), {"light": light, "heavy": heavy})()
    BatchPostprocessCommand(args).execute()


@app.command("diff")
def diff(
    path_one: str = typer.Argument(
        ..., help="First path to compare"
    ),
    path_two: str = typer.Argument(
        ..., help="Second path to compare"
    ),
):
    """
    Compare the NetCDF files in the given directories.
    The given two directories must contain the same files.
    """
    args = type("Args", (), {"path_one": path_one, "path_two": path_two})()
    DiffCommand(args).execute()


# This duplicated definition is removed since we've replaced it with a more complete one below


# This duplicated definition is removed since we've replaced it with a more complete one below


@app.command("slice_input")
def slice_input(
    input_path: str = typer.Option(
        ..., "--input-path", "-i", help="Path to the input folder to slice"
    ),
    output_path: str = typer.Option(
        ..., "--output-path", "-o", help="Path for writing the sliced input dataset"
    ),
    force: bool = typer.Option(
        False, "--force", "-f", help="Override if the given output path exists"
    ),
    launch_as_job: bool = typer.Option(
        False,
        "--launch-as-job",
        "-l",
        help="Never pass this flag. It will be used internally to launch this command as a separate job.",
    ),
):
    """
    Slices the given input data into 10 smaller folders.
    To use this command, the given input has to have at least 500,000 cells.
    """
    args = type(
        "Args",
        (),
        {
            "input_path": input_path,
            "output_path": output_path,
            "force": force,
            "launch_as_job": launch_as_job,
        },
    )()
    SliceInputCommand(args).execute()


# Update the command definitions to include the common parameters directly
# This is a better approach than trying to add parameters after command definition

# For batch split command
@batch_app.command("split")
def batch_split(
    slurm_partition: SlurmPartition = typer.Option(
        SlurmPartition.spot,
        "--slurm-partition",
        "-sp",
        help="Specificy the Slurm partition.",
    ),
    input_path: str = typer.Option(
        ...,
        "--input-path",
        "-i",
        help=(
            "Remote or local path to the directory that contains the input files. "
            "If remote, prefix the path with 'gcs:// '. "
            "Remote path example: gcs://my-bucket/my-site. "
            "Local path example: /mnt/exacloud/dvmdostem-inputs/cru-ts40_ar5_rcp85_ncar-ccsm4_Toolik_50x50"
        ),
    ),
    launch_as_job: bool = typer.Option(
        False,
        "--launch-as-job",
        help="Never pass this flag. It will be used internally to launch this command as a separate job.",
    ),
    batches: str = typer.Option(
        ...,
        "--batches",
        "-b",
        help=(
            "Path to store the splitted batches. The given path will be concataned "
            "with /mnt/exacloud/$USER"
        ),
    ),
    p: int = typer.Option(0, help="Number of PRE RUN years to run. By default, 0"),
    e: int = typer.Option(0, help="Number of EQUILIBRIUM years to run. By default, 0"),
    s: int = typer.Option(0, help="Number of SPINUP years to run. By default, 0"),
    t: int = typer.Option(0, help="Number of TRANSIENT years to run. By default, 0"),
    n: int = typer.Option(0, help="Number of SCENARIO years to run. By default, 0"),
    log_level: LogLevel = typer.Option(
        LogLevel.disabled, "--log-level", "-l", help="Set the log level"
    ),
    job_name_prefix: Optional[str] = typer.Option(
        None, "--job-name-prefix", help="Optional prefix for job names to make them unique"
    ),
    restart_run: bool = typer.Option(
        False, "--restart-run", help="Add --no-output-cleanup flag to mpirun command"
    ),
):
    """Split the given input data into smaller batches."""
    # Create args object for compatibility with command class
    all_args = {
        "slurm_partition": slurm_partition.value,
        "input_path": input_path,
        "launch_as_job": launch_as_job,
        "batches": batches,
        "p": p,
        "e": e,
        "s": s,
        "t": t,
        "n": n,
        "log_level": log_level.value,
        "job_name_prefix": job_name_prefix,
        "restart_run": restart_run,
    }
    args = type("Args", (), all_args)()
    BatchSplitCommand(args).execute()


@batch_app.command("run")
def batch_run(
    batches: str = typer.Option(
        ...,
        "--batches",
        "-b",
        help=(
            "Path to store the splitted batches. The given path will be concataned "
            "with /mnt/exacloud/$USER"
        ),
    ),
):
    """Submit the batches to the Slurm queue."""
    args = type("Args", (), {"batches": batches})()
    BatchRunCommand(args).execute()


@batch_app.command("merge")
def batch_merge(
    batches: str = typer.Option(
        ...,
        "--batches",
        "-b",
        help=(
            "Path to store the splitted batches. The given path will be concataned "
            "with /mnt/exacloud/$USER"
        ),
    ),
    bucket_path: Optional[str] = typer.Option(
        "",
        "--bucket-path",
        help=(
            "Bucket path to write the results into. "
            "Required when the total cell size is greater than 40,000."
        ),
    ),
    auto_approve: bool = typer.Option(
        False,
        "--auto-approve",
        help="Skip user confirmation prompt and automatically proceed with merging.",
    ),
):
    """Merge the batches using hybrid approach that handles missing batches gracefully."""
    args = type("Args", (), {"batches": batches, "bucket_path": bucket_path, "auto_approve": auto_approve})()
    BatchMergeCommand(args).execute()


@batch_app.command("plot")
def batch_plot(
    batches: str = typer.Option(
        ...,
        "--batches",
        "-b",
        help=(
            "Path to store the splitted batches. The given path will be concataned "
            "with /mnt/exacloud/$USER"
        ),
    ),
    all_variables: bool = typer.Option(
        False, "--all", help="Plot all variables instead of the default set."
    ),
    email_me: bool = typer.Option(
        False, "--email-me", help="Send the summary plots via email to the default address."
    ),
    email_address: Optional[str] = typer.Option(
        get_email_from_username(), "--email-address", help="Specify a custom email address to send the plots to."
    )
):
    """Plots the results."""
    args = type("Args", (), {
        "batches": batches,
        "all_variables": all_variables,
        "email_me": email_me,
        "email_address": email_address
    })()
    BatchPlotCommand(args).execute()


@app.command("extract_cell")
def extract_cell(
    input_path: str = typer.Option(
        ..., "--input-path", "-i", help="Path to the input folder"
    ),
    output_path: str = typer.Option(
        ..., "--output-path", "-o", help="Path to the output folder"
    ),
    x: int = typer.Option(..., "-X", help="The row to extract"),
    y: int = typer.Option(..., "-Y", help="The column to extract"),
    slurm_partition: SlurmPartition = typer.Option(
        SlurmPartition.spot,
        "--slurm-partition",
        "-sp",
        help="Specificy the Slurm partition.",
    ),
    p: int = typer.Option(0, help="Number of PRE RUN years to run. By default, 0"),
    e: int = typer.Option(0, help="Number of EQUILIBRIUM years to run. By default, 0"),
    s: int = typer.Option(0, help="Number of SPINUP years to run. By default, 0"),
    t: int = typer.Option(0, help="Number of TRANSIENT years to run. By default, 0"),
    n: int = typer.Option(0, help="Number of SCENARIO years to run. By default, 0"),
    log_level: LogLevel = typer.Option(
        LogLevel.disabled, "--log-level", "-l", help="Set the log level"
    ),
):
    """Extracts a single cell and creates a batch."""
    all_args = {
        "input_path": input_path,
        "output_path": output_path,
        "X": x,
        "Y": y,
        "slurm_partition": slurm_partition.value,
        "p": p,
        "e": e,
        "s": s,
        "t": t,
        "n": n,
        "log_level": log_level.value,
    }
    args = type("Args", (), all_args)()
    ExtractCellCommand(args).execute()


@app.command("map")
def map_command(
    batches: str = typer.Option(
        ...,
        "--batches",
        "-b",
        help=(
            "Path to store the splitted batches. The given path will be concataned "
            "with /mnt/exacloud/$USER"
        ),
    ),
):
    """Maps the given path's status."""
    args = type("Args", (), {"batches": batches})()
    MapCommand(args).execute()


@app.command("monitor")
def monitor(
    action: str = typer.Argument(
        "start",
        help="Action to perform: start, stop, restart, or status"
    )
):
    """
    Monitor SLURM jobs and automatically rollback preempted jobs (runs as background daemon).
    
    This command manages a background daemon that continuously monitors the SLURM queue
    for job preemptions and automatically moves preempted jobs from spot/dask partitions
    to the compute partition to ensure job completion.
    
    Examples:
        bp monitor start    # Start the monitoring daemon
        bp monitor stop     # Stop the monitoring daemon  
        bp monitor restart  # Restart the monitoring daemon
        bp monitor status   # Check daemon status
        bp monitor          # Same as 'start'
    """
    if action not in ["start", "stop", "restart", "status"]:
        typer.echo(f"Error: Invalid action '{action}'. Use: start, stop, restart, or status")
        raise typer.Exit(1)
        
    args = type("Args", (), {"action": action})()
    MonitorCommand(args).execute()


def main():
    app()
