import argparse
import textwrap

import lazy_import

InitCommand = lazy_import.lazy_class("batch_processing.cmd.init.InitCommand")
InputCommand = lazy_import.lazy_class("batch_processing.cmd.input.InputCommand")
MonitorCommand = lazy_import.lazy_class("batch_processing.cmd.monitor.MonitorCommand")
RunCheckCommand = lazy_import.lazy_class(
    "batch_processing.cmd.run_check.RunCheckCommand"
)
ExtractCellCommand = lazy_import.lazy_class(
    "batch_processing.cmd.extract_cell.ExtractCellCommand"
)
DiffCommand = lazy_import.lazy_class("batch_processing.cmd.diff.DiffCommand")
SliceInputCommand = lazy_import.lazy_class(
    "batch_processing.cmd.slice_input.SliceInputCommand"
)
BatchSplitCommand = lazy_import.lazy_class(
    "batch_processing.cmd.batch.split.BatchSplitCommand"
)
BatchRunCommand = lazy_import.lazy_class(
    "batch_processing.cmd.batch.run.BatchRunCommand"
)
BatchMergeCommand = lazy_import.lazy_class(
    "batch_processing.cmd.batch.merge.BatchMergeCommand"
)
BatchPostprocessCommand = lazy_import.lazy_class(
    "batch_processing.cmd.batch.postprocess.BatchPostprocessCommand"
)
BatchLegacySplitCommand = lazy_import.lazy_class(
    "batch_processing.cmd.batch.legacy_split.BatchLegacySplitCommand"
)
BatchLegacyRunCommand = lazy_import.lazy_class(
    "batch_processing.cmd.batch.legacy_run.BatchLegacyRunCommand"
)
BatchLegacyMergeCommand = lazy_import.lazy_class(
    "batch_processing.cmd.batch.legacy_merge.BatchLegacyMergeCommand"
)


def add_common_dvmdostem_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "-p", type=int, default=0, help="Number of PRE RUN years to run. By default, 0"
    )
    parser.add_argument(
        "-e",
        type=int,
        default=0,
        help="Number of EQUILIBRIUM years to run. By default, 0",
    )
    parser.add_argument(
        "-s", type=int, default=0, help="Number of SPINUP years to run. By default, 0"
    )
    parser.add_argument(
        "-t",
        type=int,
        default=0,
        help="Number of TRANSIENT years to run. By default, 0",
    )
    parser.add_argument(
        "-n", type=int, default=0, help="Number of SCENARIO years to run. By default, 0"
    )
    parser.add_argument(
        "-l",
        "--log-level",
        choices=["debug", "info", "note", "warn", "err", "fatal", "disabled"],
        default="disabled",
        help="Set the log level",
    )


def main():
    parser = argparse.ArgumentParser(
        prog="bp",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(
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
        ),
        epilog="Use bp <command> --help for detailed help.",
        add_help=False,
    )
    parser.add_argument(
        "-h", "--help", action="store_true", help="Show this help message and exit"
    )

    subparsers = parser.add_subparsers(title="Available commands", metavar="")

    parser_batch = subparsers.add_parser(
        "batch",
        help="Batch related operations",
    )
    batch_subparsers = parser_batch.add_subparsers(
        title="Available subcommands", metavar=""
    )

    parser_batch_split = batch_subparsers.add_parser(
        "split", help="Split the given input data into smaller batches"
    )

    parser_batch_split.add_argument(
        "-sp",
        "--slurm-partition",
        choices=["spot", "compute"],
        default="spot",
        help="Specificy the Slurm partition. By default, spot",
    )
    parser_batch_split.add_argument(
        "-i",
        "--input-path",
        required=True,
        help=(
            "Path to the directory that contains the input files. " "Example: ",
            "/mnt/exacloud/dvmdostem-inputs/cru-ts40_ar5_rcp85_ncar-ccsm4_Toolik_50x50",
        ),
    )

    add_common_dvmdostem_arguments(parser_batch_split)

    parser_batch_split.set_defaults(func=lambda args: BatchSplitCommand(args).execute())

    parser_batch_run = batch_subparsers.add_parser(
        "run", help="Submit the batches to the Slurm queue"
    )
    parser_batch_run.set_defaults(func=lambda args: BatchRunCommand(args).execute())

    parser_batch_merge = batch_subparsers.add_parser("merge", help="Merge the batches")

    parser_batch_merge.set_defaults(func=lambda args: BatchMergeCommand(args).execute())

    parser_batch_postprocess = batch_subparsers.add_parser(
        "postprocess",
        help="Post-process the merged files and creates pre-define graphs",
    )

    batch_postprocess_group = parser_batch_postprocess.add_mutually_exclusive_group(
        required=True
    )
    batch_postprocess_group.add_argument(
        "--light", action="store_true", help="Perform light post-processing"
    )
    batch_postprocess_group.add_argument(
        "--heavy", action="store_true", help="Perform heavy post-processing"
    )

    parser_batch_postprocess.set_defaults(
        func=lambda args: BatchPostprocessCommand(args).execute()
    )

    parser_batch_legacy_split = batch_subparsers.add_parser(
        "legacy_split",
        help=(
            "Split the input data into different batches. "
            "Note that this command removes the existing batches."
        ),
    )
    parser_batch_legacy_split.add_argument(
        "-c", "--cells-per-batch", type=int, help="The number of cells per batch"
    )
    parser_batch_legacy_split.add_argument(
        "-sp",
        "--slurm-partition",
        choices=["spot", "compute"],
        default="spot",
        help="Specificy the Slurm partition. By default, spot",
    )

    add_common_dvmdostem_arguments(parser_batch_legacy_split)

    parser_batch_legacy_split.set_defaults(
        func=lambda args: BatchLegacySplitCommand(args).execute()
    )

    parser_batch_legacy_run = batch_subparsers.add_parser(
        "legacy_run", help="Submit the batches to the Slurm queue"
    )
    parser_batch_legacy_run.set_defaults(
        func=lambda args: BatchLegacyRunCommand(args).execute()
    )

    parser_batch_legacy_merge = batch_subparsers.add_parser(
        "legacy_merge", help="Merge the completed batches"
    )
    parser_batch_legacy_merge.add_argument(
        "-v", "--vars", nargs="+", default=[], help="Merge only the given variables"
    )
    parser_batch_legacy_merge.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Continue merging even if not all cells ran successfully.",
    )
    parser_batch_legacy_merge.set_defaults(
        func=lambda args: BatchLegacyMergeCommand(args).execute()
    )

    parser_monitoring = subparsers.add_parser(
        "monitor",
        help=(
            "Monitor the batches and if there is an unfinished job,"
            "it resubmits that."
        ),
    )
    parser_monitoring.add_argument(
        "-c",
        "--instance-count",
        type=int,
        required=True,
        help=(
            "The maximum amount of available machines in the cluster."
            "It can be found by 'sinfo' command"
        ),
    )
    parser_monitoring.set_defaults(func=lambda args: MonitorCommand(args).execute())

    parser_init = subparsers.add_parser(
        "init", help="Initialize the environment for running the simulation"
    )
    parser_init.set_defaults(func=lambda args: InitCommand(args).execute())

    parser_input = subparsers.add_parser(
        "input", help="Modify config.js file according to the provided input path"
    )

    parser_input.add_argument(
        "-i",
        "--input-path",
        required=True,
        help=(
            "Path to the directory that contains the input files."
            "Example: $HOME/input/four-basins"
        ),
    )
    parser_input.set_defaults(func=lambda args: InputCommand(args).execute())

    parser_run_check = subparsers.add_parser("run_check", help="todo")
    parser_run_check.set_defaults(func=lambda args: RunCheckCommand(args).execute())

    parser_extract = subparsers.add_parser(
        "extract_cell", help="Extracts a single cell and creates a batch"
    )
    parser_extract.add_argument(
        "-i", "--input-path", required=True, help="Path to the input folder"
    )
    parser_extract.add_argument(
        "-o", "--output-path", required=True, help="Path to the output folder"
    )
    parser_extract.add_argument(
        "-X", type=int, required=True, help="The row to extract"
    )
    parser_extract.add_argument(
        "-Y", type=int, required=True, help="The column to extract"
    )
    parser_extract.add_argument(
        "-sp",
        "--slurm-partition",
        choices=["spot", "compute"],
        default="spot",
        help="Specificy the Slurm partition. By default, spot",
    )

    add_common_dvmdostem_arguments(parser_extract)

    parser_extract.set_defaults(func=lambda args: ExtractCellCommand(args).execute())

    parser_diff = subparsers.add_parser(
        "diff",
        help="Compare the NetCDF files in the given directories. "
        "The given two directories must contain the same files.",
    )

    parser_diff.add_argument("path_one")
    parser_diff.add_argument("path_two")
    parser_diff.set_defaults(func=lambda args: DiffCommand(args).execute())

    parser_slice_input = subparsers.add_parser(
        "slice_input",
        help="Slices the given input data into smaller folders. "
        "To use this command, the given input has to have at least 500,000 cells.",
    )

    parser_slice_input.add_argument(
        "-i", "--input-path", required=True, help="Path to the input folder to slice"
    )
    parser_slice_input.add_argument(
        "-o",
        "--output-path",
        required=True,
        help="Path for writing the sliced input dataset",
    )
    parser_slice_input.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Override if the given output path exists",
    )

    parser_slice_input.set_defaults(func=lambda args: SliceInputCommand(args).execute())

    args = parser.parse_args()

    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()
