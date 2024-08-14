import argparse
import textwrap

import lazy_import

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


def add_batch_path_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "-b",
        "--batches",
        required=True,
        help=(
            "Path to store the splitted batches. The given path will be concataned "
            "with /mnt/exacloud/$USER",
        ),
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

    parser_init = subparsers.add_parser(
        "init", help="Initialize the environment for running the simulation"
    )
    parser_init.set_defaults(func=lambda args: InitCommand(args).execute())

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
    parser_batch_split.add_argument(
        "--launch-as-job",
        action="store_true",
        help="Never pass this flag. It will be used internally "
        "to lauch this command as a separate job.",
    )

    add_common_dvmdostem_arguments(parser_batch_split)
    add_batch_path_argument(parser_batch_split)

    parser_batch_split.set_defaults(func=lambda args: BatchSplitCommand(args).execute())

    parser_batch_run = batch_subparsers.add_parser(
        "run", help="Submit the batches to the Slurm queue"
    )

    add_batch_path_argument(parser_batch_run)

    parser_batch_run.set_defaults(func=lambda args: BatchRunCommand(args).execute())

    parser_batch_merge = batch_subparsers.add_parser("merge", help="Merge the batches")

    add_batch_path_argument(parser_batch_merge)

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

    parser_diff = subparsers.add_parser(
        "diff",
        help="Compare the NetCDF files in the given directories. "
        "The given two directories must contain the same files.",
    )

    parser_diff.add_argument("path_one")
    parser_diff.add_argument("path_two")
    parser_diff.set_defaults(func=lambda args: DiffCommand(args).execute())

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

    parser_map = subparsers.add_parser("map", help="Maps the given path's status.")

    add_batch_path_argument(parser_map)

    parser_map.set_defaults(func=lambda args: MapCommand(args).execute())

    parser_slice_input = subparsers.add_parser(
        "slice_input",
        help="Slices the given input data into 10 smaller folders. "
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
    parser_slice_input.add_argument(
        "-l",
        "--launch-as-job",
        action="store_true",
        help="Never pass this flag. It will be used internally "
        "to lauch this command as a separate job.",
    )

    parser_slice_input.set_defaults(func=lambda args: SliceInputCommand(args).execute())

    args = parser.parse_args()

    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()
