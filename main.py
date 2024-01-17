import argparse
import textwrap

from cmd.configure.input import handle_configure
from cmd.configure.init import handle_init
from cmd.batch.split import handle_batch_split
from cmd.batch.run import handle_batch_run
from cmd.monitor import handle_monitoring


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="batch-processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(
            """
            This is a specialized internal tool designed for scientists at the Woodwell Climate Research Center.

            Optimized for execution in the GCP (Google Cloud Platform) cluster, this tool streamlines the process of setting up 
            and managing Slurm-based computational environments. It simplifies tasks such as configuring run parameters, 
            partitioning input data into manageable batches, and executing these batches efficiently.

            Its primary aim is to enhance productivity and reduce manual setup overhead in complex data processing workflows, 
            specifically tailored to the needs of climate research and analysis."""
        ),
        epilog="Use batch-processing <command> --help for detailed help.",
        add_help=False,
    )
    parser.add_argument(
        "-h", "--help", action="store_true", help="Show this help message and exit"
    )

    subparsers = parser.add_subparsers(title="Available commands", metavar="")

    parser_batch = subparsers.add_parser(
        "batch",
        help="Slice, run and merge batches",
    )
    batch_subparsers = parser_batch.add_subparsers(
        title="Available subcommands", metavar=""
    )

    parser_batch_split = batch_subparsers.add_parser(
        "split", help="Split the input data into different batches"
    )
    parser_batch_split.add_argument(
        "-c", "--cells-per-batch", type=int, help="The number of cells per batch"
    )
    parser_batch_split.add_argument(
        "-sp",
        "--slurm-partition",
        choices=["spot", "compute"],
        default="spot",
        help="Specificy the Slurm partition. By default, spot",
    )
    parser_batch_split.add_argument("-p", type=int, required=True, help="Number of PRE RUN years to run")
    parser_batch_split.add_argument("-e", type=int, required=True, help="Number of EQUILIBRIUM years to run")
    parser_batch_split.add_argument("-s", type=int, required=True, help="Number of SPINUP years to run")
    parser_batch_split.add_argument("-t", type=int, required=True, help="Number of TRANSIENT years to run")
    parser_batch_split.add_argument("-n", type=int, required=True, help="Number of SCENARIO years to run")
    parser_batch_split.set_defaults(func=handle_batch_split)

    parser_batch_run = batch_subparsers.add_parser("run", help="Submit the batches to the Slurm queue")
    parser_batch_run.set_defaults(func=handle_batch_run)

    parser_monitoring = subparsers.add_parser("monitor", help="Monitors the batches and if there is an unfinished job, it resubmits that.")
    group = parser_monitoring.add_mutually_exclusive_group(required=True)
    group.add_argument("--start", action="store_true", help="Starts the monitoring process")
    group.add_argument("--stop", action="store_true", help="Stops the monitoring process")
    parser_monitoring.set_defaults(func=handle_monitoring)

    parser_config = subparsers.add_parser(
        "configure", help="Configuration related operations"
    )
    config_subparsers = parser_config.add_subparsers(title="Available commands", metavar="")

    parser_config_init = config_subparsers.add_parser("init", help="Initialize the Slurm working environment")
    parser_config_init.add_argument(
        "-d",
        "--input-data",
        help="An absolute path of the data folder in the Google Bucket. Example: gs://iem-dataset/uaem-quick-datashare",
    )
    parser_config_init.set_defaults(func=handle_init)

    parser_config_input = config_subparsers.add_parser("input", help="Configure the input data")
    parser_config_input.add_argument(
        "-i",
        "--input-path",
        required=True,
        help="Path to the directory that contains the input files. Example: $HOME/input/four-basins",
    )
    parser_config_input.set_defaults(func=handle_configure)

    args = parser.parse_args()

    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()
