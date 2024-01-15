import argparse

from cmd.configure.input import handle_configure
from cmd.configure.init import handle_init
from cmd.batch.split import handle_batch_split


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="batch-processing", epilog="todo", description="This is an internal tool developed for the scientists in the Woodwell Climate Research Center. This tool is meant to run in the GCP cluster. It helps setting up the Slurm environment, configuring the run, and splitting and running the input data as batches.")
    subparsers = parser.add_subparsers(help="The operation that needs to be performed")

    parser_batch = subparsers.add_parser("batch", help="Splits, runs and merges the batches")
    batch_subparsers = parser_batch.add_subparsers(help="Batch related operations")

    parser_batch_split = batch_subparsers.add_parser("split", help="Split the input data into different batches")
    parser_batch_split.add_argument("-c", "--cells-per-batch", type=int, help="The number of cells per batch")
    parser_batch_split.add_argument("-sp", "--slurm-partition", choices=["spot", "compute"], default="spot", help="Specificy the Slurm partition. By default, spot")
    parser_batch_split.add_argument("-p", type=int, required=True, help="todo")
    parser_batch_split.add_argument("-e", type=int, required=True, help="todo")
    parser_batch_split.add_argument("-s", type=int, required=True, help="todo")
    parser_batch_split.add_argument("-t", type=int, required=True, help="todo")
    parser_batch_split.add_argument("-n", type=int, required=True, help="todo")
    parser_batch_split.set_defaults(func=handle_batch_split)

    parser_batch_run =  batch_subparsers.add_parser("run", help="Run the batches")
    parser_batch_run.add_argument("-b", "--batch-dir", default="/mnt/exacloud/$USER/output", help="A directory path that contains the batches. By default, /mnt/exacloud/$USER/output. Probably don't need to set this argument")
    # idea: might add firstIndex lastIndex as well, but not necessary right now

    parser_batch.add_argument("-r", "--run", type=bool, help="Run the batches")
    parser_batch.add_argument("-m", "--merge", type=bool, help="Merge the batches")

    parser_config = subparsers.add_parser("configure", help="Configuration related operations")
    config_subparsers = parser_config.add_subparsers(help="todo")

    parser_config_init = config_subparsers.add_parser("init", help="todo")
    parser_config_init.add_argument("-d", "--input-data", help="An absolute path of the data folder in the Google Bucket. Example: gs://iem-dataset/uaem-quick-datashare")
    parser_config_init.set_defaults(func=handle_init)

    parser_config_input = config_subparsers.add_parser("input", help="todo")
    parser_config_input.add_argument("-i", "--input-path", required=True, help="Path to the directory that contains the input files. Example: $HOME/input/four-basins")
    parser_config_input.set_defaults(func=handle_configure)

    args = parser.parse_args()

    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()
