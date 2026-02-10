# batch-processing

This is an internal helper utility program to automate the workflow on the HPC cluster.
The cluster can be found on [this repository](https://github.com/whrc/GCP-Slurm-Arctic/)

In this document, it is assumed that the HPC cluster is already up and running and you have logged in to the Slurm's login node.

## How to Install

It already comes pre-installed to the Slurm login node.
You can verify if it's installed by:

```bash
bp --help
```

If it's not installed already, you can install it by:

```bash
git clone git@github.com:whrc/batch-processing.git
cd batch-processing
pip install .
```

## How to Use

All of the available commands are:

* [bp init](#bp-init)
* [bp tem](#bp-tem)
* [bp batch split](#bp-batch-split)
* [bp batch run](#bp-batch-run)
* [bp batch merge](#bp-batch-merge)
* [bp batch plot](#bp-batch-plot)
* [bp batch postprocess](#bp-batch-postprocess) *(deprecated)*
* [bp map](#bp-map) *(deprecated)*
* [bp diff](#bp-diff)
* [bp extract_cell](#bp-extract_cell)
* [bp slice_input](#bp-slice_input)
* [bp monitor](#bp-monitor) *(deprecated)*

### bp init

The first command should be run before running any other commands.
It configures the environment such as copying the [dvm-dos-tem model](https://github.com/uaf-arctic-eco-modeling/dvm-dos-tem), creating a folder for your username in the filesystem etc.
It takes the following optional arguments:

* `--basedir`: Parent directory where dvm-dos-tem will be installed. Optional, by default `/opt/apps`. The `dvm-dos-tem` folder will be created inside this directory. This argument is useful when working with different versions of dvm-dos-tem.
* `--compile`: Clone dvm-dos-tem from GitHub and compile it instead of copying a pre-built version from the bucket. Optional, by default copies from bucket to save time.

```bash
bp init                              # Installs to /opt/apps/dvm-dos-tem
bp init --basedir /mnt/exacloud      # Installs to /mnt/exacloud/dvm-dos-tem
bp init --compile                    # Clones and compiles to /opt/apps/dvm-dos-tem
bp init --basedir /mnt/exacloud --compile
```

### bp tem

Shows the current dvm-dos-tem installation path.
This reads from the `~/.bpconfig` file created by `bp init`, or returns the default path `/opt/apps/dvm-dos-tem` if no config exists.

```bash
bp tem
```

### bp batch split

Splits the given input set into columns for faster processing.
It takes the following arguments:

* `-i/--input-path`: Remote or local path to the directory that contains the input files. If remote, prefix the path with `gcs://`. Required.
* `-b/--batches`: Path to store the split batches. Note that the given value will be concatenated with `/mnt/exacloud/$USER`. Required.
* `-sp/--slurm-partition`: Name of the slurm partition. Optional, by default `spot`.
* `-p`: Number of pre-run years to run. Optional, by default `0`.
* `-e`: Number of equilibrium years to run. Optional, by default `0`.
* `-s`: Number of spin-up years to run. Optional, by default `0`.
* `-t`: Number of transient years to run. Optional, by default `0`.
* `-n`: Number of scenario years to run. Optional, by default `0`.
* `-l/--log-level`: Level of logging. Optional, by default `disabled`.
* `--job-name-prefix`: Optional prefix for job names to make them unique.
* `--restart-run`: Add `--no-output-cleanup` and `--restart-run` flags to mpirun command. Optional.

If `bp batch split -i /mnt/exacloud/dvmdostem-input/my-big-input-dataset -b first-run -p 100 -e 1000 -s 85 -t 115 -n 85 --log-level warn` command is run, you should be able to see your batch folders in `/mnt/exacloud/$USER/first-run` where `$USER` is the username of the current logged in user.
You can check `slurm_runner.sh` to see the details of the job.

### bp batch run

Submits all of the jobs to Slurm in the given batch folder.
It takes one argument:

* `-b/--batches`: Path that stores job folders.

Assuming `bp batch split` is run with `-b first-run`, running `bp batch run -b first-run` submits all the jobs in that folder to the Slurm controller.

### bp batch merge

Combines the results of all batches using a hybrid approach that handles missing batches gracefully.
It should be run after all jobs are finished.
It takes the following arguments:

* `-b/--batches`: Path that stores job folders. Required.
* `--bucket-path`: Bucket path to write the results into. Required when the total cell size is greater than 40,000.
* `--auto-approve`: Skip user confirmation prompt and automatically proceed with merging. Optional.

Assuming `bp batch merge -b first-run` is run, it looks for the `/mnt/exacloud/$USER/first-run` folder, gathers the results, and puts them into `all-merged` folder in the batch folder, ie. `/mnt/exacloud/$USER/first-run`.

### bp batch plot

Plots the results of a batch run.
It takes the following arguments:

* `-b/--batches`: Path that stores job folders. Required.
* `--all`: Plot all variables instead of the default set. Optional.
* `--email-me`: Send the summary plots via email to the default address. Optional.
* `--email-address`: Specify a custom email address to send the plots to. Optional.

```bash
bp batch plot -b first-run --email-me
```

### bp batch postprocess

> ⚠️ **Deprecated**: This command is deprecated and may be removed in a future release.

Post-processes the merged files and creates pre-defined graphs.
It requires one of the following flags:

* `--light`: Perform light post-processing.
* `--heavy`: Perform heavy post-processing.

```bash
bp batch postprocess --light
```

### bp map

> ⚠️ **Deprecated**: This command is deprecated and may be removed in a future release.

Plots the status of a run by checking individual cell statuses and puts cells that have not succeeded in a text file for further reference.
It takes one argument:

* `-b/--batches`: Path that stores job folders.

When `bp map -b first-run` is run, it creates `run_status_visualization.png` and `failed_cell_coords.txt` in `/mnt/exacloud/$USER/first-run`.
These files can be copied to a local environment or a bucket using [`gcloud`](https://cloud.google.com/sdk/gcloud) or [`gsutil`](https://cloud.google.com/storage/docs/gsutil) tools.

### bp diff

Compares the NetCDF files in the given two directories.
Both directories must contain the same number of `.nc` files, which will be compared using CDO's `diffv` command.
It takes two positional arguments:

* `path_one`: First directory path containing NetCDF files. Required.
* `path_two`: Second directory path containing NetCDF files. Required.

```bash
bp diff /path/to/first/output /path/to/second/output
```

### bp extract_cell

Extracts a single cell from the given input set and creates a batch ready to run.
It takes the following arguments:

* `-i/--input-path`: Path to the input folder. Required.
* `-o/--output-path`: Path to the output folder. Required.
* `-X`: The row (X coordinate) to extract. Required.
* `-Y`: The column (Y coordinate) to extract. Required.
* `-sp/--slurm-partition`: Name of the slurm partition. Optional, by default `spot`.
* `-p`: Number of pre-run years to run. Optional, by default `0`.
* `-e`: Number of equilibrium years to run. Optional, by default `0`.
* `-s`: Number of spin-up years to run. Optional, by default `0`.
* `-t`: Number of transient years to run. Optional, by default `0`.
* `-n`: Number of scenario years to run. Optional, by default `0`.
* `-l/--log-level`: Level of logging. Optional, by default `disabled`.

```bash
bp extract_cell -i /mnt/exacloud/dvmdostem-input/my-input -o /mnt/exacloud/$USER/single-cell -X 10 -Y 20 -p 100 -e 1000 -s 85
```

### bp slice_input

Slices the given big input set into 10 smaller pieces by spawning a `process` node in the cluster.
It works with input sets that have more than 500,000 cells.
It takes the following arguments:

* `-i/--input-path`: Path to the input folder to slice. Required.
* `-o/--output-path`: Path for writing the sliced input dataset. Required.
* `-f/--force`: Override if the given output path exists. Optional.

```bash
bp slice_input -i /mnt/exacloud/big-input-dataset -o /mnt/exacloud/$USER/sliced-input
```

### bp monitor

> ⚠️ **Deprecated**: This command is deprecated and may be removed in a future release.

Monitors SLURM jobs and automatically rolls back preempted jobs.
This command manages a background daemon that continuously monitors the SLURM queue for job preemptions and automatically moves preempted jobs from spot/dask partitions to the compute partition to ensure job completion.

It takes one positional argument:

* `action`: Action to perform. One of `start`, `stop`, `restart`, or `status`. By default `start`.

```bash
bp monitor start    # Start the monitoring daemon
bp monitor stop     # Stop the monitoring daemon
bp monitor restart  # Restart the monitoring daemon
bp monitor status   # Check daemon status
```


## Contributing

It is pretty easy to start working on the project:

```bash
git clone https://github.com/whrc/batch-processing.git
cd batch-processing/
pip install -r requirements.txt
pre-commit install
```

You are good to go!
