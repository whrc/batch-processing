# batch-processing

This is an internal helper utility program to automate the workflow on the HPC cluster.
The cluster can be found on [this repository](https://github.com/whrc/GCP-Slurm-Arctic/)

In this document, it is assumed that the HPC cluster is already up and running and you have logged in to the Slurm's login node.

## How to Install

It already comes pre-installed to the Slurm login node.
You can verify if it's installed by:

```bash
bp --version
```

If you want to customize the program, make changes to it and install it like any other pip package:

```bash
pip install <path-to-custom-bp>
```

## How to Use

All of the available commands are:

* [init](#init)
* [batch split](#batch-split)
* [batch run](#batch-run)
* [batch merge](#batch-merge)
* [map](#map)
* [diff](#diff)
* [extract_cell](#extract_cell)
* [slice_input](#slice_input)

### init

The first command should be run before running any other commands.
It copies [dvm-dos-tem model](https://github.com/uaf-arctic-eco-modeling/dvm-dos-tem) from a GCP bucket and sets up the file system.
It doesn't take any argument.

```bash
bp init
```

### batch split

Splits the given input set into columns for faster processing.
It takes the following arguments:

* `-i/--input-path`: Relative or absolute path to the input files. Required.
* `-b/--batches`: Path to store the split batches. Note that the given value will be concatenated with `/mnt/exacloud/$USER`. Required.
* `-sp/--slurm-partition`: Name of the slurm partition. Optional, by default `spot`.
* `-p`: Number of pre-run years to run. Optional, by default `0`.
* `-e`: Number of equilibrium years to run. Optional, by default `0`.
* `-s`: Number of spin-up years to run. Optional, by default `0`.
* `-t`: Number of transient years to run. Optional, by default `0`.
* `-n`: Number of scenario years to run. Optional, by default `0`.
* `-l/--log-level`: Level of logging. Optional, by default `disabled`.

If `bp batch split -i /mnt/exacloud/dvmdostem-input/my-big-input-dataset -b first-run -p 100 -e 1000 -s 85 -t 115 -n 85 --log-level warn` command is run, you should be able to see your batch folders in `/mnt/exacloud/$USER/first-run` where `$USER` is the username of the current logged in user.
You can check `slurm_runner.sh` to see the details of the job.

### batch run

Submits all of the jobs to Slurm in the given batch folder.
It takes one argument:

* `-b/--batches`: Path that stores job folders.

Assuming `bp batch split` is run with `-b first-run`, running `bp batch run -b first-run` submits all the jobs in that folder to the Slurm controller.

### batch merge

Combines the results of all batches.
It should be run after all jobs are finished.
It takes one argument:

* `-b/--batches`: Path that stores job folders.

Assuming `bp batch merge -b first-run` is run, it looks for the `/mnt/exacloud/$USER/first-run` folder, gathers the results, and puts them into `all-merged` folder in the batch folder, ie. `/mnt/exacloud/$USER/first-run`.

### map

Plots the status of a run by checking individual cell statuses and puts cells that have not succeeded in a text file for further reference.
It takes one argument:

* `-b/--batches`: Path that stores job folders.

When `bp map -b first-run` is run, it creates `run_status_visualization.png` and `failed_cell_coords.txt` in `/mnt/exacloud/$USER/first-run`.
These files can be copied to a local environment or a bucket using [`gcloud`](https://cloud.google.com/sdk/gcloud) or [`gsutil`](https://cloud.google.com/storage/docs/gsutil) tools.

### diff

Compares the NetCDF files in the given two directories.
It takes two positional arguments:

* todo

### extract_cell

Extracts a single cell from the given input set.
It takes the following arguments:

* todo

### slice_input

Slices the given big input set into 10 smaller pieces by spawning a `process` node in the cluster.
It works with input sets that have more than 500,000 cells.
It takes the following arguments:

* todo


## Contributing

It is pretty easy to start working on the project:

```bash
git clone https://github.com/whrc/batch-processing.git
cd batch-processing/
pip install -r requirements.txt
pre-commit install
```

You are good to go!
