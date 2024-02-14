# batch-processing

This is an internal helper utility program to automate the workflow on the HPC cluster.
The cluster can be found on [this repository](https://github.com/whrc/GCP-Slurm-Arctic/)

In this document, it is assumed that the HPC cluster is already up and running and you have logged in to the Slurm's login node.

## How to Install

It already comes pre-installed to the Slurm login node.
However, if you want to customize it, first delete the existing program and follow the below steps:

```bash
git clone https://github.com/whrc/batch-processing.git
cd batch-processing/
pip install .
```

## How to Use

```
usage: bp [-h]  ...

bp (or batch-processing) is a specialized internal tool designed for
scientists at the Woodwell Climate Research Center.

Optimized for execution in the GCP (Google Cloud Platform) cluster,
this tool streamlines the process of setting up and managing Slurm-based
computational environments. It simplifies tasks such as configuring run
parameters, partitioning input data into manageable batches, and executing
these batches efficiently.

Its primary aim is to enhance productivity and reduce manual setup
overhead in complex data processing workflows, specifically tailored
to the needs of climate research and analysis.

optional arguments:
  -h, --help  Show this help message and exit

Available commands:

    batch     Slice, run and merge batches
    monitor   Monitors the batches and if there is an unfinished job,it resubmits that.
    init      Initialize the environment for running the simulation
    input     Modify config.js file according to the provided input path
    elapsed   Measures the total elapsed time for running a dataset

Use bp <command> --help for detailed help.
```

A typical workflow would look like this:

1) Initialize the environment: `bp init`.

Optionally, you can provide an additional argument `-d` which is the location of the input data you want to copy into the cluster.

2) Configure the dvmdostem: `bp input -i <path-to-input-data>`

3) Split the input data into separate batches: `bp batch split -c <cells-per-batch> -p <pre-run-years> -e <equilibrium-years> -s <spin-up-years> -t <transient-years> -n <scenario-years>`
You can learn the details via `bp batch split --help`

4) Submit the batches to Slurm: `bp batch run`

5) Start monitoring to recover the preempted machines: `bp monitor`
[Read more about why this is necessary](todo)

6) (**Optional**) Measure the elapsed time to run the simulation: `bp elapsed`


## Contributing

It is pretty easy to start working on the project:

```bash
git clone https://github.com/whrc/batch-processing.git
cd batch-processing/
pip install -r requirements.txt
pre-commit install
```

You are good to go!
