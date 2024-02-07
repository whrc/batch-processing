import json
import os
import re
import shutil
import textwrap
from cmd.base import BaseCommand

import netCDF4 as nc
import numpy as np

from utils.utils import mkdir_p

# This script is used to split a dvmdostem run into "sub domains" that can be
# run individually (submitted to the queue manager) and then merged together
# at the end. In this case, the "full domain" is NOT the entire IEM domain, but
# is simply the full area that you are trying to run, i.e. a 10x10 region, or
# a 50x50 region.

# 1) Log on to atlas, cd into your dvmdostem directory.
#
# 2) Checkout desired git version, setup environment.
#
# 3) Compile (make).
#
# 4) Setup your run as though you were going to run serially (single
#    processor). Configure as necessary the following things:
#      - paths in the config file to the input data and full-domain run mask
#      - adjust the output_spec.csv to your needs
#      - turn outputs on/off for various run-stages (eq, sp, etc)
#      - path to output data is not important - it will be overwritten
#
# 5) Figure out how many cells you want per batch, and set the constant below.
#
# 6) Run this script.
#
# This script will split your run into however many batches are necessary to
# run all the cells and keep the max cells per batch in line with the constant
# you set below. The script will setup two directory hierarchies: one for the
# outputs of the individual batch runs and one for the "staging" area for each
# batch run. The staging area allows each run to have a different config file
# and different run mask (which is what actually controls which cells are
# in which batch. Then the script will submit a job to slurm for each batch.
#
# To process the outputs, use the "batch_merge.sh" script.


class BatchSplitCommand(BaseCommand):
    def __init__(self, args):
        self._args = args
        self._cells_per_batch = self._args.cells_per_batch
        self._config_file_path = f"{os.getenv('HOME')}/dvm-dos-tem/config/config.js"

    # todo: we might create a progress bar for this since it takes
    # quite some time for a bigger input datasets and we don't want
    # to drown the terminal with the same output
    def execute(self):
        # Look in the config file to figure out where the full-domain runmask is.
        with open(self._config_file_path) as f:
            input_str = f.read()
        j = json.loads(re.sub("//.*\n", "\n", input_str))
        BASE_RUNMASK = j["IO"]["runmask_file"]
        BASE_OUTDIR = j["IO"]["output_dir"]

        # Figure out how many batches are necessary to complete the full run.
        # This is somewhat restricted by how cells are assigned to processes.
        with nc.Dataset(BASE_RUNMASK, "r") as runmask:
            TOTAL_CELLS_TO_RUN = np.count_nonzero(runmask.variables["run"])
            print(f"Total cells to run: {TOTAL_CELLS_TO_RUN}")
            runmasklist = runmask.variables["run"][:, :].flatten()
            runmaskreversed = runmasklist[::-1]
            last_cell_index = len(runmaskreversed) - np.argmax(runmaskreversed) - 1
            # Padded due to the fact that this allows for discontiguous runs
            #  while accounting for the fact that cell assignment is very
            #  rigid in the model
            padded_cell_count = last_cell_index + 1

        # nbatches = padded_cell_count / self._cells_per_batch
        nbatches = TOTAL_CELLS_TO_RUN / self._cells_per_batch
        # If there are extra cells, or fewer cells than self._cells_per_batch
        # if (padded_cell_count % self._cells_per_batch != 0):
        if TOTAL_CELLS_TO_RUN % self._cells_per_batch != 0:
            print("Adding another batch to pick up stragglers!")
            nbatches += 1

        nbatches = int(nbatches)
        print("NUMBER OF BATCHES: ", nbatches)

        #
        # SETUP DIRECTORIES
        #
        print("Removing any existing staging or batch run directories")
        if os.path.isdir(BASE_OUTDIR + "/batch-run"):
            shutil.rmtree(BASE_OUTDIR + "/batch-run")

        for batch_id in range(0, nbatches):
            print(f"Making directories for batch {batch_id}")
            mkdir_p(BASE_OUTDIR + f"/batch-run/batch-{batch_id}")

            work_dir = BASE_OUTDIR + "/batch-run"

            print(f"Copy run mask, config file, etc for batch {batch_id}")
            shutil.copy(BASE_RUNMASK, work_dir + f"/batch-{batch_id}/")
            shutil.copy(self._config_file_path, work_dir + f"/batch-{batch_id}/")

            print(f"Reset the run mask for batch {batch_id}")
            with nc.Dataset(
                work_dir + f"/batch-{batch_id}/run-mask.nc", "a"
            ) as runmask:
                runmask.variables["run"][:] = np.zeros(runmask.variables["run"].shape)

        #
        # BUILD BATCH SPECIFIC RUN-MASKS
        #
        with nc.Dataset(BASE_RUNMASK, "r") as runmask:
            nz_ycoords = runmask.variables["run"][:].nonzero()[0]
            nz_xcoords = runmask.variables["run"][:].nonzero()[1]

        # For every cell that is turned on in the main run-mask, we assign this cell
        # to a batch to be run, and turn on the corresponding cell in the batch's
        # run mask.
        print("Turning on pixels in each batch's run mask...")
        batch = 0
        cells_in_sublist = 0
        coord_list = list(zip(nz_ycoords, nz_xcoords))
        for i, cell in enumerate(coord_list):
            with nc.Dataset(
                work_dir + f"/batch-{batch}/run-mask.nc", "a"
            ) as grp_runmask:
                grp_runmask.variables["run"][cell] = True
                cells_in_sublist += 1

            if (cells_in_sublist == self._cells_per_batch) or (
                i == len(coord_list) - 1
            ):
                print(f"Group {batch} will run {cells_in_sublist} cells...")
                batch += 1
                cells_in_sublist = 0

        #
        # SUMMARIZE
        #
        number_batches = batch
        # assert (nbatches == number_batches), "PROBLEM: Something is wrong with the batch numbers: {} vs {}".format(nbatches, number_batches)
        print(f"Split cells into {number_batches} batches...")

        #
        # MODIFY THE CONFIG FILE FOR EACH BATCH
        #
        print(
            "Modifying each batch's config file; changing path to run mask and to output directory..."
        )
        for batch_num in range(0, number_batches):
            with open(work_dir + f"/batch-{batch_num}/config.js") as f:
                input_string = f.read()

            j = json.loads(
                re.sub("//.*\n", "\n", input_string)
            )  # Strip comments from json file
            j["IO"]["runmask_file"] = work_dir + f"/batch-{batch_num}/run-mask.nc"
            j["IO"]["output_dir"] = work_dir + f"/batch-{batch_num}/output/"

            output_str = json.dumps(j, indent=2, sort_keys=True)

            with open(work_dir + f"/batch-{batch_num}/config.js", "w") as f:
                f.write(output_str)

        #
        # SUBMIT SBATCH SCRIPT FOR EACH BATCH
        #
        for batch in range(0, number_batches):
            with nc.Dataset(work_dir + f"/batch-{batch}/run-mask.nc", "r") as runmask:
                cells_in_batch = np.count_nonzero(runmask.variables["run"])

            assert cells_in_batch > 0, "PROBLEM! Groups with no cells activated to run!"

            slurm_runner_scriptlet = textwrap.dedent(
                f"""\
        #!/bin/bash -l

        # Job name, for clarity
        #SBATCH --job-name="ddt-batch-{batch}"

        # Partition specification
        #SBATCH -p {self._args.slurm_partition}

        # Log the output
        #SBATCH -o /mnt/exacloud/{os.getenv('USER')}/slurm-logs/batch-{batch}.out

        # Number of MPI tasks
        #SBATCH -N 1

        echo $SLURM_JOB_NODELIST

        ulimit -s unlimited
        ulimit -l unlimited

        # Load up my custom paths stuff
        . /dependencies/setup-env.sh
        . /etc/profile.d/z00_lmod.sh
        module load openmpi
        
        cd /home/$USER/dvm-dos-tem

        mpirun ./dvmdostem -f {work_dir}/batch-{batch}/config.js -l disabled --max-output-volume=-1 -p {self._args.p} -e {self._args.e} -s {self._args.s} -t {self._args.t} -n {self._args.n}
        """.format(batch, cells_in_batch, work_dir)
            )
            print(f"Writing sbatch script for batch {batch}")
            with open(work_dir + f"/batch-{batch}/slurm_runner.sh", "w") as f:
                f.write(slurm_runner_scriptlet)

        print("Split operation is completed.")
        print(
            f"Please check /mnt/exacloud/{os.getenv('USER')}/output/batch-run for the results."
        )
