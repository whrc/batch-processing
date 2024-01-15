# T. Carman, H. Genet
# Feb, March 2018

# D. Teber
# Update on January 9, 2024

import errno
import os
import shutil
import json
import re
import textwrap
import numpy as np
import netCDF4 as nc


def mkdir_p(path):
    """Provides similar functionality to bash mkdir -p"""
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def setup_directories(output_dir, runmask, nbatches):
    print("Removing any existing staging or batch run directories")
    if os.path.isdir(output_dir + "/batch-run"):
        shutil.rmtree(output_dir + "/batch-run")

    for batch_id in range(0, nbatches):
        print("Making directories for batch {}".format(batch_id))
        mkdir_p(output_dir + "/batch-run/batch-{}".format(batch_id))

        work_dir = output_dir + "/batch-run"

        print("Copy run mask, config file, etc for batch {}".format(batch_id))
        shutil.copy(runmask, work_dir + "/batch-{}/".format(batch_id))
        shutil.copy("config/config.js", work_dir + "/batch-{}/".format(batch_id))

        print("Reset the run mask for batch {}".format(batch_id))
        with nc.Dataset(
            work_dir + "/batch-{}/run-mask.nc".format(batch_id), "a"
        ) as runmask:
            runmask.variables["run"][:] = np.zeros(runmask.variables["run"].shape)



def get_total_batch_number(runmask, cells_per_batch):
    # Figure out how many batches are necessary to complete the full run.
    # This is somewhat restricted by how cells are assigned to processes.
    with nc.Dataset(runmask, "r") as runmask:
        TOTAL_CELLS_TO_RUN = np.count_nonzero(runmask.variables["run"])
        print("Total cells to run: {}".format(TOTAL_CELLS_TO_RUN))
        runmasklist = runmask.variables["run"][:, :].flatten()
        runmaskreversed = runmasklist[::-1]
        last_cell_index = len(runmaskreversed) - np.argmax(runmaskreversed) - 1
        # Padded due to the fact that this allows for discontiguous runs
        #  while accounting for the fact that cell assignment is very
        #  rigid in the model
        padded_cell_count = last_cell_index + 1

    # nbatches = padded_cell_count / cells_per_batch
    nbatches = TOTAL_CELLS_TO_RUN / cells_per_batch
    # If there are extra cells, or fewer cells than cells_per_batch
    # if (padded_cell_count % cells_per_batch != 0):
    if TOTAL_CELLS_TO_RUN % cells_per_batch != 0:
        print("Adding another batch to pick up stragglers!")
        nbatches += 1

    nbatches = int(nbatches)
    print("NUMBER OF BATCHES: ", nbatches)

    return nbatches


def handle_batch_split(args):
    IDEAL_CELLS_PER_BATCH = args.cells_per_batch

    # Look in the config file to figure out where the full-domain runmask is.
    with open("config/config.js", "r") as f:
        input_str = f.read()
    j = json.loads(re.sub("//.*\n", "\n", input_str))
    BASE_RUNMASK = j["IO"]["runmask_file"]
    BASE_OUTDIR = j["IO"]["output_dir"]

    nbatches = get_total_batch_number(BASE_RUNMASK, IDEAL_CELLS_PER_BATCH)
    setup_directories(BASE_OUTDIR, BASE_RUNMASK, nbatches)

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
    work_dir = BASE_OUTDIR + "/batch-run"
    for i, cell in enumerate(coord_list):
        with nc.Dataset(
            work_dir + "/batch-{}/run-mask.nc".format(batch), "a"
        ) as grp_runmask:
            grp_runmask.variables["run"][cell] = True
            cells_in_sublist += 1

        if (cells_in_sublist == IDEAL_CELLS_PER_BATCH) or (i == len(coord_list) - 1):
            print("Group {} will run {} cells...".format(batch, cells_in_sublist))
            batch += 1
            cells_in_sublist = 0

    #
    # SUMMARIZE
    #
    number_batches = batch
    # assert (nbatches == number_batches), "PROBLEM: Something is wrong with the batch numbers: {} vs {}".format(nbatches, number_batches)
    print("Split cells into {} batches...".format(number_batches))

    #
    # MODIFY THE CONFIG FILE FOR EACH BATCH
    #
    print(
        "Modifying each batch's config file; changing path to run mask and to output directory..."
    )
    for batch_num in range(0, number_batches):
        with open(work_dir + "/batch-{}/config.js".format(batch_num), "r") as f:
            input_string = f.read()

        j = json.loads(
            re.sub("//.*\n", "\n", input_string)
        )  # Strip comments from json file
        j["IO"]["runmask_file"] = work_dir + "/batch-{}/run-mask.nc".format(batch_num)
        j["IO"]["output_dir"] = work_dir + "/batch-{}/output/".format(batch_num)

        output_str = json.dumps(j, indent=2, sort_keys=True)

        with open(work_dir + "/batch-{}/config.js".format(batch_num), "w") as f:
            f.write(output_str)

    #
    # SUBMIT SBATCH SCRIPT FOR EACH BATCH
    #
    for batch in range(0, number_batches):
        with nc.Dataset(work_dir + "/batch-{}/run-mask.nc".format(batch), "r") as runmask:
            cells_in_batch = np.count_nonzero(runmask.variables["run"])

        assert cells_in_batch > 0, "PROBLEM! Groups with no cells activated to run!"

        slurm_runner_scriptlet = textwrap.dedent(
            f"""\
        #!/bin/bash -l

        # Job name, for clarity
        #SBATCH --job-name="ddt-batch-{batch}"

        # Partition specification
        #SBATCH -p {args.slurm_partition}

        # Number of MPI tasks
        #SBATCH -N 1

        # Log file
        #SBATCH -o slurm-{batch}.out

        echo $SLURM_JOB_NODELIST

        ulimit -s unlimited
        ulimit -l unlimited

        # Load up my custom paths stuff
        . /dependencies/setup-env.sh
        . /etc/profile.d/z00_lmod.sh
        module load openmpi
        
        cd /home/$USER/dvm-dos-tem

        mpirun ./dvmdostem -f {work_dir}/batch-{batch}/config.js -l disabled --max-output-volume=-1 -p {args.p} -e {args.e} -s {args.s} -t {args.t} -n {args.n}
        # mpirun ./dvmdostem -f {work_dir}/batch-{batch}/config.js -l disabled --max-output-volume=-1 -p 5 -e 5 -s 5 -t 5 -n 5 2>&1 | tee {work_dir}/batch-{batch}/runlog.out
        """
        )

        print("Writing sbatch script for batch {}".format(batch))
        with open(work_dir + "/batch-{}/slurm_runner.sh".format(batch), "w") as f:
            f.write(slurm_runner_scriptlet)
