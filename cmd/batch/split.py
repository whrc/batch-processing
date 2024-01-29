# Authors: T. Carman, H. Genet, D. Teber

import errno
import json
import os
import shutil
import textwrap
import numpy as np
import netCDF4 as nc
import logging
import re

# Global Constants
BASE_OUTDIR = "batch-run"


def setup_logging():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )


def read_config(file_path):
    with open(file_path, "r") as f:
        input_str = f.read()
    return json.loads(re.sub("//.*\n", "\n", input_str))


def calculate_batches(total_cells, cells_per_batch):
    nbatches = total_cells / cells_per_batch
    if total_cells % cells_per_batch != 0:
        nbatches += 1
    return int(nbatches)


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno != errno.EEXIST or not os.path.isdir(path):
            raise


def setup_directories(base_outdir, nbatches):
    batch_dir = os.path.join(base_outdir, "batch-run")
    if os.path.isdir(batch_dir):
        shutil.rmtree(batch_dir)
    for batch_id in range(nbatches):
        batch_path = os.path.join(batch_dir, f"batch-{batch_id}")
        mkdir_p(batch_path)
    return batch_dir


def modify_config_for_batch(batch_num, work_dir, config):
    config["IO"]["runmask_file"] = f"{work_dir}/batch-{batch_num}/run-mask.nc"
    config["IO"]["output_dir"] = f"{work_dir}/batch-{batch_num}/output/"
    with open(f"{work_dir}/batch-{batch_num}/config.js", "w") as f:
        json.dump(config, f, indent=2, sort_keys=True)


def generate_slurm_script(batch, args, work_dir):
    script_content = textwrap.dedent(
        f"""\
        #!/bin/bash -l
        #SBATCH --job-name="ddt-batch-{batch}"
        #SBATCH -p {args.slurm_partition}
        #SBATCH -N 1
        #SBATCH -o slurm-{batch}.out
        ulimit -s unlimited
        ulimit -l unlimited
        . /dependencies/setup-env.sh
        . /etc/profile.d/z00_lmod.sh
        module load openmpi
        cd /home/$USER/dvm-dos-tem
        mpirun ./dvmdostem -f {work_dir}/batch-{batch}/config.js -l disabled --max-output-volume=-1 -p {args.p} -e {args.e} -s {args.s} -t {args.t} -n {args.n}
    """
    )
    with open(f"{work_dir}/batch-{batch}/slurm_runner.sh", "w") as f:
        f.write(script_content)


def handle_batch_split(args):
    setup_logging()
    home = os.getenv("HOME")
    config_file = f"{home}/dvm-dos-tem/config/config.js"
    config = read_config(config_file)
    BASE_RUNMASK = config["IO"]["runmask_file"]
    BASE_OUTDIR = config["IO"]["output_dir"]

    with nc.Dataset(BASE_RUNMASK, "r") as runmask:
        TOTAL_CELLS_TO_RUN = np.count_nonzero(runmask.variables["run"])
        logging.info(f"Total cells to run: {TOTAL_CELLS_TO_RUN}")

    nbatches = calculate_batches(TOTAL_CELLS_TO_RUN, args.cells_per_batch)
    logging.info(f"Number of batches: {nbatches}")

    batch_dir = setup_directories(BASE_OUTDIR, nbatches)

    for batch_num in range(nbatches):
        # todo: create a progress bar for splitting
        modify_config_for_batch(batch_num, batch_dir, config)
        generate_slurm_script(batch_num, args, batch_dir)
