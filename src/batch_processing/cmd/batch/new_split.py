import netCDF4
import os
import re
import shutil
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
import subprocess
import json

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import clean_and_load_json


# todo: later change this to batch-run dir
OUTPUT_DIR = "/mnt/exacloud/dteber_woodwellclimate_org/output/"
BATCH_DIRS = []
BATCH_INPUT_DIRS = [] 
INPUT_FILES = ["drainage.nc", "fri-fire.nc", "run-mask.nc", "soil-texture.nc", "topo.nc", "vegetation.nc", "historic-explicit-fire.nc", "projected-explicit-fire.nc", "projected-climate.nc", "historic-climate.nc"]
INPUT_DIR = '/mnt/exacloud/dteber_woodwellclimate_org/sliced-inputs-six'
SETUP_SCRIPTS_PATH = os.path.join(os.environ['HOME'], 'dvm-dos-tem/scripts/util')


class BatchNewSplitCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args
        # todo: fix this
        self.split_dimension = ""

    def setup_batch(self, batch_input_dir):
        os.makedirs(batch_input_dir, exist_ok=True)

        # todo: don't forget to copy co2 files from the original source
        # or, might copy them into each slice folders as well
        path = "/mnt/exacloud/dvmdostem-inputs/cru-ts40_ar5_rcp85_mri-cgcm3_all_four_basins_150x277"
        shutil.copy(os.path.join(path, 'co2.nc'), os.path.join(batch_input_dir, 'co2.nc'))
        shutil.copy(os.path.join(path, 'projected-co2.nc'), os.path.join(batch_input_dir, 'projected-co2.nc'))

    def create_chunks(self, dim_size, chunk_count):
        """Create chunk boundaries for slicing the dataset."""
        chunks = []
        chunk_size = dim_size // chunk_count
        remainder = dim_size % chunk_count

        for i in range(chunk_count):
            start = i * chunk_size + min(i, remainder)
            end = start + chunk_size + (1 if i < remainder else 0)
            chunks.append((start, end))

        return chunks

    def run_utils(self, batch_dir, batch_input_dir):
        subprocess.run([os.path.join(SETUP_SCRIPTS_PATH, "setup_working_directory.py"), batch_dir, "--input-data-path", batch_input_dir, "--copy-inputs"])
        subprocess.run([os.path.join(SETUP_SCRIPTS_PATH, "outspec.py"), os.path.join(batch_dir, "config/output_spec.csv"), "--empty"])
        subprocess.run([os.path.join(SETUP_SCRIPTS_PATH, "outspec.py"), os.path.join(batch_dir, "config/output_spec.csv"), "--on", "NPP", "m", "pft"])
        subprocess.run([os.path.join(SETUP_SCRIPTS_PATH, "outspec.py"), os.path.join(batch_dir, "config/output_spec.csv"), "--on", "TLAYER", "m", "layer"])
        subprocess.run([os.path.join(SETUP_SCRIPTS_PATH, "outspec.py"), os.path.join(batch_dir, "config/output_spec.csv"), "--on", "ALD", "y"])

    def execute(self):
        with open(self.config_path) as f:
            configuration = f.read()

        configuration = clean_and_load_json(configuration)
        runmask_file_path = configuration["IO"]["runmask_file"]
        with netCDF4.Dataset(runmask_file_path, "r") as dataset:
            X = dataset.dimensions["X"].size
            Y = dataset.dimensions["Y"].size

        print("Dimension size of X:", X)
        print("Dimension size of Y:", Y)

        # Choose the dimension to split along
        if Y > X:
            SPLIT_DIMENSION = 'X'
            DIMENSION_SIZE = X
        else:
            SPLIT_DIMENSION = 'Y'
            DIMENSION_SIZE = Y

        self.split_dimension = SPLIT_DIMENSION
        print(f"\nSplitting accros {SPLIT_DIMENSION} dimension")
        print("Dimension size:", DIMENSION_SIZE)

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        print("Cleaning up the existing directories")
        pattern = re.compile(r'^batch_\d+$')
        batch_directories = [os.path.join(OUTPUT_DIR, d) for d in os.listdir(OUTPUT_DIR) if os.path.isdir(os.path.join(OUTPUT_DIR, d)) and pattern.match(d)]

        for batch_dir in batch_directories:
            shutil.rmtree(batch_dir)

        print("Set up batch directories")
        for index in range(DIMENSION_SIZE):
            path = os.path.join(OUTPUT_DIR, f"batch_{index}")
            BATCH_DIRS.append(path)

            path = os.path.join(path, "input")
            BATCH_INPUT_DIRS.append(path)

        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
            _ = [executor.submit(self.setup_batch, batch_input_dir) for batch_input_dir in BATCH_INPUT_DIRS]

        print("Split input files")

        sliced_input_dirs = os.listdir(INPUT_DIR)
        for sliced_dir in sliced_input_dirs:

            for input_file in INPUT_FILES:
                input_file_path = os.path.join(INPUT_DIR, sliced_dir, input_file)
                tasks = []
                for start_index, end_index in self.create_chunks(15, os.cpu_count()):
                    tasks.append((start_index, end_index, input_file_path, input_file, SPLIT_DIMENSION))

                # print(tasks, end="\n\n")

                with Pool(processes=os.cpu_count()) as pool:
                    pool.starmap(split_file_chunk, tasks)

        print("Set up the batch simulation")
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
            _ = [executor.submit(self.run_utils, batch_dir, batch_input_dir) for batch_dir, batch_input_dir in zip(BATCH_DIRS, BATCH_INPUT_DIRS)]

        print("Configure each batch")
        with Pool(processes=os.cpu_count()) as pool:
            pool.map(configure, [(index, batch_dir) for index, batch_dir in enumerate(BATCH_DIRS)])


def split_file_chunk(start_index, end_index, input_path, input_file, split_dimension):
        print("splitting ", input_path)
        for index in range(start_index, end_index):
            path = os.path.join(BATCH_INPUT_DIRS[index], input_file)
            subprocess.run(["ncks", "-O", "-h", "-d", f"{split_dimension},{index}", 
                            input_path, path])

        print("done splitting ", input_file)


def configure(args):
    index, batch_dir = args
    config_file = os.path.join(batch_dir, "config/config.js")
    with open(config_file) as f:
        config_data = json.load(f)

    config_data['IO']['parameter_dir'] = f"/mnt/exacloud/dteber_woodwellclimate_org/output/batch_{index}/parameters/"
    config_data['IO']['output_dir'] = f"/mnt/exacloud/dteber_woodwellclimate_org/output/batch_{index}/output/"
    config_data['IO']['output_spec_file'] = f"/mnt/exacloud/dteber_woodwellclimate_org/output/batch_{index}/config/output_spec.csv"
    config_data['IO']['runmask_file'] = f"/mnt/exacloud/dteber_woodwellclimate_org/output/batch_{index}/input/run-mask.nc"

    config_data['IO']['hist_climate_file'] = f"/mnt/exacloud/dteber_woodwellclimate_org/output/batch_{index}/input/historic-climate.nc"
    config_data['IO']['proj_climate_file'] = f"/mnt/exacloud/dteber_woodwellclimate_org/output/batch_{index}/input/projected-climate.nc"
    config_data['IO']['veg_class_file'] = f"/mnt/exacloud/dteber_woodwellclimate_org/output/batch_{index}/input/vegetation.nc"
    config_data['IO']['drainage_file'] = f"/mnt/exacloud/dteber_woodwellclimate_org/output/batch_{index}/input/drainage.nc"
    config_data['IO']['soil_texture_file'] = f"/mnt/exacloud/dteber_woodwellclimate_org/output/batch_{index}/input/soil-texture.nc"
    config_data['IO']['co2_file'] = f"/mnt/exacloud/dteber_woodwellclimate_org/output/batch_{index}/input/co2.nc"
    config_data['IO']['proj_co2_file'] = f"/mnt/exacloud/dteber_woodwellclimate_org/output/batch_{index}/input/projected-co2.nc"
    config_data['IO']['topo_file'] = f"/mnt/exacloud/dteber_woodwellclimate_org/output/batch_{index}/input/topo.nc"
    config_data['IO']['fri_fire_file'] = f"/mnt/exacloud/dteber_woodwellclimate_org/output/batch_{index}/input/fri-fire.nc"
    config_data['IO']['hist_exp_fire_file'] = f"/mnt/exacloud/dteber_woodwellclimate_org/output/batch_{index}/input/historic-explicit-fire.nc"
    config_data['IO']['proj_exp_fire_file'] = f"/mnt/exacloud/dteber_woodwellclimate_org/output/batch_{index}/input/projected-explicit-fire.nc"

    with open(config_file, 'w') as f:
        json.dump(config_data, f, indent=4)

    # Write slurm_runner.sh file
    with open(os.path.join(OUTPUT_DIR, f"batch_{index}/slurm_runner.sh"), 'w') as f:
        f.write(f"""#!/bin/bash -l

#SBATCH --job-name="ddt-batch-{index}"

#SBATCH -p compute

#SBATCH -o /mnt/exacloud/dteber_woodwellclimate_org/slurm-logs/batch-{index}.out

#SBATCH -N 1

echo $SLURM_JOB_NODELIST

ulimit -s unlimited
ulimit -l unlimited

. /dependencies/setup-env.sh
. /etc/profile.d/z00_lmod.sh
module load openmpi

mpirun --use-hwthread-cpus {os.path.join(os.environ['HOME'], 'dvm-dos-tem/dvmdostem')} -l disabled -f {os.path.join(OUTPUT_DIR, f'batch_{index}/config/config.js')} -p 10 -e 10 -s 10 -t 10 -n 10
""")