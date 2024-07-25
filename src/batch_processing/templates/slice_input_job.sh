#!/bin/bash -l

#SBATCH --job-name="$job_name"

#SBATCH -p $partition

#SBATCH -o $log_path

#SBATCH -N 1

. /etc/profile.d/z00_lmod.sh

git clone https://github.com/whrc/batch-processing.git
cd batch-processing/
pip install .

~/.local/bin/bp slice_input -i $input_path -o $output_path --launch-as-job
