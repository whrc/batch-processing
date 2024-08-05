#!/bin/bash -l

#SBATCH --job-name="$job_name"

#SBATCH -p $partition

#SBATCH -o $log_path

#SBATCH -N 1

. /etc/profile.d/z00_lmod.sh

cd $home
git clone https://github.com/whrc/batch-processing.git
cd batch-processing/
pip install .

~/.local/bin/bp batch split -p $p -e $e -s $s -t $t -n $n -i $input_path -b $batches -l $log_level --launch-as-job
