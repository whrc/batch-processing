#!/bin/bash -l

#SBATCH --job-name="$job_name"

#SBATCH -p $partition

#SBATCH -o $log_file_path

#SBATCH -N 1

ulimit -s unlimited
ulimit -l unlimited

. /dependencies/setup-env.sh
. /etc/profile.d/z00_lmod.sh
module load openmpi

mpirun --use-hwthread-cpus $dvmdostem_binary -f $config_path -l $log_level --max-output-volume=-1 $additional_flags -p $p -e $e -s $s -t $t -n $n