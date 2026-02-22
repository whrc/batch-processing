#!/bin/bash -l

#SBATCH --job-name="$job_name"

#SBATCH -p $partition

#SBATCH -o $log_file_path

#SBATCH -N 1

ulimit -s unlimited
ulimit -l unlimited

source /etc/profile.d/z00_lmod.sh
module purge
module use /mnt/exacloud/lustre/modulefiles
module avail

module load openmpi/v4.1.x
module load dvmdostem-deps/2026-02

# Suppress PMIx compression library warning (optional, cosmetic)
export PMIX_MCA_pcompress_base_silence_warning=1

# Lustre: disable HDF5 file locking (incompatible with Lustre without flock)
export HDF5_USE_FILE_LOCKING=FALSE

# OpenMPI 4.1.x: use ROMIO instead of buggy OMPIO for NetCDF/HDF5 parallel I/O
mpirun -x HDF5_USE_FILE_LOCKING -x PMIX_MCA_pcompress_base_silence_warning --use-hwthread-cpus --mca io ^ompio $dvmdostem_binary -f $config_path -l $log_level --max-output-volume=-1 $additional_flags -p $p -e $e -s $s -t $t -n $n
