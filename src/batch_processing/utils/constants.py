import os

HOME = os.getenv("HOME")
USER = os.getenv("USER")
INPUT_DIR = os.path.join(HOME, "input")
SLURM_LOG_PATH = os.path.join(HOME, "slurm-logs")
DVMDOSTEM_DIR = os.path.join(HOME, "dvm-dos-tem")
BUCKET_DVMDOSTEM = "gs://slurm-homefs/dvm-dos-tem"
# ask: why are we copying this output_spec.csv to again? can we not put the file in a bucket already?
BUCKET_OUTPUT_SPEC = "gs://four-basins/all-merged/config/output_spec.csv"
CONFIG_PATH = os.path.join(DVMDOSTEM_DIR, "config/config.js")
