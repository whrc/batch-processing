import os

HOME = os.getenv("HOME")
USER = os.getenv("USER")
INPUT_DIR = os.path.join(HOME, "input")
EXACLOUD_USER_DIR = f"/mnt/exacloud/{USER}"
SLURM_LOG_PATH = os.path.join(HOME, "slurm-logs")
DVMDOSTEM_DIR = os.path.join(HOME, "dvm-dos-tem")
DVMDOSTEM_BIN_PATH = os.path.join(DVMDOSTEM_DIR, "dvmdostem")
BUCKET_DVMDOSTEM = "gs://slurm-homefs/dvm-dos-tem"
BUCKET_OUTPUT_SPEC = "gs://four-basins/all-merged/config/output_spec.csv"
OUTPUT_SPEC_PATH = os.path.join(DVMDOSTEM_DIR, "config/output_spec.csv")
CONFIG_PATH = os.path.join(DVMDOSTEM_DIR, "config/config.js")
OUTPUT_PATH = os.path.join(EXACLOUD_USER_DIR, "output/")
IO_FILE_KEYS = [
    "hist_climate_file",
    "proj_climate_file",
    "veg_class_file",
    "drainage_file",
    "soil_texture_file",
    "co2_file",
    "proj_co2_file",
    "runmask_file",
    "topo_file",
    "fri_fire_file",
    "hist_exp_fire_file",
    "proj_exp_fire_file",
    "topo_file",
]
