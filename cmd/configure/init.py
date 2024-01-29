from batch_processing.constants import (
    BUCKET_DVMDOSTEM,
    BUCKET_OUTPUT_SPEC,
    CONFIG_PATH,
    DVMDOSTEM_BIN_PATH,
    EXACLOUD_USER_DIR,
    HOME,
    INPUT_DIR,
    OUTPUT_PATH,
    OUTPUT_SPEC_PATH,
    SLURM_LOG_PATH,
    USER,
)
from batch_processing.utils import run_command


# todo: display info messages after the each operation is completed
def handle_init(args):
    if USER == "root":
        raise ValueError("Do not run as root or with sudo.")

    data = args.input_data
    if data:
        if not data.startswith("gs://"):
            raise ValueError("The input path needs to be started with gs://")

        run_command(["mkdir", "-p", INPUT_DIR])
        # todo: use gcloud's python client package
        run_command(["gsutil", "-m", "cp", "-r", data, INPUT_DIR])

    # Copy necessary files from the cloud
    run_command(["gsutil", "-m", "cp", "-r", BUCKET_DVMDOSTEM, HOME])
    run_command(["chmod", "+x", DVMDOSTEM_BIN_PATH])
    run_command(
        [
            "gsutil",
            "-m",
            "cp",
            BUCKET_OUTPUT_SPEC,
            OUTPUT_SPEC_PATH,
        ]
    )

    run_command(["sudo", "-H", "mkdir", "-p", EXACLOUD_USER_DIR])
    run_command(["sudo", "-H", "chown", "-R", f"{USER}:{USER}", EXACLOUD_USER_DIR])
    run_command(["sudo", "mkdir", "-p", SLURM_LOG_PATH])

    run_command(["lfs", "setstripe", "-S", "0.25M", EXACLOUD_USER_DIR])

    # Modify `output_dir` field in the configuration file
    # todo: move this operation to cmd/input.py file
    run_command(
        [
            "sed",
            "-i",
            f's|"output_dir":\s*"output/",|"output_dir": "{OUTPUT_PATH}",|',
            CONFIG_PATH,
        ]
    )
