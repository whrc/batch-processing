import os
import getpass
import subprocess


def run_command(command):
    """Executes a shell command."""
    subprocess.run(command, check=True)


def handle_init(args):
    user = getpass.getuser()
    if user == "root":
        print("Do not run as root or with sudo.")
        os.exit(2)

    home_dir = os.getenv("HOME")
    dvm_dos_tem_dir = os.path.join(home_dir, "dvm-dos-tem")

    # Copy necessary files from the cloud
    run_command(["gsutil", "-m", "cp", "-r", "gs://slurm-homefs/dvm-dos-tem", home_dir])
    run_command(["chmod", "+x", os.path.join(dvm_dos_tem_dir, "dvmdostem")])
    run_command(
        [
            "gsutil",
            "-m",
            "cp",
            "gs://four-basins/all-merged/config/output_spec.csv",
            os.path.join(dvm_dos_tem_dir, "config/output_spec.csv"),
        ]
    )

    # Copy the input data if the path is provided
    data = args.input_data
    if data:
        run_command(["mkdir", "-p", "input/"])
        run_command(["gsutil", "-m", "cp", "-r", data, "."])

    exacloud_user_dir = f"/mnt/exacloud/{user}"
    slurm_logs_dir = os.path.join(home_dir, "slurm-logs")

    # Create directories and set permissions
    run_command(["sudo", "-H", "mkdir", "-p", exacloud_user_dir])
    run_command(["sudo", "-H", "chown", "-R", f"{user}:{user}", exacloud_user_dir])
    run_command(["sudo", "mkdir", "-p", slurm_logs_dir])

    # Set striping on the user directory
    run_command(["lfs", "setstripe", "-S", "0.25M", exacloud_user_dir])

    # Modify the configuration file
    config_path = os.path.join(dvm_dos_tem_dir, "config/config.js")
    output_dir_path = os.path.join(exacloud_user_dir, "output/")
    run_command(
        [
            "sed",
            "-i",
            f's|"output_dir":\s*"output/",|"output_dir": "{output_dir_path}",|',
            config_path,
        ]
    )
