import os
import subprocess


def get_batch_count(batch_dir: str) -> int:
    full_paths = [os.path.join(batch_dir, item) for item in os.listdir(batch_dir)]
    total_batches = sum(1 for path in full_paths if os.path.isdir(path))

    return total_batches


def submit_batches():
    USER = os.getenv("USER")
    BATCH_DIR = f"/mnt/exacloud/{USER}/output/batch-run"
    total_batches = get_batch_count(BATCH_DIR)
    for index in range(0, total_batches):
        slurm_script_path = f"{BATCH_DIR}/batch-{index}/slurm_runner.sh"
        subprocess.run(["sbatch", slurm_script_path])


def handle_batch_run(args):
    # todo: add a progress bar like split.py
    submit_batches()
