import subprocess


def run_command(command):
    """Executes a shell command."""
    subprocess.run(command, check=True)
