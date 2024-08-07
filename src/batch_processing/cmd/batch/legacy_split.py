import json
import os
import shutil
import textwrap

import netCDF4 as nc
import numpy as np
from rich import print
from rich.progress import track

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import clean_and_load_json, get_progress_bar, mkdir_p


class BatchLegacySplitCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args
        self._cells_per_batch = self._args.cells_per_batch

    def execute(self):
        # Look in the config file to figure out where the full-domain runmask is.
        with open(self.config_path) as f:
            input_str = f.read()

        # the config file contains comments which are not valid
        # therefore, we are removing them before parsing
        j = clean_and_load_json(input_str)
        BASE_RUNMASK = j["IO"]["runmask_file"]
        BASE_OUTDIR = j["IO"]["output_dir"]

        # Figure out how many batches are necessary to complete the full run.
        # This is somewhat restricted by how cells are assigned to processes.
        with nc.Dataset(BASE_RUNMASK, "r") as runmask:
            TOTAL_CELLS_TO_RUN = np.count_nonzero(runmask.variables["run"])
            print(f"Total cells to run: {TOTAL_CELLS_TO_RUN}")

        nbatches = TOTAL_CELLS_TO_RUN / self._cells_per_batch

        # If there are extra cells, or fewer cells than self._cells_per_batch
        if TOTAL_CELLS_TO_RUN % self._cells_per_batch != 0:
            print("[blue]Adding another batch to pick up stragglers![/blue]")
            nbatches += 1

        nbatches = int(nbatches)
        print("[blue]NUMBER OF BATCHES: [/blue]", nbatches)

        # SETUP DIRECTORIES
        print("[blue]Removing any existing staging or batch run directories[/blue]")
        if os.path.isdir(BASE_OUTDIR + "/batch-run"):
            shutil.rmtree(BASE_OUTDIR + "/batch-run")

        for batch_id in track(
            range(0, nbatches), description="[blue]Setting up the batches[/blue]"
        ):
            mkdir_p(BASE_OUTDIR + f"/batch-run/batch-{batch_id}")

            work_dir = BASE_OUTDIR + "/batch-run"

            shutil.copy(BASE_RUNMASK, work_dir + f"/batch-{batch_id}/")
            shutil.copy(self.config_path, work_dir + f"/batch-{batch_id}/")

            with nc.Dataset(
                work_dir + f"/batch-{batch_id}/run-mask.nc", "a"
            ) as runmask:
                runmask.variables["run"][:] = np.zeros(runmask.variables["run"].shape)

        # BUILD BATCH SPECIFIC RUN-MASKS
        with nc.Dataset(BASE_RUNMASK, "r") as runmask:
            nz_ycoords = runmask.variables["run"][:].nonzero()[0]
            nz_xcoords = runmask.variables["run"][:].nonzero()[1]

        # For every cell that is turned on in the main run-mask, we assign this cell
        # to a batch to be run, and turn on the corresponding cell in the batch's
        # run mask.
        batch = 0
        cells_in_sublist = 0
        coord_list = list(zip(nz_ycoords, nz_xcoords))

        with nc.Dataset(work_dir + f"/batch-{batch}/run-mask.nc", "a") as grp_runmask:
            for i, cell in track(enumerate(coord_list), description="[blue]Turning on pixels in each batch's run mask[/blue]", total=len(coord_list)):
                grp_runmask.variables["run"][cell] = True
                cells_in_sublist += 1

                if cells_in_sublist == self._cells_per_batch or i == len(coord_list) - 1:
                    # Update the file in batches
                    grp_runmask.sync()
                    cells_in_sublist = 0

                    # If not the last batch, open a new batch file
                    if i != len(coord_list) - 1:
                        batch += 1
                        grp_runmask.close()
                        grp_runmask = nc.Dataset(work_dir + f"/batch-{batch}/run-mask.nc", "a")

            grp_runmask.sync()

        # SUMMARIZE
        number_batches = batch + 1
        print(f"[green]Split cells into {number_batches} batches...[/green]")

        # MODIFY THE CONFIG FILE FOR EACH BATCH
        for batch_num in track(range(0, number_batches), description="[blue]Modifying each batch's config file changing path to run mask and to output directory...[/blue]"):
            with open(work_dir + f"/batch-{batch_num}/config.js") as f:
                input_string = f.read()

            # Strip comments from json file
            j = clean_and_load_json(input_string)
            j["IO"]["runmask_file"] = work_dir + f"/batch-{batch_num}/run-mask.nc"
            j["IO"]["output_dir"] = work_dir + f"/batch-{batch_num}/output/"

            output_str = json.dumps(j, indent=2, sort_keys=True)

            with open(work_dir + f"/batch-{batch_num}/config.js", "w") as f:
                f.write(output_str)

        with get_progress_bar() as progress_bar:
            task = progress_bar.add_task(
                "Writing sbatch script for each batch", total=number_batches
            )
            # SUBMIT SBATCH SCRIPT FOR EACH BATCH
            for batch in range(0, number_batches):
                with nc.Dataset(
                    work_dir + f"/batch-{batch}/run-mask.nc", "r"
                ) as runmask:
                    cells_in_batch = np.count_nonzero(runmask.variables["run"])

                assert (
                    cells_in_batch > 0
                ), "[red]PROBLEM! Groups with no cells activated to run![/red]"

                slurm_runner_scriptlet = textwrap.dedent(
                    f"""\
            #!/bin/bash -l

            # Job name, for clarity
            #SBATCH --job-name="ddt-batch-{batch}"

            # Partition specification
            #SBATCH -p {self._args.slurm_partition}

            # Log the output
            #SBATCH -o /mnt/exacloud/{self.user}/slurm-logs/batch-{batch}.out

            # Number of MPI tasks
            #SBATCH -N 1

            echo $SLURM_JOB_NODELIST

            ulimit -s unlimited
            ulimit -l unlimited

            # Load up my custom paths stuff
            . /dependencies/setup-env.sh
            . /etc/profile.d/z00_lmod.sh
            module load openmpi

            cd /home/$USER/dvm-dos-tem

            mpirun --use-hwthread-cpus ./dvmdostem -f {work_dir}/batch-{batch}/config.js -l {self._args.log_level} --max-output-volume=-1 -p {self._args.p} -e {self._args.e} -s {self._args.s} -t {self._args.t} -n {self._args.n}
            """.format(batch, cells_in_batch, work_dir)
                )
                with open(work_dir + f"/batch-{batch}/slurm_runner.sh", "w") as f:
                    f.write(slurm_runner_scriptlet)

                progress_bar.advance(task)

        log_files = os.listdir(self.slurm_log_dir)
        for file_name in track(
            log_files,
            description=f"[blue]Deleting files under {self.slurm_log_dir}[/blue]",
            total=len(log_files),
        ):
            file_path = os.path.join(self.slurm_log_dir, file_name)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
            except Exception as e:
                print(
                    f"[red]Encountered an error while deleting the log file {file_path}: {e}[/red]"
                )

        print(
            f"[bold green]Deletion of files under {self.slurm_log_dir} is completed.[/bold green]"
        )

        print("[bold green]Split operation is completed.[/bold green]")
        print(f"[bold blue]Please check {self.batch_dir} for the results.[/bold blue]")
