import os
import subprocess

from batch_processing.cmd.base import BaseCommand


class BatchMergeCommand(BaseCommand):
    def __init__(self, args):
        self._args = args

    def execute(self):
        STAGES = ["eq", "sp", "tr", "sc"]
        RES_STAGES = ["pr", "eq", "sp", "tr", "sc"]
        TIMESTEPS = ["daily", "monthly", "yearly"]

        os.makedirs(self.result_dir, exist_ok=True)

        variables = open(self.output_spec_path).readlines()[0].split(",")[0]

        # If merging files for a single variable
        if len(os.sys.argv) != 1:
            print("single variable:", os.sys.argv[1])
            variables = os.sys.argv[1]

        # First handle all the normal outputs.
        for variable in variables.split(","):
            print("Processing variable:", variable.strip())
            if variable.strip() != "Name":  # ignore the header
                for stage in STAGES:
                    print("  --> stage:", stage)

                    for timestep in TIMESTEPS:
                        print("  --> timestep:", timestep)

                        # Determine the file name of the outputs variable
                        # for the specific run mode and time step
                        filename = f"{variable.strip()}_{timestep}_{stage}.nc"
                        print("  --> find", filename)

                        # List all the output files for the variable in question
                        # in every output sub-directory
                        # (one directory = one sub-regional run)
                        filelist = subprocess.getoutput(
                            f"find {self.batch_dir} -maxdepth 4 -type f -name '{filename}'"
                        )
                        # print("  --> filelist:", filelist)

                        if filelist:
                            # Concatenate all these files together
                            print("merge files")

                            # Something is messed up with my quoting, as this only
                            # works with the filelist variable **unquoted** which
                            # I think is bad practice.
                            subprocess.run(
                                ["ncea", "-O", "-h", "-y", "avg"]
                                + filelist.split()
                                + [f"{self.result_dir}/{filename}"]
                            )
                        else:
                            print("  --> nothing to do; no files found...")

        # Next handle the restart files
        for stage in RES_STAGES:
            filename = f"restart-{stage}.nc"
            print("  --> stage:", stage)

            filelist = subprocess.getoutput(
                f"find {self.batch_dir} -maxdepth 4 -type f -name '{filename}'"
            )
            print("THE FILE LIST IS:", filelist)

            if filelist:
                subprocess.run(
                    ["ncea", "-O", "-h", "-y", "avg"]
                    + filelist.split()
                    + [f"{self.result_dir}/{filename}"]
                )
            else:
                print(f"nothing to do - no restart files for stage {stage} found?")

        # Next handle the run_status file
        filelist = subprocess.getoutput(
            f"find {self.batch_dir} -maxdepth 4 -type f -name 'run_status.nc'"
        )
        print("THE FILE LIST IS:", filelist)
        if filelist:
            # NOTE: for some reason the 'avg' operator does not work with this file!!
            subprocess.run(
                ["ncea", "-O", "-h", "-y", "max"]
                + filelist.split()
                + [f"{self.result_dir}/run_status.nc"]
            )
        else:
            print("nothing to do - no run_status.nc files found?")

        # Finally, handle the fail log
        filelist = subprocess.getoutput(
            f"find {self.batch_dir} -maxdepth 4 -type f -name 'fail_log.txt'"
        )
        print("THE FILE LIST IS:", filelist)
        if filelist:
            for f in filelist.split():
                with open(f) as f_read:
                    with open(f"{self.result_dir}/fail_log.txt", "a") as f_write:
                        f_write.write(f_read.read())
        else:
            print("nothing to do - no fail_log.txt files found?")
