import os
import subprocess
import sys


# todo: refactor this file
def merge_files(
    output_dir_prefix,
    output_spec_path,
    stages,
    res_stages,
    timesteps,
    batch_dir,
    final_dir,
):
    os.makedirs(final_dir, exist_ok=True)
    variables = [line.split(",")[0] for line in open(output_spec_path).readlines()]

    if len(sys.argv) != 1:
        print(f"single variable: {sys.argv[1]}")
        variables = [sys.argv[1]]

    for variable in variables:
        if variable != "Name":
            print(f"Processing variable: {variable}")

            for stage in stages:
                print(f"  --> stage: {stage}")

                for timestep in timesteps:
                    print(f"  --> timestep: {timestep}")

                    filename = f"{variable}_{timestep}_{stage}.nc"
                    print(f"  --> find {filename}")

                    filelist = subprocess.getoutput(
                        f"find {batch_dir} -maxdepth 4 -type f -name {filename}"
                    ).splitlines()

                    if filelist:
                        print("merge files")
                        subprocess.run(
                            ["ncea", "-O", "-h", "-y", "avg"]
                            + filelist
                            + [f"{final_dir}/{filename}"]
                        )
                    else:
                        print("  --> nothing to do; no files found...")

    for stage in res_stages:
        filename = f"restart-{stage}.nc"
        print(f"  --> stage: {stage}")

        filelist = subprocess.getoutput(
            f"find {batch_dir} -maxdepth 4 -type f -name {filename}"
        ).splitlines()

        if filelist:
            subprocess.run(
                ["ncea", "-O", "-h", "-y", "avg"]
                + filelist
                + [f"{final_dir}/{filename}"]
            )
        else:
            print("nothing to do - no restart files for stage {stage} found?")

    handle_special_files(batch_dir, final_dir, "run_status.nc", "max")
    handle_special_files(batch_dir, final_dir, "fail_log.txt", "cat")


def handle_special_files(batch_dir, final_dir, filename, operation):
    filelist = subprocess.getoutput(
        f"find {batch_dir} -maxdepth 4 -type f -name {filename}"
    ).splitlines()
    print(f"THE FILE LIST IS: {filelist}")

    if filelist:
        if operation == "max":
            subprocess.run(
                ["ncea", "-O", "-h", "-y", "max"]
                + filelist
                + [f"{final_dir}/{filename}"]
            )
        elif operation == "cat":
            with open(f"{final_dir}/{filename}", "a") as outfile:
                for f in filelist:
                    with open(f) as infile:
                        outfile.write(infile.read())
    else:
        print(f"nothing to do - no {filename} files found?")


def handle_batch_merge(args):
    output_dir_prefix = "/mnt/exacloud/{}".format(os.environ["USER"])
    output_spec_path = "/home/{}/dvm-dos-tem/config/output_spec.csv".format(
        os.environ["USER"]
    )
    stages = ["eq", "sp", "tr", "sc"]
    res_stages = ["pr", "eq", "sp", "tr", "sc"]
    timesteps = ["daily", "monthly", "yearly"]
    batch_dir = f"{output_dir_prefix}/output/batch-run"
    final_dir = f"{output_dir_prefix}/all-merged"

    merge_files(
        output_dir_prefix,
        output_spec_path,
        stages,
        res_stages,
        timesteps,
        batch_dir,
        final_dir,
    )
