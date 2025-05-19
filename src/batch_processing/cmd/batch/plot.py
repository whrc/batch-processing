import os
import sys
import numpy as np
import matplotlib.pyplot as plt
from netCDF4 import Dataset
from matplotlib.backends.backend_pdf import PdfPages
from pathlib import Path

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import extract_variable_name


class BatchPlotCommand(BaseCommand):
    # variable name can't start with a number, so an underscore added
    _4D_VARIABLES = ["TLAYER"]

    def __init__(self, args):
        super().__init__()
        self._args = args
        self.base_batch_dir = Path(self.exacloud_user_dir, args.batches)
        self.result_dir = self.base_batch_dir / "all_merged"

        if not self.result_dir.exists():
            raise FileNotFoundError(f"{self.result_dir} doesn't exist")

    def _plot_3d_variable(self, nc_file, variable_name):
        """
        Reads the specified variable from a NetCDF file, calculates mean over time,
        and returns a Matplotlib figure.
        """
        try:
            with Dataset(nc_file, "r") as nc:
                # Check if variable exists
                if variable_name not in nc.variables:
                    print(f"Variable {variable_name} not found in {nc_file}")
                    return None

                # Extract dimensions
                time_dim = "time" if "time" in nc.dimensions else None
                Y = nc.dimensions['y'].size
                X = nc.dimensions['x'].size

                # Extract data
                var_data = nc.variables[variable_name][:]
                
                # Replace fill values with NaN
                fill_value = nc.variables[variable_name]._FillValue if hasattr(nc.variables[variable_name], "_FillValue") else np.nan
                var_data = np.where(var_data == fill_value, np.nan, var_data)

                print('time_dim:',time_dim)
                t_size = nc.dimensions[time_dim].size
                time_steps = np.arange(t_size)
                print(t_size)
                if t_size == 12000:
                    print("Reducing time dimension by averaging every 12 steps...")
        
                    # Ensure time is actually divisible by 12
                    if t_size % 12 != 0:
                        print("⚠️ Warning: Time dimension is not exactly divisible by 12. Skipping downsampling.")
                    else:
                        # Reshape: (12000, Y, X) → (1000, 12, Y, X)
                        var_data = var_data.reshape(1000, 12, Y, X)

                        # Compute mean along the second axis (reducing time dimension)
                        var_data = np.nanmean(var_data, axis=1)  # Shape: (1000, Y, X)
                        time_steps = np.arange(var_data.shape[0])

                        print("✅ New time dimension size:", var_data.shape[0])  # Should be 1000

                # Compute mean var_data over X and Y for each time step
                mean_var_data = np.nanmean(var_data, axis=(1, 2))  # Shape: (time,)
                std_var_data = np.nanstd(var_data, axis=(1, 2))  # Standard deviation for shading
                # Plot
                fig, axes = plt.subplots(1, 3, figsize=(12, 5))

                # Plot var_data at first time step
                im0 = axes[0].imshow(var_data[0,:,:], cmap="viridis", origin="lower", aspect="auto")
                axes[0].set_title(f"{variable_name} - Year 1")
                axes[0].set_xlabel("X")
                axes[0].set_ylabel("Y")
                fig.colorbar(im0, ax=axes[0], label="Depth (m)")

                # Plot var_data at last time step
                imN = axes[1].imshow(var_data[-1,:,:], cmap="viridis", origin="lower", aspect="auto")
                axes[1].set_title(f"{variable_name} - Year N")
                axes[1].set_xlabel("X")
                axes[1].set_ylabel("Y")
                fig.colorbar(imN, ax=axes[1], label="Depth (m)")

                axes[2].plot(time_steps, mean_var_data, color="b", label="Mean ALD")
                axes[2].fill_between(time_steps, mean_var_data - std_var_data, mean_var_data + std_var_data, color="b", alpha=0.2, label="±1 Std Dev")

                # Labels and titles
                axes[2].set_xlabel("Time (years)")
                axes[2].set_ylabel(f"{variable_name}")
                axes[2].set_title(f"Mean {variable_name} Over Time with ±1 Std Dev")


                plt.tight_layout()

                return fig

        except Exception as e:
            print(f"Error processing {nc_file}: {e}")
            return None

    def _plot_4d_variable(self, nc_file, variable_name):
        print(f"plotting {nc_file} for {variable_name}")

    def execute(self):
        # Only files starting with a capital letter
        nc_files = [f for f in os.listdir(self.result_dir) if f.endswith(".nc") and f[0].isupper()]
        if not nc_files:
            raise ValueError("No valid NetCDF files found in the specified folder.")

        new_file_path = os.path.join(self.result_dir, "summary_plots.pdf")

        with PdfPages(new_file_path) as pdf:
            for nc_file in nc_files:
                nc_file_path = os.path.join(self.result_dir, nc_file)
                variable_name = extract_variable_name(nc_file)

                if variable_name:
                    if variable_name in self._4D_VARIABLES:
                        fig = self._plot_4d_variable(nc_file_path, variable_name)
                        # fig = 10
                    else:
                        fig = self._plot_3d_variable(nc_file_path, variable_name)
                        # fig = 10

                    if fig:
                        pdf.savefig(fig)  # Save the figure to PDF
                        plt.close(fig)
                        print(f"Added plot for {variable_name} from {nc_file}")

        print(f"Plots saved in {new_file_path}")
