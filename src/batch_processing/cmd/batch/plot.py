import os
import numpy as np
import matplotlib.pyplot as plt
from netCDF4 import Dataset
from matplotlib.ticker import MaxNLocator
import matplotlib.cm as cm
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

                # print('time_dim:',time_dim)
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
        """
        Average 100 years of data for each month and display the monthly temperature profiles.
        This will show the seasonal cycle in the vertical temperature structure.
        """
        try:
            with Dataset(nc_file, 'r') as nc:
                # Get dimensions
                num_layers = nc.dimensions['layer'].size
                num_times = nc.dimensions['time'].size

                # Calculate how many complete years we have (assuming monthly data)
                num_years = num_times // 12
                print(f"Total number of years available: {num_years}")

                # Use up to 100 years, or whatever is available
                years_to_use = min(100, num_years)
                print(f"Using {years_to_use} years for monthly averages")

                # Find valid layers (excluding those with mostly zeros)
                valid_layers = []
                for layer_idx in range(num_layers):
                    # Check the first time step for this layer
                    layer_data = nc.variables[variable_name][0, layer_idx, :, :]

                    # Check if this layer has meaningful data (not all zeros)
                    zero_percentage = np.sum(layer_data == 0) / layer_data.size * 100

                    # Skip layers that are mostly zeros (likely default values)
                    if zero_percentage <= 80:
                        valid_layers.append(layer_idx)
                    else:
                        print(f"Layer {layer_idx}: Skipping - {zero_percentage:.2f}% of values are zero")

                valid_layers = np.array(valid_layers)

                # Create arrays to store monthly average temperatures
                # 12 months, each with data for all valid layers
                monthly_avg_temps = np.zeros((12, len(valid_layers)))

                # Process each month
                for month in range(12):
                    print(f"Processing month {month+1}...")

                    # Collect temperature data for this month across years
                    month_data = []

                    # Get data for this month from each year
                    for year in range(years_to_use):
                        time_idx = year * 12 + month
                        if time_idx < num_times:  # Make sure we don't exceed the data bounds
                            # For each valid layer, get the temperature data
                            for i, layer_idx in enumerate(valid_layers):
                                layer_data = nc.variables[variable_name][time_idx, layer_idx, :, :]
                                avg_temp = np.nanmean(layer_data)  # Average across spatial dimensions

                                # If we're in the first year, initialize the list
                                if year == 0:
                                    month_data.append([avg_temp])
                                else:
                                    month_data[i].append(avg_temp)

                    # Calculate average temperature for each layer in this month (across all years)
                    for i in range(len(valid_layers)):
                        monthly_avg_temps[month, i] = np.nanmean(month_data[i])

                # Month names for the plot
                month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                            'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

                # Create the plot
                fig, ax = plt.subplots(figsize=(14, 9))

                # Use a cyclic colormap for months (so December and January are similar colors)
                cmap = cm.twilight_shifted

                # Plot temperature vs. layer for each month
                for month in range(12):
                    color = cmap(month / 12)
                    ax.plot(monthly_avg_temps[month], valid_layers, '-', 
                            linewidth=2.5,
                            color=color, 
                            label=f'{month_names[month]}')

                # Configure axes for depth:
                # 1. Move x-axis to the top
                ax.xaxis.set_ticks_position('top')
                ax.xaxis.set_label_position('top')

                # 2. Set y-axis limits to ensure layer 0 is at the top and increases downward
                ax.set_ylim(max(valid_layers), min(valid_layers))

                # 3. Make sure y-axis ticks are integers (layer numbers)
                ax.yaxis.set_major_locator(MaxNLocator(integer=True))

                # Add vertical line at x=0
                ax.axvline(x=0, color='black', linestyle='-', linewidth=0.5)

                # Add grid
                ax.grid(True, linestyle='--', alpha=0.5)

                # Set labels and title
                ax.set_xlabel('Temperature (°C)', fontsize=14)
                ax.set_ylabel('Layer/Depth', fontsize=14)
                ax.set_title(f'Monthly Average Temperature Profiles (Averaged over {years_to_use} years)', 
                            fontsize=16)

                # Add legend with month names
                ax.legend(loc='lower right', fontsize=12)

                return fig

        except Exception as e:
            print(f"Error processing {nc_file}: {e}")
            return None

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
                    print(f"Plotting {variable_name}")
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
