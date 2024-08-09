from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import ListedColormap
from netCDF4 import Dataset
from numpy.ma.core import MaskedArray

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import (
    get_batch_number,
    get_dimensions,
    write_text_file,
)

WHITE = 0
BLACK = 1
RED = 2
GREEN = 3
GRAY = 4


class MapCommand(BaseCommand):
    """Generates a visualization of the run results, and identifies failed cells."""
    def __init__(self, args):
        super().__init__()
        self.base_batch_dir = Path(self.exacloud_user_dir, args.batches)

    def execute(self):
        print("Pulling run_status and run-mask files...")
        run_status_files = [
            file for file in self.base_batch_dir.glob("batch_*/output/run_status.nc")
        ]
        run_mask_files = [
            file for file in self.base_batch_dir.glob("batch_*/input/run-mask.nc")
        ]

        run_status_batch_numbers = [get_batch_number(file) for file in run_status_files]
        run_status_files.sort(key=get_batch_number)

        run_mask_batch_numbers = [get_batch_number(file) for file in run_mask_files]
        run_mask_files.sort(key=get_batch_number)

        missing_batches = list(
            set(run_mask_batch_numbers) - set(run_status_batch_numbers)
        )
        missing_batches.sort()

        run_status_data = []
        X, Y = get_dimensions(run_mask_files[0])
        print("Organizing files and filling missing data...")
        for file in run_status_files:
            batch_number = get_batch_number(file)
            if batch_number in missing_batches:
                data = generate_empty_array((Y, X))
            else:
                data = get_variable(file, "run_status")

            run_status_data.append(data[0, :])

        run_status_matrix = np.stack(run_status_data, axis=0)

        run_mask_data = []

        for file in run_mask_files:
            batch_number = get_batch_number(file)
            if batch_number in missing_batches:
                data = generate_empty_array((Y, X))
            else:
                data = get_variable(file, "run")

            run_mask_data.append(data[0, :])

        run_mask_matrix = np.stack(run_mask_data, axis=0)

        # Initialize a numeric matrix to store color codes for each coordinate
        numeric_color_matrix = np.zeros(run_status_matrix.shape)

        for (i, j), elem in np.ndenumerate(run_status_matrix):
            # the cell is skipped
            if elem == 0:
                numeric_color_matrix[i, j] = WHITE
            # the cell is failed
            elif elem < 0:
                run_mask_val = run_mask_matrix[i, j]
                # we are not supposed to run this cell
                if run_mask_val == 0:
                    numeric_color_matrix[i, j] = BLACK
                # we are supposed to run this cell
                elif run_mask_val == 1:
                    numeric_color_matrix[i, j] = RED
                # unexpected value
                else:
                    numeric_color_matrix[i, j] = GRAY
            # the cell successfully ran
            else:
                numeric_color_matrix[i, j] = GREEN

        # Define the colormap
        cmap = ListedColormap(["white", "black", "red", "gray", "green"])

        # Plot the matrix
        plt.figure(figsize=(10, 8))
        plt.imshow(numeric_color_matrix, cmap=cmap, aspect="auto")
        plt.colorbar(ticks=[0, 1, 2, 3, 4], label="Status")
        plt.clim(-0.5, 3.5)  # Set the limits for color bar to align with categories

        # Set the color bar labels
        plt.gca().images[-1].colorbar.set_ticks([0, 1, 2, 3, 4])
        plt.gca().images[-1].colorbar.set_ticklabels(
            ["Disabled", "Should Be Disabled", "Failure", "Unknown Case", "Success"]
        )

        plt.title("Run Status Visualization")
        plt.xlabel("Coordinate X")
        plt.ylabel("Coordinate Y")
        plt.tight_layout()

        # Save the plot as an image file
        output_image_path = self.base_batch_dir / "run_status_visualization.png"
        plt.savefig(output_image_path)

        print(f"Visualization saved as {output_image_path}")

        failed_cells = []
        row = []
        temp_i = 0
        for (i, j), elem in np.ndenumerate(numeric_color_matrix):
            if elem == RED:
                if temp_i != i:
                    temp_i = i
                    if row:
                        failed_cells.append(row)
                        row = []
                else:
                    row.append((i, j))

        failed_cells.append(row)

        failed_coords_file_path = self.base_batch_dir / "failed_cell_coords.txt"
        content = "\n\n".join([str(row) for row in failed_cells])
        write_text_file(failed_coords_file_path, content)

        print(f"The failed cell coordinates are written to {failed_coords_file_path}")


def get_variable(file_path: str, variable_name: str) -> MaskedArray:
    with Dataset(file_path, "r") as dataset:
        data = dataset.variables[variable_name][:]
    return data


def generate_empty_array(shape: tuple, fill_value: int = -999) -> np.ma.MaskedArray:
    data = np.full(shape, fill_value)
    return np.ma.masked_array(data, fill_value=fill_value)
