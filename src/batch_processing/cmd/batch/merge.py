import gcsfs
import xarray as xr
import glob
from pathlib import Path
import numpy as np
from dask.distributed import Client

from batch_processing.cmd.base import BaseCommand
from batch_processing.cmd.batch.check import BatchCheckCommand
from batch_processing.utils.utils import get_batch_number, get_dimensions, get_gcsfs, get_cluster


class BatchMergeCommand(BaseCommand):
    __MIN_CELL_COUNT_FOR_DASK = 40_000

    def __init__(self, args):
        super().__init__()
        self._args = args
        self.base_batch_dir = Path(self.exacloud_user_dir, args.batches)
        self.result_dir = self.base_batch_dir / "all_merged"
        self.result_dir.mkdir(parents=True, exist_ok=True)

    def _get_available_batches(self):
        """Get list of available batch directories, sorted by batch number."""
        batch_dirs = []
        for p in self.base_batch_dir.iterdir():
            if "batch_" in p.as_posix() and p.is_dir():
                batch_dirs.append(p)
        return sorted(batch_dirs, key=lambda x: get_batch_number(x.name))

    def _get_available_output_files(self):
        """Get list of output files from the first available batch."""
        batch_dirs = self._get_available_batches()
        if not batch_dirs:
            return []
        
        first_batch_output_dir = batch_dirs[0] / "output"
        if not first_batch_output_dir.exists():
            return []
        
        return [f.name for f in first_batch_output_dir.iterdir() if f.is_file()]

    def _create_canvas_for_variable(self, output_file, available_batches):
        """Create a canvas dataset for a specific output file using available batches."""
        print(f"Creating canvas for {output_file}")
        
        # Determine concatenation dimension
        concat_dim = "y"
        if output_file.startswith("restart") or output_file == "run_status.nc":
            concat_dim = "Y"
        
        # Collect all available files for this output
        available_files = []
        batch_coords = []
        
        for batch_dir in available_batches:
            file_path = batch_dir / "output" / output_file
            if file_path.exists():
                available_files.append(file_path.as_posix())
                batch_coords.append(get_batch_number(batch_dir.name))
            else:
                print(f"  Warning: {output_file} not found in {batch_dir.name}")
        
        if not available_files:
            print(f"  Error: No files found for {output_file}")
            return None
        
        # Read the first available file to get structure
        first_file = available_files[0]
        first_ds = xr.open_dataset(first_file, engine="h5netcdf", decode_times=False)
        
        # Get all data variables and their info
        data_vars_info = {}
        all_dims = set()
        
        for var_name in first_ds.data_vars:
            var = first_ds[var_name]
            # Get fill value for this variable
            varfv = var.encoding.get('_FillValue')
            if varfv is None:
                # Use appropriate fill value based on data type
                var_dtype = var.dtype
                if np.issubdtype(var_dtype, np.timedelta64):
                    varfv = np.timedelta64('NaT')  # Not a Time for timedelta data
                elif np.issubdtype(var_dtype, np.datetime64):
                    varfv = np.datetime64('NaT')   # Not a Time for datetime data
                else:
                    varfv = np.nan  # Use NaN for numeric data
            
            # Special handling for run_status: use -99 for missing values instead of -9999
            if var_name == 'run_status':
                varfv = -99
            
            # Special handling for total_runtime: preserve as integer seconds, not timedelta64
            if var_name == 'total_runtime':
                # Ensure we use integer fill value, not timedelta64
                if varfv is None or np.issubdtype(type(varfv), np.timedelta64):
                    varfv = -9999  # Use integer fill value for missing runtime values
            
            data_vars_info[var_name] = {
                'dims': list(var.dims),
                'fill_value': varfv,
                'attrs': var.attrs.copy(),
                'encoding': var.encoding.copy()
            }
            all_dims.update(var.dims)
        
        # If no data variables, use all dimensions from the dataset
        if not data_vars_info:
            all_dims = set(first_ds.dims.keys())
        
        # Calculate total canvas size
        total_batch_count = len(available_batches)
        canvas_dims = {}
        canvas_coords = {}
        
        for dim in all_dims:
            if dim == concat_dim:
                # This is the dimension we're concatenating along
                canvas_dims[dim] = first_ds[dim].shape[0] * total_batch_count
                # Create extended coordinates for the canvas
                if dim == 'y':
                    canvas_coords[dim] = np.arange(canvas_dims[dim])
                elif dim == 'Y':
                    canvas_coords[dim] = np.arange(canvas_dims[dim])
            else:
                # Keep original dimensions
                canvas_dims[dim] = first_ds[dim].shape[0]
                canvas_coords[dim] = first_ds[dim].values
        
        # Create canvas dataset with all data variables
        data_vars = {}
        for var_name, var_info in data_vars_info.items():
            dims = var_info['dims']
            fill_value = var_info['fill_value']
            data_vars[var_name] = (tuple(dims), np.full(tuple(canvas_dims[dim] for dim in dims), fill_value))
        
        canvas = xr.Dataset(data_vars, coords=canvas_coords)
        canvas.attrs = first_ds.attrs.copy()
        
        # Set attributes and encoding for all variables
        for var_name, var_info in data_vars_info.items():
            canvas[var_name].attrs = var_info['attrs']
            canvas[var_name].attrs['_FillValue'] = var_info['fill_value']
            canvas[var_name].encoding = var_info['encoding']
        
        first_ds.close()
        return canvas, available_files, batch_coords

    def _fill_canvas_with_batches(self, canvas, output_file, available_files, batch_coords, concat_dim):
        """Fill the canvas with data from available batch files."""
        print(f"Filling canvas for {output_file}")
        
        # Read all available files
        datasets = []
        for file_path in available_files:
            ds = xr.open_dataset(file_path, engine="h5netcdf", decode_times=False)
            datasets.append(ds)
        
        # Concatenate along the specified dimension
        if datasets:
            combined_ds = xr.concat(datasets, dim=concat_dim)
            
            # Fill the canvas with the combined data
            if concat_dim == 'y':
                # For y dimension, we need to map the combined data to the canvas
                canvas_slice = {concat_dim: slice(0, combined_ds[concat_dim].shape[0])}
                canvas = combined_ds.sel(canvas_slice).combine_first(canvas)
            elif concat_dim == 'Y':
                canvas_slice = {concat_dim: slice(0, combined_ds[concat_dim].shape[0])}
                canvas = combined_ds.sel(canvas_slice).combine_first(canvas)
            else:
                canvas = combined_ds.combine_first(canvas)
            
            # Close datasets to free memory
            for ds in datasets:
                ds.close()
            combined_ds.close()
        
        return canvas

    def _merge_with_canvas(self, output_file, output_path):
        """Merge output file using canvas approach, handling missing batches gracefully."""
        available_batches = self._get_available_batches()
        
        if not available_batches:
            print(f"No available batches found for {output_file}")
            return
        
        # Create canvas
        canvas_result = self._create_canvas_for_variable(output_file, available_batches)
        if canvas_result is None:
            return
        
        canvas, available_files, batch_coords = canvas_result
        
        # Determine concatenation dimension
        concat_dim = "y"
        if output_file.startswith("restart") or output_file == "run_status.nc":
            concat_dim = "Y"
        
        # Fill canvas with available data
        canvas = self._fill_canvas_with_batches(canvas, output_file, available_files, batch_coords, concat_dim)
        
        # Save the merged result
        output_file_path = output_path / output_file
        print(f"Saving merged {output_file} to {output_file_path}")
        canvas.to_netcdf(output_file_path.as_posix(), engine="h5netcdf")
        canvas.close()

    def _merge_small_dataset(self, output_file, output_path):
        """Original merge method for small datasets - kept for compatibility."""
        path = self.base_batch_dir / "batch_*" / "output" / output_file
        files = sorted(glob.glob(path.as_posix()), key=get_batch_number)
        concat_dim = "y"
        if output_file.startswith("restart") or output_file == "run_status.nc":
            concat_dim = "Y"

        print(f"Reading {output_file}")
        ds = xr.open_mfdataset(files, engine="h5netcdf", combine="nested", concat_dim=concat_dim, data_vars="minimal", coords="minimal", compat="override", decode_cf=False, decode_times=False)
        ds.to_netcdf(f"{output_path}/{output_file}")

    def _merge(self, output_file, bucket_path):
        """Original merge method for large datasets - kept for compatibility."""
        fs = get_gcsfs()
        path = self.base_batch_dir / "batch_*" / "output" / output_file
        files = sorted(glob.glob(path.as_posix()), key=get_batch_number)
        print(f"Reading {output_file}")

        concat_dim = "y"
        chunk_size = {"time": -1, "x": "auto", "y": "auto"}
        if output_file.startswith("restart") or output_file == "run_status.nc":
            concat_dim = "Y"
            chunk_size = {"X": "auto", "Y": "auto"}
        ds = xr.open_mfdataset(files, engine="h5netcdf", combine="nested", concat_dim=concat_dim, parallel=True, data_vars="minimal", coords="minimal", compat="override", decode_cf=False, decode_times=False)
        ds = ds.chunk(chunk_size)

        gcsmap = gcsfs.mapping.GCSMap(f"{bucket_path}/{output_file[:len(output_file)-3]}.zarr", gcs=fs, check=False, create=True)
        delayed_obj = ds.to_zarr(gcsmap, mode="w", compute=False)
        return delayed_obj

    def _merge_with_dask(self, bucket_path):
        """Original dask merge method - kept for compatibility."""
        cluster = get_cluster(n_workers=10)
        client = Client(cluster)
        client.wait_for_workers(5)
        print(f"Dashboard link: {client.dashboard_link}")

        path = self.base_batch_dir / "batch_0" / "output"
        output_files = [f.name for f in path.iterdir()]

        for f in output_files:
            obj = self._merge(f, bucket_path)
            print(f"Computing {f}")
            obj.compute()

        cluster.close()

    def _check_status(self):
        """Check run status - modified to be more lenient with missing batches."""
        run_status_file_pattern = f"{self.base_batch_dir.as_posix()}/batch_*/output/run_status.nc"
        
        # Find all available run_status files
        available_status_files = glob.glob(run_status_file_pattern)
        
        if not available_status_files:
            print("No run_status.nc files found. Cannot check status.")
            return True  # Allow merging to proceed
        
        try:
            ds = xr.open_mfdataset(available_status_files, engine="h5netcdf", concat_dim="Y", combine="nested")
            # Replace nan values with -99 to indicate they were originally nan
            status_values = ds.run_status.values
            status_values = np.where(np.isnan(status_values), -99, status_values)
            status_unique, status_counts = np.unique(status_values, return_counts=True)
            merged = dict(zip(status_unique, status_counts))
            ds.close()

            if len(status_unique) and status_unique[0] == 100:
                print("All available status codes are 100! Continuing to merge")
                return True
            else:
                print("Status code : count")
                print(merged)
                print(f"Note: Only {len(available_status_files)} out of expected batches have status files")
                
                # Check if auto-approve flag is set
                if hasattr(self._args, 'auto_approve') and self._args.auto_approve:
                    print("Auto-approve enabled. Continuing with merge despite non-100 status codes.")
                    return True
                
                while True:
                    choice = input("Status codes different than 100 were found. Do you want to continue merging (y/n) ? ")
                    choice = choice.lower()
                    if choice in ['y', 'n']:
                        return choice == 'y'
                    print("Please enter 'y' or 'n'.")
        except Exception as e:
            print(f"Error checking status: {e}")
            print("Proceeding with merge despite status check failure")
            return True

    def execute(self):
        """Main execution method with hybrid approach."""
        internal_check_command = BatchCheckCommand(self._args)
        
        # Get the check result to determine if files are missing/incomplete
        equal_files_check, file_counts = internal_check_command._check_equal_output_files(self.base_batch_dir)
        internal_check_command.execute()

        if not self._check_status():
            print("Cancelled.")
            return

        # Get available batches
        available_batches = self._get_available_batches()
        if not available_batches:
            print("No batch directories found!")
            return

        print(f"Found {len(available_batches)} available batches:")
        for batch in available_batches:
            print(f"  - {batch.name}")

        # Get output files from first available batch
        output_files = self._get_available_output_files()
        if not output_files:
            print("No output files found in available batches!")
            return

        print(f"Found {len(output_files)} output files to merge")

        # Check if we should use canvas approach
        total_expected_batches = len([p for p in self.base_batch_dir.iterdir() if "batch_" in p.as_posix()])
        has_missing_batch_dirs = len(available_batches) < total_expected_batches
        has_unequal_files = not equal_files_check
        
        # Use canvas approach if we have missing batch directories OR unequal file counts
        should_use_canvas = has_missing_batch_dirs or has_unequal_files

        if should_use_canvas:
            if has_missing_batch_dirs:
                print(f"Missing batch directories detected ({len(available_batches)}/{total_expected_batches} available). Using canvas approach.")
            if has_unequal_files:
                print("Unequal output file counts detected across batches. Using canvas approach.")
            
            # Use canvas approach for all files
            for output_file in output_files:
                self._merge_with_canvas(output_file, self.result_dir)
        else:
            print("All batches available with equal file counts. Using standard merge approach.")
            # Use original approach for small datasets
            file_path = available_batches[0] / "output" / "run_status.nc"
            if file_path.exists():
                x, y = get_dimensions(file_path.as_posix())
                total_cell_count = x * y * len(available_batches)

                if total_cell_count < self.__MIN_CELL_COUNT_FOR_DASK:
                    for output_file in output_files:
                        self._merge_small_dataset(output_file, self.result_dir)
                else:
                    if self._args.bucket_path == "":
                        raise ValueError(
                            f"--bucket-path is required for this dataset which has {total_cell_count}"
                        )

                    fs = get_gcsfs()
                    bucket_name = f"{Path(self._args.bucket_path).parts[0]}/"
                    if bucket_name not in fs.buckets:
                        raise ValueError(
                            f"There is no bucket named {bucket_name}"
                        )

                    self._merge_with_dask(self._args.bucket_path)

        # Print average cell run time
        run_status_file = self.result_dir / "run_status.nc"
        if run_status_file.exists():
            try:
                ds = xr.open_dataset(run_status_file.as_posix(), engine="h5netcdf")
                # Filter out fill values and get valid runtime values
                total_runtime_values = ds.total_runtime.values.flatten()
                
                # Check if data is stored as timedelta64 or numeric
                if np.issubdtype(total_runtime_values.dtype, np.timedelta64):
                    # Handle timedelta64 data - filter out NaT and negative values
                    valid_mask = ~np.isnat(total_runtime_values)
                    valid_runtimes = total_runtime_values[valid_mask]
                    
                    if len(valid_runtimes) > 0:
                        # Convert to seconds 
                        runtimes_in_seconds = valid_runtimes / np.timedelta64(1, 's')
                        # Filter out negative and zero values (invalid runtimes)
                        positive_mask = runtimes_in_seconds > 0
                        runtimes_in_seconds = runtimes_in_seconds[positive_mask]
                    else:
                        runtimes_in_seconds = []
                else:
                    # Handle numeric data (int or float)
                    # Filter out NaN, negative values, and specific fill values
                    valid_mask = (~np.isnan(total_runtime_values)) & (total_runtime_values > 0) & (total_runtime_values != -9999)
                    valid_runtimes = total_runtime_values[valid_mask]
                    runtimes_in_seconds = valid_runtimes.astype(float)
                
                if len(runtimes_in_seconds) > 0:
                    average_cell_runtime = np.mean(runtimes_in_seconds)
                    print(f"The average cell run time is {average_cell_runtime} seconds ({round(average_cell_runtime / 60, 2)} min)")
                    print(f"Calculated from {len(runtimes_in_seconds)} valid runtime values out of {len(total_runtime_values)} total cells")
                else:
                    print("No valid runtime data found. All values are fill values.")
                ds.close()
            except Exception as e:
                print(f"Could not calculate average runtime: {e}")
        else:
            print(f"Couldn't find {run_status_file.as_posix()}. Skipping runtime calculation.")
