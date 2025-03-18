import os
from pathlib import Path
from collections import defaultdict
from typing import Tuple, Dict

from batch_processing.cmd.base import BaseCommand
from batch_processing.utils.utils import get_batch_number, get_batch_folders


class BatchCheckCommand(BaseCommand):
    def __init__(self, args):
        super().__init__()
        self._args = args
        self.base_batch_dir = Path(self.exacloud_user_dir, args.batches)

    def _check_equal_output_files(self, path: Path) -> Tuple[bool, Dict[int, int]]:
        """
        Check if all batch folders have the same number of output files in their output/ subfolder.
        
        Args:
            path (Path): Path to the directory containing batch folders
            
        Returns:
            Tuple[bool, Dict[int, int]]: 
                - Boolean indicating if all batch folders have the same number of output files
                - Dictionary mapping batch numbers to their file counts
        """
        batch_folders = get_batch_folders(path)

        batch_file_counts = {}

        for folder in batch_folders:
            batch_num = get_batch_number(folder)
            output_dir = folder / "output"

            if not output_dir.exists() or not output_dir.is_dir():
                batch_file_counts[batch_num] = 0
                continue

            file_count = sum(1 for entry in os.scandir(output_dir) if entry.is_file())
            batch_file_counts[batch_num] = file_count

        file_count_values = list(batch_file_counts.values())
        all_equal = all(count == file_count_values[0] for count in file_count_values)

        return all_equal, batch_file_counts

    def _diagnose_output_files(self, counts: Tuple[bool, Dict[int, int]]) -> None:
        # Group batches by file count for more concise reporting
        count_to_batches = defaultdict(list)
        for batch_num, file_count in counts.items():
            count_to_batches[file_count].append(batch_num)

        max_files = max(counts.values())
        max_count_batches = count_to_batches[max_files]

        print(f"Batch folders have different numbers of output files:")

        # Print batches with the maximum (expected) file count on a single line
        batch_nums_str = ", ".join(f"batch_{b}" for b in sorted(max_count_batches))
        print(f"- {len(max_count_batches)} batches with {max_files} files: {batch_nums_str}")

        # Print other counts (abnormal ones)
        for file_count, batch_nums in sorted(count_to_batches.items()):
            if file_count != max_files:
                batch_nums_str = ", ".join(f"batch_{b}" for b in sorted(batch_nums))
                print(f"- {len(batch_nums)} batches with {file_count} files ({max_files - file_count} missing): {batch_nums_str}")

    def execute(self):
        print("Checking to see if every batch folder has equal number of output files...")
        equal, counts = self._check_equal_output_files(self.base_batch_dir)
        if not counts:
            print("No batch folders found. Aborting.")
            return

        if equal:
            file_count = next(iter(counts.values()), 0)
            print(f"All {len(counts)} batch folders have {file_count} output files.")
            print("The check is passed!")
            return

        self._diagnose_output_files(self.base_batch_dir)
