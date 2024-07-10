import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock, call

from batch_processing.cmd.init import InitCommand
from batch_processing.utils.utils import download_directory


@pytest.fixture
def init_command():
    args = MagicMock()
    command = InitCommand(args)
    command.user = "testuser"
    command.exacloud_user_dir = Path("/fake/dir/exacloud_user")
    command.slurm_log_dir = Path("/fake/dir/slurm_log")
    command.output_dir = Path("/fake/dir/output")
    return command


def test_init_command_as_root(init_command):
    init_command.user = "root"
    
    with pytest.raises(ValueError):
        init_command.execute()


@pytest.fixture
def mock_download_directory():
    with patch('google.cloud.storage.Client') as mock_client, \
         patch('google.cloud.storage.Blob') as mock_blob, \
         patch('pathlib.Path.mkdir') as mock_mkdir:
        
        # Set up the mock client
        mock_bucket = mock_client.return_value.get_bucket.return_value
        
        # Create mock blobs
        mock_blob1 = mock_blob.return_value
        mock_blob1.name = "test-directory/file1.txt"
        mock_blob2 = mock_blob.return_value
        mock_blob2.name = "test-directory/subdirectory/"
        mock_blob3 = mock_blob.return_value
        mock_blob3.name = "test-directory/subdirectory/file2.txt"
        
        # Set up the mock blobs
        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3]
        
        yield mock_client, mock_blob, mock_mkdir


def test_execute(init_command, mock_download_directory):
    mock_client, mock_blob, mock_mkdir = mock_download_directory
    
    init_command.execute()
    
    mock_client.assert_called_once()
    mock_client.return_value.get_bucket.assert_called_once_with("gcp-slurm")
    mock_client.return_value.get_bucket.return_value.list_blobs.assert_called_once_with(prefix="dvm-dos-tem/")
    
    # mock_mkdir.assert_any_call(exist_ok=True)
    # assert mock_mkdir.call_count == 3
    
    mock_blob.return_value.download_to_filename.assert_any_call('/tmp/test-output/test-directory/file1.txt')
    mock_blob.return_value.download_to_filename.assert_any_call('/tmp/test-output/test-directory/subdirectory/file2.txt')
    assert mock_blob.return_value.download_to_filename.call_count == 2
