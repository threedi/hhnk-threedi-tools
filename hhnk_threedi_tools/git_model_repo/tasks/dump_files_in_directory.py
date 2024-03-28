import logging
import os

from git import Repo

from utils.check_if_file_is_ignored import is_file_gitignored
from utils.dump_gpkg import GeoPackageDump
from utils.dump_xlsx import ExcelDump

log = logging.getLogger(__name__)


def dump_files_in_directory(directory: str, root_repo: str = None,
                            output_path: str = None, excel_dump: bool = True, gpkg_dump: bool = True):
    """Dump all files in a directory to a json file. Checks on gitignore and file extension.

    Args:
        directory (str): Path to the directory with files (also in subdirs) to dump.
        root_repo (str, optional): Path to the root of the git repo. Defaults to None (root will be searched).
        output_path (str, optional): Path to the output directory. Defaults to None. used for testing.
        excel_dump (bool, optional): Dump Excel files. Defaults to True.
        gpkg_dump (bool, optional): Dump geopackage files. Defaults to True.
    """
    out_dir = None
    if root_repo is None:
        root_repo = directory

    repo = Repo(root_repo)

    # loop recursively over all files in the directory
    for root, dirs, files in os.walk(directory):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            if os.path.isfile(file_path):
                if is_file_gitignored(file_path, repo):
                    log.debug(f"Skipping ignored file {file_name} in directory {directory}")
                    continue

                if output_path:
                    out_dir = os.path.join(output_path, file_name.replace(".", "_"))
                if file_name.endswith(".gpkg") and gpkg_dump:
                    dumper = GeoPackageDump(file_path, out_dir)
                    dumper.dump_schema()
                    dumper.dump_layers()
                    log.debug(f"Dumped file {file_name} in directory {directory}")
                elif file_name.endswith(".xlsx") and excel_dump:
                    dumper = ExcelDump(file_path, out_dir)
                    dumper.dump_schema()
                    dumper.dump_sheets()
                    log.debug(f"Dumped file {file_name} in directory {directory}")
                else:
                    log.debug(f"Skipping file {file_name} in directory {directory}")
