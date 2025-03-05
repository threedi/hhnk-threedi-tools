import logging
import os

from git import Repo

from hhnk_threedi_tools.git_model_repo.utils.dump_gpkg import GeoPackageDump
from hhnk_threedi_tools.git_model_repo.utils.dump_xlsx import ExcelDump
from hhnk_threedi_tools.git_model_repo.utils.file_change_detection import is_file_git_modified
from hhnk_threedi_tools.git_model_repo.utils.rreplace import rreplace

log = logging.getLogger(__name__)


def dump_files_in_directory(
    directory: str, root_repo: str = None, output_path: str = None, excel_dump: bool = True, gpkg_dump: bool = True
) -> [str]:
    """Dump all files in a directory to a json file. Checks on gitignore and file extension.

    Args:
        directory (str): Path to the directory with files (also in subdirs) to dump.
        root_repo (str, optional): Path to the root of the git repo. Defaults to None (root will be searched).
        output_path (str, optional): Path to the output directory. Defaults to None. used for testing.
        excel_dump (bool, optional): Dump Excel files. Defaults to True.
        gpkg_dump (bool, optional): Dump geopackage files. Defaults to True.

    """
    changed_files = []

    out_dir = None
    if root_repo is None:
        root_repo = directory

    repo = Repo(root_repo)

    # loop recursively over all files in the directory
    for root, dirs, files in os.walk(directory, topdown=True):
        # remove ignored directories
        dirs[:] = [d for d in dirs if not repo.ignored(d) and d != ".git"]

        for file_name in files:
            file_path = os.path.join(root, file_name)
            # relative path for printing
            rel_file_path = os.path.relpath(file_path, directory)
            log.debug("Checking file '%s'", rel_file_path)
            # currently only model schematisation
            if rel_file_path[:2] not in ["02"]:
                continue

            if output_path:
                file_path = os.path.join(output_path, rel_file_path)
            if os.path.isfile(file_path):
                if repo.ignored(file_path):
                    log.info("Skipping ignored file '%s'", rel_file_path)
                    continue

                if not is_file_git_modified(repo, file_path):
                    log.info("Skip not modified file '%s'", rel_file_path)
                    continue

                if file_name.endswith(".gpkg") and gpkg_dump:
                    out_dir = os.path.join(os.path.dirname(file_path), rreplace(file_name, ".", "_", 1))
                    log.info("dump geopackage '%s'", file_path)
                    dumper = GeoPackageDump(file_path, out_dir)
                    dumper.dump_schema()
                    dumper.dump_layers()
                    log.info("Dumped geopackage file '%s', %i changed files", rel_file_path, len(dumper.changed_files))
                    changed_files.extend(dumper.changed_files)
                elif file_name.endswith(".xlsx") and excel_dump:
                    out_dir = os.path.join(os.path.dirname(file_path), rreplace(file_name, ".", "_", 1))
                    dumper = ExcelDump(file_path, out_dir)
                    dumper.dump_schema()
                    dumper.dump_sheets()
                    log.info("Dumped excel file '%s', %i changed files", rel_file_path, len(dumper.changed_files))
                    changed_files.extend(dumper.changed_files)
                else:
                    log.debug("Skipping file '%s'", rel_file_path)

    return changed_files


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    dump_files_in_directory(os.path.join(os.path.dirname(__file__), "../../../../callantsoog-test/"))
