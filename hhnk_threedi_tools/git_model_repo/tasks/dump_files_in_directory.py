import logging
from pathlib import Path
from typing import List, Optional

from git import Repo

from hhnk_threedi_tools.git_model_repo.utils.dump_gpkg import GeoPackageDump
from hhnk_threedi_tools.git_model_repo.utils.dump_xlsx import ExcelDump
from hhnk_threedi_tools.git_model_repo.utils.file_change_detection import is_file_git_modified
from hhnk_threedi_tools.git_model_repo.utils.rreplace import rreplace

logger = logging.getLogger(__name__)


def dump_files_in_directory(
    directory: Path,
    root_repo: Optional[Path] = None,
    output_path: Optional[Path] = None,
    excel_dump: bool = True,
    gpkg_dump: bool = True,
) -> List[Path]:
    """Dump all files in a directory to a JSON file, checking gitignore and file extension.

    The function recursively walks through the directory, processes files based on extension,
    and uses the appropriate dumper class for Excel and GeoPackage files. Skips files ignored
    by git and files that are not modified.

    Parameters
    ----------
    directory : Path
        Path to the directory with files (including subdirectories) to dump.
    root_repo : Path, optional
        Path to the root of the git repository. If None, the root will be searched.
    output_path : Path, optional
        Path to the output directory. If None, output will be stored in the same directory.
        Used for testing.
    excel_dump : bool, optional
        Whether to dump Excel files. Default is True.
    gpkg_dump : bool, optional
        Whether to dump GeoPackage files. Default is True.

    Returns
    -------
    List[Path]
        List of paths to changed files.
    """
    changed_files: List[Path] = []

    if root_repo is None:
        root_repo = directory

    repo = Repo(str(root_repo))

    for root, dirs, files in directory.walk(top_down=True):
        # Remove ignored directories
        dirs[:] = [d for d in dirs if not repo.ignored(d) and d != ".git"]

        for file_name in files:
            file_path = root / file_name
            rel_file_path = file_path.relative_to(directory)
            logger.debug("Checking file '%s'", rel_file_path)
            # Only process files starting with "02"
            if not str(rel_file_path).startswith("02"):
                continue

            actual_file_path = file_path
            if output_path:
                actual_file_path = output_path / rel_file_path

            if actual_file_path.is_file():
                if repo.ignored(str(actual_file_path)):
                    logger.info("Skipping ignored file '%s'", rel_file_path)
                    continue

                if not is_file_git_modified(repo, str(rel_file_path)):
                    logger.info("Skip not modified file '%s'", rel_file_path)
                    continue

                if file_name.endswith(".gpkg") and gpkg_dump:
                    out_dir = actual_file_path.parent / rreplace(file_name, ".", "_", 1)
                    logger.info("Dump geopackage '%s'", actual_file_path)
                    dumper = GeoPackageDump(actual_file_path, out_dir)
                    dumper.dump_schema()
                    dumper.dump_layers()
                    logger.info(
                        "Dumped geopackage file '%s', %i changed files", rel_file_path, len(dumper.changed_files)
                    )
                    changed_files.extend([Path(f) for f in dumper.changed_files])
                elif file_name.endswith(".xlsx") and excel_dump:
                    out_dir = actual_file_path.parent / rreplace(file_name, ".", "_", 1)
                    dumper = ExcelDump(actual_file_path, out_dir)
                    dumper.dump_schema()
                    dumper.dump_sheets()
                    logger.info("Dumped excel file '%s', %i changed files", rel_file_path, len(dumper.changed_files))

                    changed_files.extend([f for f in dumper.changed_files])
                else:
                    logger.debug("Skipping file '%s'", rel_file_path)

    return changed_files


# todo: remove. This was for development
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_dir = Path(__file__).parent.parent.parent.parent / "callantsoog-test"
    dump_files_in_directory(test_dir)
