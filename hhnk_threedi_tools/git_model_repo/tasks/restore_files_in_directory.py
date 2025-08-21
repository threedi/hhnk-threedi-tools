import logging
import typing
from pathlib import Path

from hhnk_threedi_tools.git_model_repo.utils.restore_gpkg import GeoPackageRestore
from hhnk_threedi_tools.git_model_repo.utils.restore_xlsx import ExcelRestore
from hhnk_threedi_tools.git_model_repo.utils.rreplace import rreplace

log = logging.getLogger(__name__)


def get_file_names_from_path(path: typing.Union[Path | str]) -> tuple[Path, Path, Path]:
    """
    Get the temporary, original, and backup file names based on a given path.

    Parameters
    ----------
    path : Path or str
        Path to a file.

    Returns
    -------
    tuple of Path
        A tuple containing:
        - tmp_filename: Path to the temporary file.
        - orig_filename: Path to the original file.
        - backup_filename: Path to the backup file.

    """
    tmp_filename = rreplace(Path(path).name, "_", "~.", 1).replace(".", "", 1)
    orig_filename = rreplace(tmp_filename, "~.", ".", 1)
    backup_filename = rreplace(tmp_filename, "~.", "_backup.", 1)

    p = Path(path)
    return (p.parent / tmp_filename, p.parent / orig_filename, p.parent / backup_filename)


def restore_files_in_directory(
    directory: typing.Union[str, Path], output_file_path: typing.Optional[typing.Union[str, Path]] = None
):
    """Restore all files previously dumped to original files in a directory.

    The function recursively walks through the directory, restoring files for
    GeoPackage and Excel formats. It removes empty directories and backup files,
    and restores files using the appropriate restore class.

    Parameters
    ----------
    directory : str or Path
        Path to the directory containing the dumped files.
    output_file_path : str or Path, optional
        Path to the output file. If None, the output file will be stored in the same
        directory as the parent of the input directory. This parameter is especially
        useful for testing.

    Returns
    -------
    None
    """
    directory = Path(directory)

    # loop recursively over all files in the directory
    for root, dirs, files in directory.walk():
        root: Path = Path(root)
        # if endswith _gpkg or _xlsx and is empty, remove the directory
        if len(files) == 0 and (str(root).endswith("_gpkg") or str(root).endswith("_xlsx")):
            log.info("removing empty directory %s", root)
            root.rmdir()

        for file_name in files:
            # if ends on _backup.xlsx or _backup.gpkg, remove the file
            if file_name.endswith("_backup.xlsx") or file_name.endswith("_backup.gpkg"):
                fn = root / file_name
                log.info("removing backup file %s", fn)
                fn.unlink()

        for rel_path in dirs:
            path = root / rel_path

            if path.is_dir():
                if rel_path.startswith(".") and rel_path.endswith("_gpkg"):
                    if output_file_path is None:
                        tmp_file_path, orig_file_path, backup_file_path = get_file_names_from_path(path)

                        log.info("restoring geopackage %s", orig_file_path)

                        log.debug("first restore geopackage from geojson %s", tmp_file_path)
                        # restore the geopackage
                        restorer = GeoPackageRestore(path, tmp_file_path)
                        restorer.restore()

                        # move the restored file to original location and the original (LFS) file to backup location
                        if backup_file_path.exists():
                            backup_file_path.unlink()
                        log.debug("moving %s to %s", orig_file_path, backup_file_path)
                        orig_file_path.rename(backup_file_path)
                        log.debug("moving %s to %s", tmp_file_path, orig_file_path)
                        tmp_file_path.rename(orig_file_path)

                    else:
                        log.info("restoring geopackage %s", output_file_path)
                        # restore the geopackage
                        restorer = GeoPackageRestore(path, output_file_path)
                        restorer.restore()

                elif rel_path.startswith(".") and rel_path.endswith("_xlsx"):
                    if output_file_path is None:
                        tmp_file_path, orig_file_path, backup_file_path = get_file_names_from_path(path)

                        log.info("restoring excel %s", orig_file_path)

                        log.debug("first restore excel from json %s", tmp_file_path)
                        # restore the Excel
                        restorer = ExcelRestore(path, tmp_file_path)
                        restorer.restore()

                        # move the restored file to original location and the original (LFS) file to backup location
                        if backup_file_path.exists():
                            backup_file_path.unlink()
                        log.debug("moving %s to %s", orig_file_path, backup_file_path)
                        orig_file_path.rename(backup_file_path)
                        log.debug("moving %s to %s", tmp_file_path, orig_file_path)
                        tmp_file_path.rename(orig_file_path)

                    else:
                        log.info("restoring excel %s", output_file_path)
                        restorer = ExcelRestore(path, output_file_path)
                        restorer.restore()
