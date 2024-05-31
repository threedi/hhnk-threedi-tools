import os
import logging

from hhnk_threedi_tools.git_model_repo.utils.restore_xlsx import ExcelRestore
from hhnk_threedi_tools.git_model_repo.utils.restore_gpkg import GeoPackageRestore
from hhnk_threedi_tools.git_model_repo.utils.rreplace import rreplace

log = logging.getLogger(__name__)


def get_file_names_from_path(path):
    """Get the file name from a path.

    :param path: path to a file
    :return: file name
    """
    tmp_filename = rreplace(os.path.basename(path), "_", "~.", 1).replace(".", "", 1)
    orig_filename = rreplace(tmp_filename, "~.", ".", 1)
    backup_filename = rreplace(tmp_filename, "~.", "_backup.", 1)
    return (
        os.path.join(os.path.dirname(path), tmp_filename),
        os.path.join(os.path.dirname(path), orig_filename),
        os.path.join(os.path.dirname(path), backup_filename),
    )


def restore_files_in_directory(directory, output_file_path=None):
    """Restore all files, previously dumped to original files.

    :param directory: path to the directory containing the dumped files
    :param output_file_path: path to the output file. If None, the output file will be stored in the same
                             directory as the parent of the input directory.
                             This parameter is especially usefull for testing
    """

    # loop recursively over all files in the directory
    for root, dirs, files in os.walk(directory):
        # if endswith _gpkg or _xlsx and is empty, remove the directory
        if len(files) == 0 and (root.endswith("_gpkg") or root.endswith("_xlsx")):
            log.info("removing empty directory %s", root)
            os.rmdir(root)

        for file_name in files:
            # if ends on _backup.xlsx or _backup.gpkg, remove the file
            if file_name.endswith("_backup.xlsx") or file_name.endswith("_backup.gpkg"):
                log.info("removing backup file %s", os.path.join(root, file_name))
                os.remove(os.path.join(root, file_name))

        for rel_path in dirs:
            path = os.path.join(root, rel_path)

            if os.path.isdir(path):
                if rel_path.startswith('.') and rel_path.endswith("_gpkg"):

                    if output_file_path is None:
                        tmp_file_path, orig_file_path, backup_file_path = get_file_names_from_path(path)

                        log.info("restoring geopackage %s", orig_file_path)

                        log.debug("first restore geopackage from geojson %s", tmp_file_path)
                        # restore the geopackage
                        restorer = GeoPackageRestore(path, tmp_file_path)
                        restorer.restore()

                        # move the restored file to original location and the original (LFS) file to backup location
                        if os.path.exists(backup_file_path):
                            os.remove(backup_file_path)
                        log.debug("moving %s to %s", orig_file_path, backup_file_path)
                        os.rename(orig_file_path, backup_file_path)
                        log.debug("moving %s to %s", tmp_file_path, orig_file_path)
                        os.rename(tmp_file_path, orig_file_path)

                    else:
                        log.info("restoring geopackage %s", output_file_path)
                        # restore the geopackage
                        restorer = GeoPackageRestore(path, output_file_path)
                        restorer.restore()

                elif rel_path.startswith('.') and rel_path.endswith("_xlsx"):
                    if output_file_path is None:
                        tmp_file_path, orig_file_path, backup_file_path = get_file_names_from_path(path)

                        log.info("restoring excel %s", orig_file_path)

                        log.debug("first restore excel from json %s", tmp_file_path)
                        # restore the excel
                        restorer = ExcelRestore(path, tmp_file_path)
                        restorer.restore()

                        # move the restored file to original location and the original (LFS) file to backup location
                        if os.path.exists(backup_file_path):
                            os.remove(backup_file_path)
                        log.debug("moving %s to %s", orig_file_path, backup_file_path)
                        os.rename(orig_file_path, backup_file_path)
                        log.debug("moving %s to %s", tmp_file_path, orig_file_path)
                        os.rename(tmp_file_path, orig_file_path)

                    else:
                        log.info("restoring excel %s", output_file_path)
                        restorer = ExcelRestore(path, output_file_path)
                        restorer.restore()
