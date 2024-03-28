import os
import logging

from hhnk_threedi_tools.git_model_repo.utils.restore_xlsx import ExcelRestore
from hhnk_threedi_tools.git_model_repo.utils.restore_gpkg import GeoPackageRestore
from utils.rreplace import rrplace

log = logging.getLogger(__name__)


def get_file_name_from_path(path):
    """Get the file name from a path.

    :param path: path to a file
    :return: file name
    """
    filename = rrplace(os.path.basename(path), "_", ".", 1)
    return os.path.join(os.path.dirname(path), os.pardir, filename)


def restore_file_in_directory(directory, output_file_path=None):
    """Restore all files, previously dumped to original files.

    :param directory: path to the directory containing the dumped files
    :param output_file_path: path to the output file. If None, the output file will be stored in the same
                             directory as the parent of the input directory.
                             This parameter is especially usefull for testing
    """
    out_file_path = None

    # loop recursively over all files in the directory
    for root, dirs, files in os.walk(directory):
        for rel_path in dirs:
            path = os.path.join(root, rel_path)
            if os.path.isdir(path):
                if path.endswith("_gpkg"):
                    if output_file_path is None:
                        out_file_path = get_file_name_from_path(path)
                    restorer = GeoPackageRestore(path, out_file_path)
                    restorer.restore()
                elif path.endswith("_xlsx"):
                    if output_file_path is None:
                        out_file_path = get_file_name_from_path(path)
                    restorer = ExcelRestore(path, out_file_path)
                    restorer.restore()
