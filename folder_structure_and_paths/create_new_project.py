import os
from .build_folder_structure_dict import build_folder_structure_dict

def create_new_project_folder(base_path):
    try:
        dict = build_folder_structure_dict(polder_path=base_path)
        for item in dict.values():
            os.makedirs(item, exist_ok=True)
        expected_source_files = "Expected files are:\n\n" \
                                "Damo geodatabase (*.gdb) named 'DAMO.gdb'\n" \
                                "Datachecker geodatabase (*.gdb) named 'datachecker_output.gdb'\n" \
                                "Hdb geodatabase (*.gdb) named 'HDB.gdb'\n" \
                                "Folder named 'modelbuilder_output' and polder shapefile " \
                                "(*.shp and associated file formats)"
        with open(os.path.join(dict['source_data_folder'], 'read_me.txt'), mode='w') as f:
            f.write(expected_source_files)
        expected_model_files = "Expected files are:\n\n" \
                               "Sqlite database (model): *.sqlite\n" \
                               "Folder named 'rasters' containing DEM raster (*.tif) and other rasters\n"
        with open(os.path.join(dict['model_folder'], 'read_me.txt'), mode='w') as f:
            f.write(expected_model_files)
        expected_threedi_files = "Expected files are:\n\n" \
                                 "Both sub folders in this folder expect to contain folders corresponding to " \
                                 "3di results from different revisions (e.g. containing *.nc, *.h5 and *.sqlite file)"
        with open(os.path.join(dict['threedi_results_folder'], 'read_me.txt'), mode='w') as f:
            f.write(expected_threedi_files)
        output_folder_explanation = "This folder is the default folder where the HHNK toolbox " \
                                    "saves results of tests. The inner structure of these result folders " \
                                    "is automatically generated"
        with open(os.path.join(dict['output_folder'], 'read_me.txt'), mode='w') as f:
            f.write(output_folder_explanation)
    except Exception as e:
        raise e from None
