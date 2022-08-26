# %%
if __name__ == "__main__":
    import sys

    sys.path.insert(0, r"E:\github\wvangerwen\hhnk-threedi-tools")
    sys.path.insert(0, r"E:\github\wvangerwen\hhnk-research-tools")

try:
    rm_paths = [
        "C:/Users/wvangerwen/AppData/Roaming/QGIS/QGIS3/profiles/default/python/plugins/hhnk_threedi_plugin/external-dependencies",
        "C:/Users/wvangerwen/AppData/Roaming/QGIS/QGIS3/profiles/default/python",
    ]
    for rm in rm_paths:
        sys.path.remove(rm)
except:
    pass

import os
import hhnk_threedi_tools as htt
from threedidepth.calculate import calculate_waterdepth

# from hhnk_threedi_tools.core.climate_scenarios.interpolate_levels import edit_nodes_in_grid

folder_path = r"E:\02.modellen\model_test_v2"
folder = htt.Folders(folder_path)


# # local imports

# # 1D replacement find the nearest 1d cell and pushes it on the selected cell.
# replaced_data = edit_nodes_in_grid(netcdf_path,waterdeel,grid,"1d_replacement",output_path)


# # Recalculate the associated depth with:
# timestep = 576
# calculate_depth(folder_path=folder_path, results_type="1d2d_results", revision='', calculation_step=576)


results_type = "1d2d_results"
revision = "BWN bwn_test #6 1d2d_test"
calculation_step = 57

folder = htt.Folders(folder_path)
result = folder.threedi_results[results_type][revision]
dem_path = folder.model.schema_base.rasters.dem.path

output_folder = folder.output[results_type][revision]
output_folder.create()
output_folder.add_file(f"depth", f"depth_{calculation_step}.tif")

overwrite = True
cont = True

if output_folder.depth.exists:
    if overwrite:
        output_folder.depth.pl.unlink()
    else:
        cont = False

if cont:
    print("calc depth")
    calculate_waterdepth(
        gridadmin_path=result.admin_path.path,
        results_3di_path=result.grid_path.path,
        dem_path=dem_path,
        waterdepth_path=output_folder.depth.path,
        calculation_steps=range(0, 10),
    )

# %%
