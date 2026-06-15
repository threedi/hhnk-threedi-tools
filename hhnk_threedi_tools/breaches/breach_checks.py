# %%
import shutil
from pathlib import Path

import geopandas as gpd
import threedigrid_builder.application as app

sqlite_path = Path(r"H:\02.modellen\midden_zuid_pytest\work in progress\schematisation\midden_zuid_pytest.gpkg")
dem_path = Path(
    r"H:\02.modellen\midden_zuid_pytest\work in progress\schematisation\rasters\dem_schemer_midden_zuid_fix_compressed_v2.tif"
)
out_path = Path(
    r"H:\02.modellen\midden_zuid_pytest\work in progress\schematisation\gridadmin_midden_zuid_pytest_v2.gpkg"
)
convert_to_geopackage = True
tmp_folder = sqlite_path.parent / "tmp"
tmp_folder.mkdir(exist_ok=True)

temp_sqlite_path = sqlite_path.parent / "tmp" / (sqlite_path.stem + "_copy.sqlite")

shutil.copy(sqlite_path, temp_sqlite_path)

app.make_gridadmin(
    sqlite_path=temp_sqlite_path,
    dem_path=dem_path,
    out_path=out_path,
    meta=None,
    progress_callback=None,
    upgrade=True,
    convert_to_geopackage=convert_to_geopackage,
)
shutil.rmtree(tmp_folder)

grid_read_nodes = gpd.read_file(out_path, layer="nodes")