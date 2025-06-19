# %%

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd

import hhnk_threedi_tools as htt
import hhnk_threedi_tools.core.checks.grid_result_metadata as grid_result_metadata
from hhnk_threedi_tools.core.folders import Folders

# %%

# from threedigrid.admin.gridresultadmin import GridH5ResultAdmin
# GridH5ResultAdmin(self.admin_path.file_path, self.grid_path.file_path)

# Input
folder_path = r"\\corp.hhnk.nl\data\Hydrologen_data\Data\02.modellen\Geestmerambacht_leggertool"
berekening_naam = (
    "Geestmerambacht_leggertool #9 0d1d_test leggertool"  # Deze moet in map 03_3di_resultaten\0d1d_results staan
)


# %%


folder = Folders(folder_path)
threedi_result = folder.threedi_results.zero_d_one_d[berekening_naam].grid


structure_all = pd.DataFrame()

for structure_name in ["channels", "culverts", "orifices", "weirs"]:
    structure = pd.DataFrame()

    structure_lines = getattr(threedi_result.lines, structure_name)

    # Bepalen tijdstappen voor laatste dag neerslag
    (
        rain,
        detected_rain,
        timestep,
        days_dry_start,
        days_dry_end,
        df,
    ) = grid_result_metadata.construct_scenario(threedi_result)

    t_end_rain_min_one = df["t_end_rain_min_one"].value
    t_end_rain = df["t_end_rain"].value

    structure["q_mean_m3_s"] = np.mean(
        structure_lines.timeseries(indexes=slice(t_end_rain_min_one, t_end_rain)).q, 0
    )  # m3/s

    # Add geometry
    lines_geometry = hrt.threedi.line_geometries_to_coords(structure_lines.line_geometries)
    structure = hrt.df_add_geometry_to_gdf(df=structure, geometry_col=lines_geometry)

    # Add additional cols
    structure["map_id"] = structure_lines.content_pk.tolist()
    structure["node_up"] = structure_lines.line_nodes[:, 0]
    structure["node_down"] = structure_lines.line_nodes[:, 1]  # Downstream id van de connection nodes
    structure["type"] = structure_name

    # Absolute waarden meenemen
    structure["richting"] = structure["q_mean_m3_s"].apply(lambda x: -1 if x < 0 else 1)
    structure["qabs"] = structure["q_mean_m3_s"].abs()  # Absoluut debiet, richting staat in structure['richting']

    # Geometry als laatste kolom
    structure["geometry"] = structure.pop("geometry")

    # Samenvoegen structures (channel, culvert, orifice)
    structure_all = pd.concat([structure_all, structure])


structure_all.to_file(f"{folder_path}\\debiet_{berekening_naam}.gpkg", driver="GPKG", layer="afvoer_debiet")
structure_all.to_file(f"{folder_path}\\debiet_{berekening_naam}.shp", driver="ESRI Shapefile", layer="afvoer_debiet")
