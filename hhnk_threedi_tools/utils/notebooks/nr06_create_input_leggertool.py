# %%

import shutil
from pathlib import Path

import hhnk_research_tools as hrt
import numpy as np
import pandas as pd

import hhnk_threedi_tools as htt
import hhnk_threedi_tools.core.checks.grid_result_metadata as grid_result_metadata
from hhnk_threedi_tools.core.folders import Folders


# %%
def create_input_leggertool(folder: Folders, berekening_naam: str, output_file: Path):
    """Prepare legger input gpkg. Also adds a style file.

    Parameters
    ----------
    folder : htt.Folders
        Project class
    berekening_naam : str
        name of simulation in the threediresults 0d1d subfolder
    output_file : Path
        filepath where gpkg should be stored
    """
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

    structure_all.to_file(output_file, driver="GPKG", layer="afvoer_debiet")

    # Kopieer standaard opmaak naar dezelfde locatie
    style_src_path = hrt.get_pkg_resource_path(
        package_resource=htt.resources.qgis_layer_styles.zero_d_one_d, name="discharge_alt_qabs_with_arrows.qml"
    )
    style_dest_path = output_file.with_suffix(".qml")

    if not style_dest_path.exists():
        shutil.copy(
            src=style_src_path,
            dst=style_dest_path,
        )

    return structure_all


# %%
if __name__ == "__main__":
    # %% Op een specifieke map
    folder_dir = Path(r"E:\02.modellen\VNK_leggertool")

    # %% of in de local folder
    from notebook_setup import setup_notebook

    notebook_data = setup_notebook()
    folder_dir = Path(notebook_data["polder_folder"])
    # %%

    # Input
    berekening_naam = (
        "vnk_leggertool #11 0d1d_test leggertool"  # Deze moet in map 03_3di_resultaten\0d1d_results staan
    )
    folder = Folders(folder_dir)
    output_file = folder.joinpath(f"debiet_{berekening_naam}_{hrt.get_uuid()}.gpkg")

    create_input_leggertool(
        folder=folder,
        berekening_naam=berekening_naam,
        output_file=output_file,
    )
