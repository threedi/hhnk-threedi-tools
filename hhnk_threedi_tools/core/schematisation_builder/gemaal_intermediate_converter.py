# %%
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional, Tuple

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
import pyogrio

import hhnk_threedi_tools as htt
from hhnk_threedi_tools.core.folders import Folders

# %%


class PompIntermediateConverter:
    """
    Intermediate converter for pomp data.
    From CSO format to DAMO/intermediate format, ready for converting to HyDAMO.
    Add column "distance_validation_rule" to the gemaal layer.
    Parameters
    ----------
    damo_file_path : Path
        Path to the DAMO geopackage file.
    poler_polygon :  Path
        Path to the poler_polygon
    logger : logging.Logger, optional
        Logger for logging messages. If not provided, a default logger will be used.
    """

    def __init__(self, damo_file_path: Path, logger=None):
        self.damo_file_path = Path(damo_file_path)

        if logger:
            self.logger = logger
        else:
            self.logger = hrt.logging.get_logger(__name__)

        self.pomp: Optional[gpd.GeoDataFrame] = None
        self.gemaal: Optional[gpd.GeoDataFrame] = None
        self.combinatiepeilgebied: Optional[gpd.GeoDataFrame] = None
        self.polder_polygon: Optional[gpd.GeoDataFrame] = None
        self.peilafwijking: Optional[gpd.GeoDataFrame] = None
        self.peilgebiedpraktijk: Optional[gpd.GeoDataFrame] = None

        self.logger.info(f"Initialized GemaalIntermediateConverter with DAMO file: {self.damo_file_path}")

    def load_layers(self):
        """Load the necessary pomp layer from the DAMO geopackage file and DAMO schema."""
        self.logger.info("Loading pomp layer from DAMO file...")
        self.pomp = gpd.read_file(self.damo_file_path, layer="POMP")
        # self.logger(self.pomp.head())

        self.gemaal = gpd.read_file(self.damo_file_path, layer="GEMAAL")
        self.logger.info("Pomp layer loaded successfully.")

        self.combinatiepeilgebied = gpd.read_file(self.damo_file_path, layer="COMBINATIEPEILGEBIED")
        self.logger.info("Combinatiepeilgebied layer loaded successfully.")

        self.polder_polygon = gpd.read_file(self.damo_file_path.parent / "polder_polygon.shp")
        self.logger.info("Poler polygon layer loaded successfully.")

    def add_column_gemaalid(self):
        """Add gemaalid column to the pomp layer."""
        self.logger.info("Link pomp to gemaal and pick global_id as to define gemaalid in pomp layer")
        # Check if gemaal and pomp layer are loaded
        if self.gemaal is None:
            self.logger.error("Gemaal layer is not loaded. Please load the gemaal layer first.")
            return
        if self.pomp is None:
            self.logger.error("Pomp layer is not loaded. Please load the pomp layer first.")
            return

        # Merge pomp with gemaal based on CODEBEHEEROBJECT (pomp) and code (gemaal)
        merged = self.pomp.merge(
            self.gemaal[["code", "globalid"]], left_on="codebeheerobject", right_on="code", how="left"
        )
        # Assign the globalid from gemaal as gemaalid in pomp
        self.pomp["gemaalid"] = merged["globalid"]
        # TODO: make a check if globalid is in correct format.

        self.logger.info("gemaalid column added successfully.")

    def add_columns_to_pomp(self):
        """Add necessary columns to the pomp layer."""
        self.logger.info("Adding necessary columns to the pomp layer...")
        # Load the pomp layer if not already loaded
        if self.pomp is None or self.gemaal is None:
            self.load_layers()

        # Add 'gemaalid' column if it does not exist
        if "gemaalid" not in self.pomp.columns:
            self.add_column_gemaalid()

        # TODO: discuss if this is nessessary to add
        # Add 'globalid' column if it does not exist
        # if "globalid" not in self.pomp.columns:
        #     self.logger.info("Adding 'globalid' column to the pomp layer")
        #     # Generate a unique globalid for each pomp
        #     self.pomp["globalid"] = [str(uuid.uuid4()) for _ in range(len(self.pomp))]
        #     self.logger.info("'globalid' column added successfully.")

        # TODO: use gemaal maximalecapaciteit for pomp maximalecapaciteit if aantalpompen = 1

    def update_pomp_layer(self):
        """Update the pomp layer with gemaalid and save it to the DAMO geopackage."""
        self.logger.info("Updating pomp layer with gemaalid...")
        # Load the pomp layer if not already loaded
        self.load_layers()
        # Add gemaalid column to the pomp layer
        self.add_columns_to_pomp()
        # Save the updated pomp layer to the DAMO geopackage
        pyogrio.write_dataframe(self.pomp, self.damo_file_path, layer="pomp", driver="GPKG", overwrite=True)
        self.logger.info("Pomp layer updated successfully.")

    def intesected_pump_peilgebiden(self):
        # transform MULTIPOLYGON  to LINESTRING
        # self.polder_polygon
        self.load_layers()
        if "distance_to_peilgebied" not in self.gemaal.columns:
            gdf_peilgebiedpraktijk_linestring = self.combinatiepeilgebied.copy()

            gdf_peilgebiedpraktijk_linestring["geometry"] = gdf_peilgebiedpraktijk_linestring["geometry"].apply(
                lambda geom: geom.boundary if geom.type == "MultiPolygon" else geom
            )
            gdf_peilgebiedpraktijk_linestring = gdf_peilgebiedpraktijk_linestring.explode(index_parts=True)
            gdf_peilgebiedpraktijk_linestring = gdf_peilgebiedpraktijk_linestring.reset_index(drop=True)

            # clip linestrings to the polder_polygon
            gdf_peilgebiedcombinatie = gpd.clip(gdf_peilgebiedpraktijk_linestring, self.polder_polygon)

            # make spatial join between gdf_gemaal and gdf_peilgebiedpraktijk_linestring distance 10 cm
            gemaal_spatial_join = gpd.sjoin_nearest(
                self.gemaal,
                gdf_peilgebiedcombinatie,
                how="inner",
                max_distance=0.01,
                distance_col="distance_to_peilgebied",
            )
            # rename column code_left to code
            if "code_left" in gemaal_spatial_join.columns:
                gemaal_spatial_join = gemaal_spatial_join.rename(columns={"code_left": "code"})

            # Join the column 'distance_to_peilgebied' from gemaal_spatial_join into gdf_gemaal based on the 'code' column
            gdf_gemaal = self.gemaal.merge(
                gemaal_spatial_join[["code", "distance_to_peilgebied"]], on="code", how="left"
            )

            # save the layer in DAMO
            gdf_gemaal.drop_duplicates().to_file(self.damo_file_path, layer="GEMAAL", driver="GPKG")

        else:
            print("column distance_to_peilgebied already exists")

    def gemaal_streefpeil_value(self):
        # load DAMO layers
        self.load_layers()

        # Make a buffer using the gemaal point shapefile to be intersected with the combinatiepeilgebied
        buffer_gemaal = self.gemaal.buffer(distance=1)

        # Intersect buffer_gemaal with combinatiepeilgebied
        buffer_gemaal_gdf = gpd.GeoDataFrame(self.gemaal.copy(), geometry=buffer_gemaal, crs=self.gemaal.crs)

        # drop duplicantes
        buffer_gemaal.drop_duplicates()

        # intersect gemaaal buffer with the combined peilgebied
        gemaal_intersect_peilgebied = gpd.overlay(buffer_gemaal_gdf, self.combinatiepeilgebied, how="intersection")

        # Rename all the columns, removing the "_1" if they have it
        gemaal_intersect_peilgebied.columns = [
            col[:-2] if col.endswith("_1") else col for col in gemaal_intersect_peilgebied.columns
        ]

        # select columns 'streefpeil_zomer', 'streefpeil_winter', 'soort_streefpeilom' to join from gemaal_intersect_peilgebied
        columns_to_join = ["streefpeil_zomer", "streefpeil_winter", "soort_streefpeilom", "code_2"]

        # gather all the columns to be used
        columns_to_keep = self.gemaal.columns.to_list() + columns_to_join

        # create a subset using the desired columns, and drop duplicates from the intersection between gemaal and the combined peilgebied
        gemaal = gemaal_intersect_peilgebied[columns_to_keep].drop_duplicates()

        # Select codes in a list to be used in a for loop
        codes = gemaal["code"].to_list()
        for code in codes:
            # Select features with the same code value
            code_selection = gemaal[gemaal["code"] == code]

            # Save the codes from the streefpeil to be store in a new column
            code_streefpeils = ", ".join(str(x) for x in code_selection["code_2"].values.tolist())

            # Save the code from de peilgebied in the column streefpeils_code
            gemaal.loc[gemaal["code"] == code, "streefpeils_codes"] = code_streefpeils

            # Select the waterlevel values in zomer and in winter
            values_zomer = code_selection["streefpeil_zomer"].values.tolist()
            values_winter = code_selection["streefpeil_winter"].values.tolist()

            # Check is the selection made previously have more than 1 feature. If there is no, the point does not fall in a boundary
            if len(code_selection) > 1:
                # loop over the zomer values
                for i in range(len(values_zomer) - 1):
                    # check if they are the same for the streefpeil praktijk and the sreefpeil afwijking
                    iqual = np.equal(values_zomer[i], values_zomer[i + 1])

                    # if they are the same store the same value in two separeted columns
                    if iqual == True:
                        gemaal.loc[
                            gemaal["code"] == code,
                            ["streefpeil_zomer_value_praktijk", "streefpeil_zomer_value_afwijking"],
                        ] = [values_zomer[0], values_zomer[0]]
                    # if are diferent store those values also
                    else:
                        gemaal.loc[gemaal["code"] == code, "streefpeil_zomer_value_praktijk"] = values_zomer[0]
                        gemaal.loc[gemaal["code"] == code, "streefpeil_zomer_value_afwijking"] = values_zomer[1]
                # loop over the winter values
                for i in range(len(values_winter) - 1):
                    # check if they have the same values
                    iqual = np.equal(values_winter[i], values_winter[i + 1])

                    # if they are the same store the same value in two separeted columns
                    if iqual == True:
                        gemaal.loc[
                            gemaal["code"] == code,
                            ["streefpeil_winter_value_praktijk", "streefpeil_winter_value_afwijking"],
                        ] = [values_winter[0], values_winter[0]]

                    # if are diferent store those values also
                    else:
                        gemaal.loc[
                            gemaal["code"] == code,
                            ["streefpeil_winter_value_praktijk", "streefpeil_winter_value_afwijking"],
                        ] = [values_winter[0], values_winter[1]]
            # If there only one value then save those values in the streefpeil praktijk
            else:
                gemaal.loc[gemaal["code"] == code, "streefpeil_zomer_value_praktijk"] = values_zomer[0]

                gemaal.loc[gemaal["code"] == code, "streefpeil_winter_value_praktijk"] = values_winter[0]

        # dissolve the values using the column code and drop the columns:"streefpeil_zomer", "streefpeil_winter", "soort_streefpeilom", "code_2"
        gemaal_disolve = gemaal.dissolve(by="code").drop(
            columns=["streefpeil_zomer", "streefpeil_winter", "soort_streefpeilom", "code_2"]
        )
        # Select columns to join
        columns_to_merge = [
            "code",
            "streefpeils_codes",
            "streefpeil_zomer_value_praktijk",
            "streefpeil_zomer_value_afwijking",
            "streefpeil_winter_value_praktijk",
            "streefpeil_winter_value_afwijking",
        ]
        # reset the index column and select only the columns set it in the previous step
        gemaal_buffer_subset = gemaal_disolve.reset_index(drop=False)[columns_to_merge]

        # Merge the dataframes on the 'code' column using the gemaal buffer subet
        gemaal_point = self.gemaal.drop_duplicates().merge(gemaal_buffer_subset, on="code", how="left")

        # save the layer in DAMO
        gemaal_point.to_file(self.damo_file_path, layer="GEMAAL", driver="GPKG")
        # return gemaal_point


# %%
if __name__ == "__main__":
    project_folder = Path(r"E:\09.modellen_speeltuin\test_with_pomp_table_juan")
    folder = Folders(project_folder)
    damo = folder.source_data.path / "DAMO.gpkg"
    intermediate_convertion = PompIntermediateConverter(damo)
    pump_function = intermediate_convertion.gemaal_streefpeil_value()


# delete specific layters that are inside geopackge.
# %%
# import fiona

# def delete_layer_from_geopackage(geopackage_path, layer_to_delete):
#     with fiona.open(geopackage_path, "r") as src:
#         schema = src.schema
#         crs = src.crs
#         driver = src.driver
#         # Filter out features from the layer you want to delete
#         features = [f for f in src if f['properties']['layer_name'] != layer_to_delete]

#     with fiona.open(geopackage_path, "w", driver=driver, schema=schema, crs=crs) as dst:
#         for feature in features:
#             dst.write(feature)


# layers = fiona.listlayers(damo)

# print("Layers in GeoPackage:")
# layers_to_delete = []
# for layer in layers:
#     print(layer)
#     geopackge = gpd.read_file(damo, layer=layer)
#     columns = geopackge.columns
#     if 'code' in columns:
#         print(layer)
#     else:
#         layers_to_delete.append(layer)
#     if len(geopackge)!= 0:
#         layers_to_delete.append(layer)

# for layer_to_delete in layers_to_delete:
#     delete_layer_from_geopackage(damo, layer_to_delete)

# delete empu
# delete_layer_from_geopackage("your_geopackage.gpkg", "your_layer_name")
#
# %%
