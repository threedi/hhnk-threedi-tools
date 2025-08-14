# %%
from pathlib import Path
from typing import Optional

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pyogrio

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
        self.logger.info("Polder polygon layer loaded successfully.")

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

    def intersected_pump_peilgebieden(self):
        # transform MULTIPOLYGON  to LINESTRING
        self.load_layers()
        # check if the columns exists.
        if "distance_to_peilgebied" not in self.gemaal.columns:
            # make a copy of the combinatiepeilgebied.
            gdf_peilgebiedpraktijk_linestring = self.combinatiepeilgebied.copy()

            # transform multiploygon to lines geom.boundary
            gdf_peilgebiedpraktijk_linestring["geometry"] = gdf_peilgebiedpraktijk_linestring["geometry"].apply(
                lambda geom: geom.boundary if geom.type == "MultiPolygon" else geom
            )

            # explode lines into part and reset index
            gdf_peilgebiedpraktijk_linestring = gdf_peilgebiedpraktijk_linestring.explode(index_parts=True)
            gdf_peilgebiedpraktijk_linestring = gdf_peilgebiedpraktijk_linestring.reset_index(drop=True)

            # clip linestrings to the polder_polygon
            gdf_peilgebiedcombinatie = gpd.clip(gdf_peilgebiedpraktijk_linestring, self.polder_polygon)

            # make spatial join between gdf_gemaal and gdf_peilgebiedpraktijk_linestring distance 10 cm
            gemaal_spatial_join = gpd.sjoin_nearest(
                self.gemaal,
                gdf_peilgebiedcombinatie,
                how="inner",
                max_distance=1000,
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

        # intersect gemaaal buffer with the combined peilgebied
        gemaal_intersect_peilgebied = gpd.overlay(buffer_gemaal_gdf, self.combinatiepeilgebied, how="intersection")

        # Rename all the columns, removing the "_1" if they have it
        gemaal_intersect_peilgebied.columns = [
            col[:-2] if col.endswith("_1") else col for col in gemaal_intersect_peilgebied.columns
        ]

        # select columns 'streefpeil_zomer', 'streefpeil_winter', 'soort_streefpeilom' to join from gemaal_intersect_peilgebied
        columns_to_join = ["streefpeil_zomer", "streefpeil_winter", "soort_streefpeilom", "peilgebied_soort", "code_2"]

        # gather all the columns to be used
        columns_to_keep = self.gemaal.columns.to_list() + columns_to_join

        # create a subset using the desired columns, and drop duplicates from the intersection between gemaal and the combined peilgebied
        gemaal = gemaal_intersect_peilgebied[columns_to_keep].drop_duplicates()

        # Select codes in a list to be used in a for loop
        codes = gemaal["code"].to_list()
        for code in codes:
            # Select features with the same code value
            code_selection = gemaal[gemaal["code"] == code]

            # Save the codes from the peilgebiede to be store in a new column that columns come from code_2 (code of the peilgebied)
            pgd_codes = ", ".join(str(x) for x in code_selection["code_2"].values.tolist())
            values_zomer = ", ".join(str(x) for x in code_selection["streefpeil_zomer"].values.tolist())
            values_winter = ", ".join(str(x) for x in code_selection["streefpeil_winter"].values.tolist())
            peil_gebied_soort = ", ".join(str(x) for x in code_selection["peilgebied_soort"].values.tolist())
            soort_streefpeilom = ", ".join(str(x) for x in code_selection["soort_streefpeilom"].values.tolist())

            # Save the code from de peilgebied in the column pgd_codes
            gemaal.loc[gemaal["code"] == code, "pgd_codes"] = pgd_codes
            gemaal.loc[gemaal["code"] == code, "streefpeil_peilgebide_zomer"] = values_zomer
            gemaal.loc[gemaal["code"] == code, "streefpeil_peilgebide_winter"] = values_winter
            gemaal.loc[gemaal["code"] == code, "soort_streefpeilom_comb"] = soort_streefpeilom
            gemaal.loc[gemaal["code"] == code, "peilgebied_soort_comb"] = peil_gebied_soort

        # dissolve the values using the column code and drop the columns:"streefpeil_zomer", "streefpeil_winter", "soort_streefpeilom", "code_2"
        gemaal_disolve = gemaal.dissolve(by="code").drop(
            columns=["streefpeil_zomer", "streefpeil_winter", "soort_streefpeilom", "code_2"]
        )

        # fucntie gemaal: 1 Sumply, 2, dranage: To avoid the level raises to much, 3.
        # Select columns to join
        columns_to_merge = [
            "code",
            "pgd_codes",
            "streefpeil_peilgebide_zomer",
            "streefpeil_peilgebide_winter",
            "soort_streefpeilom_comb",
            "peilgebied_soort_comb",
        ]
        # reset the index column and select only the columns set it in the previous step
        gemaal_buffer_subset = gemaal_disolve.reset_index(drop=False)[columns_to_merge]

        # Merge the dataframes on the 'code' column using the gemaal buffer subet
        gemaal_point = self.gemaal.drop_duplicates().merge(gemaal_buffer_subset, on="code", how="left")

        # Add the column 'gemaal_functie_value' based on the 'soort_streefpeilom_comb' column, use the list gemaal_functie_test to store the values
        gemaal_functie_test = []
        for zomer_values in gemaal_point["streefpeil_peilgebide_zomer"]:
            diff = np.diff([float(zomer_value) for zomer_value in zomer_values.split(",")])
            if len(diff) <= 1 and (not (diff.tolist()) or diff.tolist()[0] == 0):
                gemaal_functie_test.append(4)
            else:
                gemaal_functie_test.append(-999)
        # add the gemaal_functie_test to the gemaal_point dataframe
        gemaal_point["gemaal_functie_test"] = gemaal_functie_test
        # copy column functiegemaalcode in new column functiegemaalcode_DAMO
        gemaal_point["functiegemaalcode_damo"] = gemaal_point["functiegemaal"]

        # Add the column 'aantal_peilgebieden' based on the 'pgd_codes' column
        for count_peilgebieden in gemaal_point["pgd_codes"]:
            if count_peilgebieden is not None and count_peilgebieden != "":
                # count the number of peilgebieden
                count = len(count_peilgebieden.split(","))
                gemaal_point.loc[gemaal_point["pgd_codes"] == count_peilgebieden, "aantal_peilgebieden"] = count
            else:
                gemaal_point.loc[gemaal_point["pgd_codes"] == count_peilgebieden, "aantal_peilgebieden"] = 999

        # save the layer in DAMO
        gemaal_point.to_file(self.damo_file_path, layer="GEMAAL", driver="GPKG")

        return gemaal_point
        # return gemaal_point


# %%
if __name__ == "__main__":
    project_folder = Path(r"E:\09.modellen_speeltuin\test_with_pomp_table_juan")
    folder = Folders(project_folder)
    damo = folder.source_data.path / "DAMO.gpkg"
    intermediate_convertion = PompIntermediateConverter(damo)
    pump_function = intermediate_convertion.gemaal_streefpeil_value()
