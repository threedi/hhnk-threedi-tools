# %%
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd

from hhnk_threedi_tools.core.folders import Folders

logger = hrt.logging.get_logger(name=__name__)


class StructureControl:
    """
    This class summarizes **table** control settings from your 3Di model and the 'Hydrologen Database'.

    Parameters
    ----------
    model: hrt.SpatialDatabase
        SpatialDatabase object that is your model (gpkg), i.e. folder.model.schema_base.database
    hdb_control_layer: hrt.SpatialDatabaseLayer
        SpatialDatabase object that referes to the control table overview in the HDB, i.e. folder.source_data.hdb.layers.sturing_kunstwerken
    output_file: str
        Path to gpkg output file, i.e. folder.output.sqlite_tests.gestuurde_kunstwerken.base

    Returns
    -------
    File `output_file` with an overview of table control settings

    #### TODO there is no check on the control logic in this class...
    """

    def __init__(self, model: hrt.SpatialDatabase, hdb_control_layer: hrt.SpatialDatabaseLayer, output_file: str):
        self.model = model
        self.hdb_control_layer = hdb_control_layer
        self.output_file = Path(output_file)

        self.layers = self.Layers()

    class Layers:
        def __init__(self):
            # Raw results
            self.control_table: gpd.GeoDataFrame
            self.control_measure_location: gpd.GeoDataFrame
            self.control_measure_map: gpd.GeoDataFrame
            self.connection_node: gpd.GeoDataFrame
            self.hdb_control: gpd.GeoDataFrame = None

            # Derived
            self.control_nodes: gpd.GeoDataFrame
            self.control_merge = {}  # Merged control tables split per structure type

    @property
    def local_layers(self):
        return self.layers

    def load_layers(self):
        # Load layers

        self.local_layers.control_table = self.model.load("table_control", index_column="id")
        self.local_layers.control_table.rename(
            {
                "tags": "control_table_tags",
                "code": "control_table_code",
                "display_name": "control_table_display_name",
            },
            axis=1,
            inplace=True,
        )
        if self.local_layers.control_table.empty:
            logger.info("No control_table entries found in the model database.")
        # Warn abouw control on culver, pipe or channel, not supported
        if self.local_layers.control_table["target_type"].isin(["culvert", "pipe", "channel"]).any():
            unsupported = self.local_layers.control_table[
                self.local_layers.control_table["target_type"].isin(["culvert", "pipe", "channel"])
            ]["target_type"].unique()
            logger.warning(
                f"Control structures of type(s) {unsupported} found in control_table. "
                "These are not supported in this check and will be ignored."
            )

        self.local_layers.control_measure_location = self.model.load("measure_location", index_column="id")
        self.local_layers.control_measure_location.rename(
            {
                "tags": "measure_location_tags",
                "code": "measure_location_code",
                "display_name": "measure_location_display_name",
            },
            axis=1,
            inplace=True,
        )

        self.local_layers.control_measure_map = self.model.load("measure_map", index_column="id")
        self.local_layers.control_measure_map.rename(
            {
                "tags": "measure_map_tags",
                "code": "measure_map_code",
                "display_name": "measure_map_display_name",
            },
            axis=1,
            inplace=True,
        )

        # Waarschuwing voor wanneer meerdere measure location per control id zijn gedefinieerd. # TODO testen
        multiple_measure_locations = (
            self.local_layers.control_measure_map.groupby("control_id")["measure_location_id"].nunique() > 1  # noqa: PD101
        )
        if multiple_measure_locations.any():
            multiple_ids = multiple_measure_locations[multiple_measure_locations].index.tolist()
            logger.warning(
                f"Multiple measure locations found for control_id(s): {multiple_ids}. "
                "This may lead to unexpected results."
            )

        self.local_layers.connection_node = self.model.load("connection_node", index_column="id")
        self.local_layers.connection_node.rename(
            {
                "tags": "connection_node_tags",
                "code": "connection_node_code",
                "display_name": "connection_node_display_name",
            },
            axis=1,
            inplace=True,
        )

        if self.hdb_control_layer.parent.exists():
            self.local_layers.hdb_control = self.hdb_control_layer.load()[
                ["CODE", "STREEFPEIL", "MIN_KRUINHOOGTE", "MAX_KRUINHOOGTE"]
            ]
            self.local_layers.hdb_control.rename(
                columns={
                    "CODE": "code",
                    "STREEFPEIL": "hdb_streefpeil",
                    "MIN_KRUINHOOGTE": "hdb_kruin_min",
                    "MAX_KRUINHOOGTE": "hdb_kruin_max",
                },
                inplace=True,
            )

    def merge_control(self):
        """Merge control tables with the measure map geometry as base"""
        self.control_gdf = (
            self.local_layers.control_measure_map.merge(
                self.local_layers.control_table.drop(columns="geometry"),
                how="left",
                left_on="control_id",
                right_index=True,
            )
            .merge(
                self.local_layers.control_measure_location.drop(columns="geometry"),
                how="left",
                left_on="measure_location_id",
                right_index=True,
            )
            .merge(
                self.local_layers.connection_node.drop(
                    columns=[
                        "geometry",
                        "visualisation",
                        "hydraulic_conductivity_in",
                        "hydraulic_conductivity_out",
                        "exchange_thickness",
                    ]
                ),
                how="left",
                left_on="connection_node_id",
                right_index=True,
            )
        )
        # add structure code
        self.control_gdf["struct_code"] = ""
        structure_types = self.control_gdf["target_type"].unique()
        for s in structure_types:
            # load df with strcuture code per id
            structure_codes = self.model.load(s, index_column="id")["code"]
            # update structure code in control_gdf if target_type matches
            self.control_gdf.loc[self.control_gdf["target_type"] == s, "struct_code"] = self.control_gdf.loc[
                self.control_gdf["target_type"] == s, "target_id"
            ].map(structure_codes)

    def get_action_values(self, row: pd.Series) -> tuple:
        """-> start, min, max values in action table"""
        if row["action_type"] == "set_crest_level":
            action_values = [float(b.split(",")[1]) for b in row["action_table"].split("\n")]
        elif row["action_type"] == "set_discharge_coefficients":
            action_values = [float(b.split(",")[1].split(",")[0]) for b in row["action_table"].split("\n")]
        else:  # TODO set_gate_level, or set_pump_capacity, what does control for these look like?
            logger.error(f"Action type {row['action_type']} not supported for control id {row['control_id']}")
        return action_values[0], min(action_values), max(action_values)

    def append_hdb_layer(self):
        if self.layers.hdb_control is not None:
            return self.control_gdf.merge(
                self.layers.hdb_control,
                left_on="struct_code",
                right_on="code",
                how="left",
                suffixes=["", "_hdb"],
            )
        else:
            return self.control_gdf

    def save(self):
        """Save output to file"""
        self.control_gdf = gpd.GeoDataFrame(self.control_gdf)
        self.control_gdf.to_file(self.output_file)

    def run(self, overwrite: bool = False) -> gpd.GeoDataFrame:
        # Check overwrite
        create = hrt.check_create_new_file(output_file=self.output_file, overwrite=overwrite)

        if create:
            # Load layers from sqlite
            self.load_layers()

            # Merge the different control tables
            self.merge_control()

            # Add action values
            self.control_gdf[["start_action_value", "min_action_value", "max_action_value"]] = self.control_gdf.apply(
                self.get_action_values, axis=1, result_type="expand"
            )

            # append_hdb_layer
            self.control_gdf = self.append_hdb_layer()

            self.save()
            return self.control_gdf


def create_sorted_actiontable_queries(database: hrt.SpatialDatabase) -> list[str]:
    """
    Sommige modellen hebben een sturing die niet door de validatie van 3Di komt.
    Hier is ergens een keer de action_table verkeerd gesorteerd.

    Met dit script kan de sortering goed gezet worden in het model.

    Parameters
    ----------
    database : hrt.SpatialDatabase
        path to the database. This can be retrieved from htt.Folders with:
        folder.model.schema_base.database

    Returns
    -------
    queries : list[str]
        list of sql queries of all controls in the provided model. This list should be
    """

    control_df = database.load("table_control", index_column="id")
    queries = []

    # Voor elke sturings regel de actiontable sorteren
    for index, row in control_df.iterrows():
        action_table_string = row["action_table"]

        measure_list = []
        action_list = []

        action_type = row["action_type"]
        for entry in action_table_string.split("\n"):
            try:
                measurement = [float(entry.split(",")[0])]
                measure_list.append(measurement[0])
            except ValueError as e:
                # Problem with action table
                logger.error(f"""Problem with '{entry}' at index {action_table_string.index(entry)} of the action_table_string for
    {row}
    """)
                raise e

            if action_type in ["set_crest_level", "set_pump_capacity"]:
                action = [float(entry.split(",")[1])]
                action_list.append(action[0])

            order = np.argsort(measure_list)

            measure_order = np.array(measure_list)[order]
            action_order = np.array(action_list)[order]

            action_string = ""
            for nr, (m, a) in enumerate(zip(measure_order, action_order)):
                action_string += f"{m},{a}"
                if nr != len(measure_order) - 1:
                    action_string += "\n"

        update_str = f"UPDATE table_control SET action_table='{action_string}' WHERE id={index}"

        queries.append(update_str)
    return queries


def update_sorted_actiontable(database: hrt.SpatialDatabase, queries: list[str]) -> None:
    logger.info(f"Updating {len(queries)} table controls for {database.name}")
    for query in queries:
        database.modify_gpkg_using_query(query=query)


# %%
if __name__ == "__main__":
    for i in range(1, 5):
        TEST_MODEL = Path(__file__).parents[i].absolute() / "tests/data/model_test/"
        folder = Folders(TEST_MODEL)
        if folder.exists():
            break

    self = StructureControl(
        model=folder.model.schema_base.database,
        hdb_control_layer=folder.source_data.hdb.layers.sturing_kunstwerken,
        output_file=folder.output.hhnk_schematisation_checks.gestuurde_kunstwerken.path,
    )
    self.run(overwrite=True)

    test_set_discharge_coeeficient = "-10,0,0.5\n-2,1.0,0.2"
    self.get_action_values(test_set_discharge_coeeficient)

# %%
