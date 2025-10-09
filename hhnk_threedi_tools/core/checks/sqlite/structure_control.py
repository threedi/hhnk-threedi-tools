# %%
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import pandas as pd
from hhnk_research_tools.variables import DEF_TRGT_CRS
from shapely.geometry import LineString

from hhnk_threedi_tools.core.folders import Folders


class StructureControl:
    """
    Deze test selecteert alle gestuurde kunstwerken (uit de v2_culvert, v2_orifice en v2_weir tabellen van het model) op
    basis van de v2_control_table. Per kunstwerk worden actiewaarden opgevraagd. Per gevonden gestuurd kunstwerk
    wordt ook relevante informatie uit de HDB database toegevoegd, zoals het streefpeil en minimale en maximale kruin
    hoogtes.
    """

    def __init__(self, model: hrt.Sqlite, hdb_control_layer: hrt.SpatialDatabaseLayer, output_file: str):
        self.model = model  # folder.model.schema_base.database
        self.hdb_control_layer = hdb_control_layer  # folder.source_data.hdb.layers.sturing_kunstwerken
        self.output_file = Path(output_file)  # folder.output.sqlite_checks.gestuurde_kunstwerken.base

        self.layers = self.Layers()

    class Layers:
        def __init__(self):
            # Raw results
            self.control: pd.DataFrame  # v2_control table
            self.control_table: pd.DataFrame  # v2_control_table table
            self.control_measure_map: pd.DataFrame  # v2_control_measure_map
            self.conn_nodes: pd.DataFrame  # v2_connection_nodes
            self.hdb_control: pd.DataFrame = None

            # Derived
            self.control_nodes: pd.DataFrame
            self.control_merge = {}  # Merged control tables split per structure type

    @property
    def l(self):
        return self.layers

    def load_layers(self):
        # Load layers
        self.l.control = self.model.read_table("v2_control")
        self.l.control.rename({"id": "v2_control_id"}, axis=1, inplace=True)
        self.l.control.set_index("v2_control_id", inplace=True)

        self.l.control_table = self.model.read_table("v2_control_table")
        self.l.control_table.rename({"id": "v2_control_table_id"}, axis=1, inplace=True)

        self.l.control_measure_map = self.model.read_table("v2_control_measure_map")
        self.l.control_measure_map.rename({"id": "v2_control_measure_map_id"}, axis=1, inplace=True)

        self.l.conn_nodes = self.model.read_table("v2_connection_nodes")
        self.l.conn_nodes.rename({"id": "node_id"}, axis=1, inplace=True)

        if self.hdb_control_layer.parent.exists():
            self.l.hdb_control = self.hdb_control_layer.load()[
                ["CODE", "STREEFPEIL", "MIN_KRUINHOOGTE", "MAX_KRUINHOOGTE"]
            ]
            self.l.hdb_control.rename(
                columns={
                    "CODE": "code",
                    "STREEFPEIL": "hdb_streefpeil",
                    "MIN_KRUINHOOGTE": "hdb_kruin_min",
                    "MAX_KRUINHOOGTE": "hdb_kruin_max",
                },
                inplace=True,
            )

    def merge_control(self):
        """merge control tables and split per structure type"""
        # Merge control tables
        control_merge_all = self.l.control.join(self.l.control_table.set_index("v2_control_table_id"))
        control_merge_all = pd.merge(
            control_merge_all.reset_index(drop=False),
            self.l.control_measure_map,
            left_on="measure_group_id",
            right_on="measure_group_id",
        )

        # Split per structure type
        for target_type in control_merge_all["target_type"].unique():
            self.l.control_merge[target_type] = control_merge_all[control_merge_all["target_type"] == target_type]

    def create_geometry_start_end(self, target_type: str, control_merge_structure: pd.DataFrame):
        """Create a linestring from the measure point to the controlled structure"""

        # Read structure table
        structure_df = self.model.read_table(target_type)[["id", "code", "connection_node_start_id"]]

        # Merge with structure
        control_merge_structure = pd.merge(control_merge_structure, structure_df, left_on="target_id", right_on="id")
        control_merge_structure.set_index("v2_control_id", inplace=True)

        # Create linegeometry from measure point to startnode structure
        start_nodes = pd.merge(control_merge_structure, self.l.conn_nodes, left_on="object_id", right_on="node_id")[
            ["control_id", "geometry"]
        ]
        end_nodes = pd.merge(
            control_merge_structure, self.l.conn_nodes, left_on="connection_node_start_id", right_on="node_id"
        )[["control_id", "initial_waterlevel", "geometry"]]
        self.l.control_nodes = pd.merge(
            start_nodes, end_nodes, left_index=True, right_index=True, suffixes=("_start", "_end")
        )
        self.l.control_nodes.set_index("control_id_start", inplace=True)
        geom = self.l.control_nodes.apply(lambda x: LineString([x["geometry_start"], x["geometry_end"]]), axis=1)

        control_merge_structure["initial_wlvl"] = self.l.control_nodes["initial_waterlevel"]
        return gpd.GeoDataFrame(control_merge_structure, geometry=geom, crs=DEF_TRGT_CRS)

    def get_action_values(self, row):
        """-> start, min, max values in action table"""
        if row["target_type"] == "v2_weir":
            action_values = [float(b.split(";")[1]) for b in row["action_table"].split("#")]
        else:
            action_values = [float(b.split(";")[1].split(" ")[0]) for b in row["action_table"].split("#")]
        return action_values[0], min(action_values), max(action_values)

    def append_hdb_layer(self):
        if self.layers.hdb_control is not None:
            return self.out_df.merge(self.layers.hdb_control, on="code", how="left", suffixes=["", "_hdb"])
        else:
            return self.out_df

    def save(self):
        """save output to file"""
        self.out_df = gpd.GeoDataFrame(self.out_df)
        self.out_df.to_file(self.output_file)

    def run(self, overwrite=False):
        # Check overwrite
        create = hrt.check_create_new_file(output_file=self.output_file, overwrite=overwrite)

        if create:
            # Load layers from sqlite
            self.load_layers()

            # merge the different control tables
            self.merge_control()

            # Add linestring geometry
            for target_type, control_merge_structure in self.l.control_merge.items():
                self.l.control_merge[target_type] = self.create_geometry_start_end(
                    target_type, control_merge_structure
                )

            # Combine structure tables
            if self.l.control_merge:
                control_df = pd.concat(self.l.control_merge.values())
            else:
                control_df = None

            # Create empty df, so we always have a result, then add the controls
            self.out_df = pd.DataFrame(
                columns=["control_id", "action_table", "target_type", "target_id", "code", "initial_wlvl", "geometry"]
            )
            self.out_df = pd.concat([self.out_df, control_df], join="inner")

            # add action values
            self.out_df[["start_action_value", "min_action_value", "max_action_value"]] = self.out_df.apply(
                self.get_action_values, axis=1, result_type="expand"
            )

            # append_hdb_layer
            self.out_df = self.append_hdb_layer()

            self.save()
            return self.out_df


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
        output_file=folder.output.sqlite_checks.gestuurde_kunstwerken.path,
    )
    self.run(overwrite=True)
