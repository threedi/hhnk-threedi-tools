# %%
"""
Layer structure is de basis voor een qgisproject om automatisch
lagen toe te voegen.
Deze staan in een csv gedefineerd (vb: r'tests_hrt\data\layer_structure.csv')
en worden hier uitgelezen om ze aan het project toe te voegen.
"""
import os
from dataclasses import dataclass, field

import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
from typing import Union
from pathlib import Path


@dataclass
class QgisLayerSettings:
    """
    Base settings for a qgis layer.
    provide either:
        - file with optionally filters
        - wms_source -> wms source url.

        qml_lst (list):
            list of hrt.File objects

    TODO: match columns in csv-file with properties in data-class
    """

    name: str
    file: Union[str, Path, hrt.File, None] = None
    filters: str = None
    wms_source: str = None
    qml_lst: list = field(default_factory=list)
    group_lst: list = field(default_factory=list)
    subject: str = None
    load_layer: bool = True
    # theme_lst: list = None

    def __post_init__(self):
        self.ftype = self.get_ftype()

        # set source
        self.source = self.get_source()

    def get_ftype(self):
        """Get ftype based on file suffix or source
        get available ftypes from; QgsProviderRegistry.instance().providerList()
        they are input in QgsVectorLayer and QgsRasterLayer
        """
        # Get ftype from file suffix
        if self.file and self.wms_source:
            print(f"file: {self.file}")
            print(f"wms_source: {self.wms_source}")
            raise Exception("Cannot provide both file and wms_source")

        ftype = None
        if self.file:
            try:
                if self.file.suffix in [".shp", ".gdb", ".gpkg"]:
                    ftype = "ogr"
                elif ".tif" in self.file.suffix:
                    ftype = "gdal"  # raster
            except:
                # Filetype unknown.
                pass

        # Get ftype from (wms)source
        if self.wms_source:
            if ("MapServer" in self.wms_source) and ("/arcgis/rest/" in self.wms_source):
                ftype = "arcgismapserver"
            elif ("FeatureServer" in self.wms_source) and ("/arcgis/rest/" in self.wms_source):
                ftype = "arcgisfeatureserver"
            else:
                ftype = "wms"
        return ftype

    def get_source(self):
        source = None
        if self.file:
            if self.filters is not None:
                source = rf"{self.file}|{self.filters}"
            else:
                source = rf"{self.file}"
        if self.wms_source:
            source = self.wms_source
        return source

    @property
    def id(self):
        """Layer names are net necessarily unique in qgis.
        The combination of of name and group_lst is however"""
        # TODO test group_lst = None
        return f"{self.name}____{'__'.join(self.group_lst)}"

    @property
    def group_id(self):
        return f"{'__'.join(self.group_lst)}"

    def __repr__(self):
        return f"""<class {self.__class__.__name__}>: {self.name}
    variables: .{" .".join(hrt.get_variables(self, stringify=False))}"""


class QgisThemeSettings:
    """QgisTheme has a name and layers.
    name (str): name of theme
    layer_ids (list): list of htt.QgisLayerSettings items

    """

    def __init__(self, name: str, layer_ids: list = []):
        self.name = name
        self.layer_ids = []
        self.add_layers(layer_ids=layer_ids)

    @property
    def id(self):
        return self.name

    def add_layer(self, layer):
        """Add single layer to theme"""
        if layer not in self.layer_ids:
            self.layer_ids.append(layer)

    def add_layers(self, layer_ids: list):
        """Add multiple layers to theme

        layer_ids (list): list of layer ids.
        """
        for layer in layer_ids:
            self.add_layer(layer=layer)

    def __repr__(self):
        return f"""<class {self.__class__.__name__}>: {self.name}
    layer_ids ({len(self.layer_ids)}x)={self.layer_ids}"""


class QgisGroupSettings:
    def __init__(self, lvl, name="qgis_main", parent_group_lst=[], load_group=True, subject=None):
        self.name = name
        self.lvl = lvl
        self.parent_group_lst = parent_group_lst
        self.load_group = load_group
        self.subject = subject

        self.children = {}

    def add_child(self, name, lvl, load_group, subject):
        if (name not in self.children.keys()) and (name is not None):
            if self.name != "qgis_main":
                parent_group_lst = self.parent_group_lst + [self.name]
            else:
                parent_group_lst = []

            # Create child class
            self.children[name] = self.__class__(
                name=name, lvl=lvl, parent_group_lst=parent_group_lst, load_group=load_group, subject=subject
            )
        return self.children[name]

    def get_children(self):
        for child in self.children.values():
            yield child

    def get_all_children(self):
        """recursively yield all children."""
        if self.name != "qgis_main":
            yield self
        for child in self.get_children():
            yield from child.get_all_children()

    @property
    def id(self):
        return f"{'__'.join(self.parent_group_lst + [self.name])}"

    @property
    def parent_id(self):  # id of parent group
        return f"{'__'.join(self.parent_group_lst)}"

    @property
    def parent_name(self):
        if len(self.parent_group_lst) > 0:
            return self.parent_group_lst[-1]
        else:
            return ""

    def __repr__(self):
        """
        should view self and all children indented like this:
        group_lvl1
            group_lvl2a
                group_lvl3
            group_lvl2b
            group_lvl2c
                group_lvl3
        """
        reprstr = f"{self.__class__}\n"
        for node in self.get_all_children():
            tabstr = "\t" * node.lvl  # + '└'+ '─'*node.lvl
            reprstr += f"{tabstr} {node.name}\n"

        return reprstr


class QgisAllGroupsSettings:
    def __init__(self, layers):
        self.df = self.create_groups_df(layers)
        self.groups = self.generate_groups()

    def create_groups_df(self, layers):
        df_group = pd.DataFrame(layers.apply(lambda x: x.group_lst).values.tolist()).add_prefix("lvl_")
        df_group["id"] = layers.apply(lambda x: x.id.split("____")[-1]).reset_index(drop=True)
        df_group["load_group"] = layers.apply(lambda x: x.load_layer).reset_index(drop=True)
        df_group["subject"] = layers.apply(lambda x: x.subject).reset_index(drop=True)
        df_group = df_group.drop_duplicates(["id"]).set_index("id", drop=True)
        return df_group

    def generate_groups(self):
        """generate_groups from dataframe"""
        groups = QgisGroupSettings(lvl=0, parent_group_lst=[])

        for idx, row in self.df.iterrows():
            for col in self.df.keys():
                if col.startswith("lvl_"):
                    name = row[col]
                    lvl = int(col.split("lvl_")[-1]) + 1
                    if name is not None:
                        if lvl == 1:
                            child = groups.add_child(
                                name=name, lvl=lvl, load_group=row["load_group"], subject=row["subject"]
                            )
                        else:
                            child = child.add_child(
                                name=name, lvl=lvl, load_group=row["load_group"], subject=row["subject"]
                            )
            else:
                continue
            break
        return groups


@dataclass
class SelectedRevisions:
    """Names of selected revisions to use in LayerStructure"""

    check_0d1d: str = ""
    check_1d2d: str = ""
    klimaatsommen: str = ""


class LayerStructure:
    """
    Contruct layer structure from a source csv.

    Themes can be added using new columns in the csv that start with 'theme_'
    All layers that you want to add to a theme should be 'True' in that column.

    Most notable attributes of this structure are:
    .layers (pd.Series): layers as QgisLayerSettings
    .themes (pd.Series): themes as QgisThemeSettings

    a layer has groups and possibly themes. All are recored in these series.

    TODO:
        - convert to dataclass
        - consider integrating self.run() in self.__post_init__(), possibly only when debug=False
    """

    def __init__(
        self,
        layer_structure_path=None,
        subjects=None,
        revisions: SelectedRevisions = SelectedRevisions(),
        folder=None,
    ):
        # FIXME meeste input naar kwargs. Deze zijn specifiek nodig voor de bijhoordende xls.
        # init variables
        self.file = hrt.File(layer_structure_path)
        self.subjects = subjects
        self.revisions = revisions

        self.folder = folder

    def load_df(self, subjects):
        """load csv as dataframe and filter the subjects."""
        if self.file.exists():
            self.df_full = pd.read_csv(
                self.file.path, sep=";"
            )  # Read csv with configuration for the available layers.
            self.df_full = self.df_full.where(pd.notnull(self.df_full), None)  # nan to none

            # FIXME subjects op andere manier. We laden nu altijd alles.
            if subjects is not None:
                self.df = self.df_full[self.df_full["subject"].isin(subjects)]  # Filter on selected subjects.
            else:
                self.df = self.df_full
                self.subjects = np.unique(self.df["subject"].values)
        self._verify_input()

    def _verify_input(self):
        """Verify input. Needs to be done after loading df."""
        # If wrong structure path raise
        if (self.file._base is not None) and (not self.file.exists()):
            raise Exception(f"File does not exist: {self.file}")

        # Check all subjects in df
        if self.subjects is not None:
            if not all([s in np.unique(self.df_full["subject"].values) for s in self.subjects]):
                raise Exception(f"Not all subjects are in df: {self.subjects}")

    def get_layers_from_df(self) -> pd.Series:
        """Get a list of all layers"""
        layers = {}
        for index, row in self.df_full.iterrows():
            layer = self._get_layer_from_row(row)
            layers[layer.id] = layer
        return pd.Series(layers)

    def get_themes_from_df(self) -> pd.Series:
        """Get a series with themes.
        First read the layers with self.get_layers_from_df"""
        theme_col_names = [i for i in self.df_full.keys() if i.startswith("theme_")]
        themes = {}
        for theme_col_name in theme_col_names:
            theme_filter = self.df_full[theme_col_name] == True

            layer_ids = self.df_full.loc[theme_filter, "layer"].apply(lambda x: x.id).tolist()
            name = theme_col_name[6:]  # remove str theme_
            themes[name] = QgisThemeSettings(name=name, layer_ids=layer_ids)
        return pd.Series(themes)

    def _get_layer_from_row(self, row) -> QgisLayerSettings:
        """Evalualte df row and create a QgisLayer instance"""

        def eval_filenames(name, suffix, folder, revisions):
            """some filenames are variables"""
            if "." in name:
                return eval(name)
            else:
                return f"{name}.{suffix}"

        def eval_qml(qmldir, qmlnames):
            """create list of qmlpaths with row.qmldir and row.qmlnames"""
            qmldir = eval(qmldir)
            if qmlnames.startswith("["):
                # Already a list
                qmlnames = eval(qmlnames)
            else:  # Set single qml name to list
                qmlnames = [qmlnames]
            qmlpaths = []
            for name in qmlnames:
                qmlpaths.append(hrt.Folder(qmldir).full_path(name))
            return qmlpaths

        # required for eval
        folder = self.folder
        revisions = self.revisions

        # Voor wms staat de volledige link die nodig is in row.wms_source.
        file = None
        if row.filename:
            file = hrt.Folder(eval(str(row.filedir))).full_path(
                eval_filenames(row.filename, row.suffix, folder=folder, revisions=self.revisions)
            )

        group_lst = []
        if row.group_lst:
            group_lst = eval(row.group_lst)

        qml_lst = []
        if row.qmlnames:
            qml_lst = eval_qml(qmldir=row.qmldir, qmlnames=row.qmlnames)

        l = QgisLayerSettings(
            name=row.qgis_name,
            file=file,
            filters=row.filters,
            wms_source=row.wms_source,
            qml_lst=qml_lst,
            group_lst=group_lst,
            load_layer=row.subject in self.subjects,
            subject=row.subject,
        )
        return l

    def run(self):
        # Load df
        self.load_df(self.subjects)
        # Load layers in df and add them as extra column
        self.layers = self.get_layers_from_df()
        self.df_full["layer"] = self.layers.reset_index(drop=True)

        self.groups = QgisAllGroupsSettings(layers=self.layers)
        # Get the themes and their layer ids.
        self.themes = self.get_themes_from_df()


if __name__ == "__main__":
    pass
