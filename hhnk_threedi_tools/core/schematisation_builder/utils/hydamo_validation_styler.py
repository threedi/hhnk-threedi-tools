import shutil
import fiona
import geopandas as gpd
from pathlib import Path
import hhnk_research_tools as hrt

LAYERS_WITH_CATEGORY = [
    "stuw",
    "regelmiddel",
    "profielpunt",
    "pomp",
    "gemaal",
    "brug",
    "profiellijn",
    "hydroobject",
    "duikersifonhevel",
    "profielgroep",
    "kunstwerkopening",
    "doorstroomopening",
]

class HyDAMOValidationStyler:
    def __init__(self, hydamo_path: Path, results_path: Path, template_path: Path, logger=None):
        self.hydamo_path = hydamo_path
        self.results_path = results_path
        self.template_path = template_path
        self.logger = self._logger(logger)

    def _logger(self, logger):
        if not logger:
            logger = hrt.logging.get_logger(__name__)
        logger.info("Validation Styler")
        return logger

    def _store_gpkg(self, path_to_gpkg: Path):
        layers = fiona.listlayers(path_to_gpkg)
        gpkg = {layer: gpd.read_file(path_to_gpkg, layer=layer) for layer in layers}
        return gpkg

    def add_hydamo_category(self):
        hydamo_gkpg = self._store_gpkg(self.hydamo_path)
        results_gpkg = self._store_gpkg(self.results_path)
        results_cat_gpkg = {}

        gdf_hydamo_hydroobject = gpd.GeoDataFrame()
        for layer, gdf_hydamo in hydamo_gkpg.items():
            if layer in ["hydroobject"]:
                gdf_hydamo_layer_columns = ["categorieoppwaterlichaam", "geometry"]
                gdf_hydamo_hydroobject = gdf_hydamo[gdf_hydamo_layer_columns].copy()
                gdf_hydamo_hydroobject["geometry_right"] = gdf_hydamo_hydroobject.geometry

        for layer, gdf_result_layer in results_gpkg.items():
            gdf_result_layer_columns = gdf_result_layer.columns

            if layer not in LAYERS_WITH_CATEGORY:
                gdf_joined_layer = gdf_result_layer.copy()
                gdf_joined_layer["categorieoppwaterlichaam"] = None
            else:
                self.logger.info(layer)
                gdf_joined_layer = gpd.sjoin_nearest(gdf_result_layer, gdf_hydamo_hydroobject, how="left")
                if layer in ["hydroobject"]:
                    gdf_joined_layer = gdf_joined_layer[gdf_joined_layer.geometry.geom_equals(gdf_joined_layer.geometry_right)]
            
            gdf_result_layer_cat_columns = gdf_result_layer_columns.to_list() + ["categorieoppwaterlichaam"]
            gdf_result_layer_cat = gdf_joined_layer[gdf_result_layer_cat_columns].copy()     

            results_cat_gpkg[layer] = gdf_result_layer_cat

        self.results_cat_gpkg: dict[str, gpd.GeoDataFrame] = results_cat_gpkg 

    def apply_style(self, path: Path = None):
        if path is None:
            path = self.results_path

        shutil.copyfile(self.template_path, path)
        for layer, gdf in self.results_cat_gpkg.items():
            gdf.to_file(path, layer=layer, mode="w", engine="pyogrio")

    def save_to_gpkg(self, path: Path = None) -> None:
        '''
        Saves results.gpkg with styling. Export path defaults to results path.
        '''
        self.add_hydamo_category()
        self.apply_style(path)
        
