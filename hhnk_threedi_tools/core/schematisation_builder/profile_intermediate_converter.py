from pathlib import Path
from typing import Optional
from shapely.geometry import MultiLineString
from shapely.validation import make_valid
import geopandas as gpd
import pandas as pd
 
import hhnk_research_tools as hrt

class ProfileIntermediateConverter:
    """
    Intermediate converter for profile data.
    From source (DAMO and ODS/CSO) to intermediate format, ready for converting to HyDAMO.
 
    Functionalities
    ---------------
    - Read and validate DAMO layers
    - Linemerge hydroobjects
 
    Parameters
    ----------
    damo_file_path : Path
        Path to the DAMO geopackage file.
    ods_cso_file_path : Path
        Path to the ODS/CSO geopackage file.
    logger : logging.Logger, optional
        Logger for logging messages. If not provided, a default logger will be used.
    """
 
    def __init__(
        self,
        damo_file_path: Path,
        ods_cso_file_path: Path,
        logger=None
    ):
        self.damo_file_path = Path(damo_file_path)
        self.ods_cso_file_path = Path(ods_cso_file_path)

        if logger:
            self.logger = logger
        else:
            self.logger = hrt.logging.get_logger(__name__)

        self.hydroobject_raw: Optional[gpd.GeoDataFrame] = None
        self.hydroobject_linemerged: Optional[gpd.GeoDataFrame] = None

        self.gw_pro: Optional[gpd.GeoDataFrame] = None
        self.gw_prw: Optional[gpd.GeoDataFrame] = None
        self.gw_pbp: Optional[gpd.GeoDataFrame] = None
        self.iws_geo_beschr_profielpunten: Optional[gpd.GeoDataFrame] = None

        self.gecombineerde_peilen: Optional[gpd.GeoDataFrame] = None

    def load_layers(self):
        """Load and preprocess necessary DAMO layers."""
        self.hydroobject_raw = self._load_and_validate(self.damo_file_path, "hydroobject")

        self.gw_pro = self._load_and_validate(self.damo_file_path, "gw_pro")
        self.gw_prw = self._load_and_validate(self.damo_file_path, "gw_prw")
        self.gw_pbp = self._load_and_validate(self.damo_file_path, "gw_pbp")
        self.iws_geo_beschr_profielpunten = self._load_and_validate(self.damo_file_path, "iws_geo_beschr_profielpunten")

        gecombineerde_peilen = self._load_and_validate(self.ods_cso_file_path, "gecombineerde_peilen")
        self.gecombineerde_peilen = gecombineerde_peilen.explode(index_parts=False).reset_index(drop=True)

    def _load_and_validate(self, source_path, layer_name: str) -> gpd.GeoDataFrame:
        """Load a layer from the DAMO geopackage and apply make_valid to its geometries."""
        gdf = gpd.read_file(source_path, layer=layer_name)
        gdf["geometry"] = gdf["geometry"].apply(make_valid)
        return gdf

    def process_linemerge(self):
        """Run line merge algorithm and store result."""
        if self.hydroobject_raw is None or self.gecombineerde_peilen is None:
            raise ValueError("Layers not loaded. Call load_damo_layers() first.")
        self.hydroobject_linemerged = self._linemerge_hydroobjects(
            self.gecombineerde_peilen, self.hydroobject_raw
        )

    def _linemerge_hydroobjects(self, gecombineerde_peilen, hydroobject):
        """Merges hydroobjects within a peilgebied."""
        merged_hydroobjects = []
        for _, peilgebied in gecombineerde_peilen.iterrows():
            hydroobjects = hydroobject[hydroobject.intersects(peilgebied.geometry)]
            if hydroobjects.empty:
                continue

            # First clip
            hydroobjects = hydroobjects.clip(peilgebied.geometry)
            if hydroobjects.empty:
                continue

            # Then union
            merged_hydroobject = hydroobjects.geometry.union_all()

            if merged_hydroobject.geom_type == 'GeometryCollection':
                self.logger.info(f"GeometryCollection found in peilgebied {peilgebied['gecombineerde_peilen_ID']}. Only (Multi)LineStrings will be kept.")
                merged_hydroobject = geometrycollection_to_linestring(merged_hydroobject)

            # upgrade all to MultiLineString, even if only one LineString is present
            if merged_hydroobject.geom_type == 'LineString':
                merged_hydroobject = MultiLineString([merged_hydroobject])

            hydroobject_codes = hydroobjects['CODE'].unique()
            merged_hydroobjects.append({
                'geometry': merged_hydroobject,
                'hydroobjectCODE': hydroobject_codes,
                'peilgebiedID': peilgebied['gecombineerde_peilen_ID']
            })

        merged_hydroobjects_gdf = gpd.GeoDataFrame(merged_hydroobjects, crs=hydroobject.crs)
        if merged_hydroobjects_gdf.empty:
            return None
        else:
            return merged_hydroobjects_gdf

    def find_linemerge_by_code(self, code: str) -> Optional[gpd.GeoSeries]:
        """
        Find the linemerged geometry that includes the given hydroobject CODE.
        """
        if self.hydroobject_linemerged is None:
            raise ValueError("Linemerged hydroobjects not yet processed.")

        match = self.hydroobject_linemerged[
            self.hydroobject_linemerged['hydroobjectCODE'].apply(lambda codes: code in codes)
        ]

        if match.empty:
            self.logger.warning(f"No linemerged hydroobject found for code '{code}'.")
            return None
        elif len(match) > 1:
            self.logger.warning(f"Multiple linemerged geometries found for code '{code}'. Returning first.")
        
        return match.iloc[0]

    def write_outputs(self, output_path: Path):
        """
        Write all GeoDataFrame attributes of this class to a GeoPackage or individual files.
        """
        output_path = Path(output_path)
        geodf_attrs = {
            name: val for name, val in self.__dict__.items()
            if isinstance(val, gpd.GeoDataFrame)
        }

        if not geodf_attrs:
            self.logger.warning("No GeoDataFrames found to write.")
            return

        if output_path.suffix == ".gpkg":
            for name, gdf in geodf_attrs.items():
                gdf.to_file(output_path, layer=name, driver="GPKG")
                self.logger.info(f"Wrote layer '{name}' to {output_path}")
        else:
            output_path.mkdir(parents=True, exist_ok=True)
            for name, gdf in geodf_attrs.items():
                gdf.to_file(output_path / f"{name}.gpkg", driver="GPKG")
                self.logger.info(f"Wrote file '{name}.gpkg' to {output_path}")


# General functions
def geometrycollection_to_linestring(geometry):
    """
    Filter a geometrycollection to keep only LineString or MultiLineString
    """
    if geometry.geom_type == 'GeometryCollection':
        fixed_geometry = [
            geom for geom in geometry.geoms
            if geom.geom_type in ['LineString', 'MultiLineString']
        ]
        if fixed_geometry:
            merged_geometry = gpd.GeoSeries(fixed_geometry).union_all()
            return merged_geometry
        else:
            return geometry
    
    return geometry