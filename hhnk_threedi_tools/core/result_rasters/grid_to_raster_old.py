"""
Most functions taken and edited from pip package: threedidepth.
Modified to have more flexibility in input and calculation

#NOTE 2025-01 replaced with GridToWaterLevel and GridToWaterDepth in grid_to_raster.py

"""

from pathlib import Path

import hhnk_research_tools as hrt
import numpy as np
from osgeo import gdal
from scipy.spatial import Delaunay
from threedidepth import morton
from threedigrid.admin.constants import NO_DATA_VALUE

from hhnk_threedi_tools.core.folders import Folders

logger = hrt.logging.get_logger(__name__)


class BaseCalculatorGPKG:
    """TODO Deprecated, remove in later release."""

    def __init__(self, **kwargs):
        raise DeprecationWarning(
            "The BaseCalculatorGPKG class has been named to htt.GridToRaster since v2024.2. \
Which will also soon will be deprecated for GridToWaterLevel and GridToWaterDepth.\
Please rewrite your code."
        )


class GridToRaster:
    """Calculate interpolated rasters from a grid. The grid_gdf is
    created using the class ThreediGrid. Which converts the NetCDF
    into a gpkg.

    It is possible to calculate the wlvl and wdepth.
    """

    PIXEL_MAP = "pixel_map"
    LOOKUP_WLVL = "lookup_wlvl"
    INTERPOLATOR = "interpolator"
    DELAUNAY = "delaunay"

    def __init__(
        self,
        dem_path,
        grid_gdf,
        wlvl_column,
    ):
        self.dem_raster = hrt.RasterOld(dem_path)

        self.grid_gdf = grid_gdf
        self.wlvl_column = wlvl_column  # Column to use in calculation

        logger.warning("DeprecationWarning: GridToRaster has been replaced by GridToWaterLevel and GridToWaterDepth")

    @property
    def lookup_wlvl(self):
        """
        Return the lookup table to find waterlevel by cell id.

        Both cells outside any defined grid cell and cells in a grid cell that
        are currently not active ('no data') will return the NO_DATA_VALUE as
        defined in threedigrid.
        """
        try:
            return self.cache[self.LOOKUP_WLVL]
        except KeyError:
            lookup_wlvl = np.full((self.grid_gdf["id"]).max() + 1, NO_DATA_VALUE)

            lookup_wlvl[self.grid_gdf["id"]] = self.wlvl_raw
            self.cache[self.LOOKUP_WLVL] = lookup_wlvl
        return lookup_wlvl

    @property
    def delaunay(self):
        """
        Return a (delaunay, s1) tuple.

        `delaunay` is a scipy.spatial.Delaunay object, and `s1` is an array of
        waterlevels for the corresponding delaunay vertices.
        """
        try:
            return self.cache[self.DELAUNAY]
        except KeyError:
            points_grid = np.array(self.grid_gdf.centroid.apply(lambda x: [x.x, x.y]).to_list())

            # reorder a la lizard
            points_grid, wlvl = morton.reorder(points_grid, self.wlvl_raw)
            delaunay = Delaunay(points_grid)
            self.cache[self.DELAUNAY] = delaunay, wlvl
            return delaunay, wlvl.astype(float)

    def _get_points_mesh(self, window):
        """Create mesh grid points with coordinates every 0.5m with the input window.
        Point are created in the centre of the cell (0.5)
        Args:
            indices (tuple): ((i1, j1), (i2, j2)) subarray indices
        """
        # i1=leftx, i2=rightx (or y.. not sure)
        (j1, i1), (j2, i2) = (window[0], window[1]), (window[0] + window[2], window[1] + window[3])
        # Create meshgrid with window bounds
        local_ji = np.mgrid[i1:i2, j1:j2].reshape(2, -1)[::-1].transpose()
        xstart, xres, b, ystart, c, yres = self.output_raster.metadata.georef

        # 0.5*res gives a point in the centre of the cell.
        return local_ji * [xres, yres] + [xstart + 0.5 * xres, ystart + 0.5 * -yres]

    def block_calculate_delauney_wlvl(self, window):
        """Return waterlevel array. of the given window

        This uses both the grid layout from the constant level method and the
        triangulation from the linear method.

        Interpolation is used to determine the waterlevel for a result cell if
        all of the following requirements are met:
        - The point is inside a grid cell
        - The point is inside the triangulation
        - The sum of weights of active (not 'no data' nodes) is more than half
          of the total weight of all nodes. Only active nodes are included in
          the interpolation.

        In all other cases, the waterlevel from the constant level method is
        used.
        """
        # start with the constant level result
        nodeid_block = self.nodeid_raster._read_array(window=window)
        # start with the constant level result
        # node_id_grid is 1d array of the node ids in the mesh grid
        nodeid_arr = nodeid_block.ravel()
        # the waterlevel is known per nodeid. This lookuptable gets the waterlevel
        # per point in the mesh grid
        level = self.lookup_wlvl[nodeid_arr]

        # determine result raster cell centers and in which triangle they are
        points_mesh = self._get_points_mesh(window)
        delaunay, wlvl = self.delaunay
        simplices = delaunay.find_simplex(points_mesh)

        # determine which points will use interpolation
        in_gridcell = nodeid_arr != 0
        in_triangle = simplices != -1
        in_interpol = in_gridcell & in_triangle
        points_int = points_mesh[in_interpol]

        # get the nodes and the transform for the corresponding triangles
        transform = delaunay.transform[simplices[in_interpol]]
        simplices = delaunay.simplices[simplices[in_interpol]]

        # calculate weight, see print(spatial.Delaunay.transform.__doc__) and
        # Wikipedia about barycentric coordinates
        weight = np.empty(simplices.shape)
        weight[:, :2] = np.sum(transform[:, :2] * (points_int - transform[:, 2])[:, np.newaxis], 2)
        weight[:, 2] = 1 - weight[:, 0] - weight[:, 1]

        # set weight to zero when for inactive nodes
        nodelevel = wlvl[simplices]
        weight[nodelevel == NO_DATA_VALUE] = 0

        # determine the sum of weights per result cell
        weight_sum = weight.sum(axis=1)

        # further subselect points suitable for interpolation
        suitable = weight_sum > 0.5
        weight = weight[suitable] / weight_sum[suitable][:, np.newaxis]
        nodelevel = nodelevel[suitable]

        # combine weight and nodelevel into result
        in_interpol_and_suitable = in_interpol.copy()
        in_interpol_and_suitable[in_interpol] &= suitable
        level[in_interpol_and_suitable] = np.sum(weight.astype(float) * nodelevel.astype(float), axis=1)

        # Return interpolated mesh grid
        return level.reshape(nodeid_block.shape)

    def block_calculate_delauney_wdepth(self, window):
        """depth=wlvl-dem"""
        # Read/create input
        wlvl_block = self.block_calculate_delauney_wlvl(window)
        dem_block = self.dem_raster._read_array(window=window)

        # Mask output
        mask = (dem_block == self.dem_raster.nodata) | (wlvl_block == self.output_raster.nodata)
        # mask = np.any([masks[i] for i in masks],0)

        # Calculate depth
        block_out = wlvl_block - dem_block

        block_out[block_out < -0.01] = self.output_raster.nodata
        block_out[mask] = self.output_raster.nodata
        return block_out

    def create_nodeid_raster(self):
        """Create raster of nodeids with same res as dem."""
        if not self.nodeid_raster.exists():
            hrt.gdf_to_raster(
                gdf=self.grid_gdf,
                value_field="id",
                raster_out=self.nodeid_raster.path,
                nodata=0,
                metadata=self.dem_raster.metadata,
                datatype=gdal.GDT_Int32,
                read_array=False,
            )

    def run(self, output_file, mode="MODE_WLVL", min_block_size=1024, overwrite=False):
        # Init rasters
        self.output_raster = hrt.RasterOld(output_file)
        self.nodeid_raster = self.output_raster.parent.full_path("nodeid.tif")

        create = hrt.check_create_new_file(output_file=self.output_raster.path, overwrite=overwrite)

        if create:
            # Create rasters
            self.output_raster.create(metadata=self.dem_raster.metadata, nodata=NO_DATA_VALUE)
            self.create_nodeid_raster()  # Nodeid raster for calculation

            self.wlvl_raw = np.array(self.grid_gdf[self.wlvl_column]).copy()  # wlvl list

            # Open outputraster
            target_ds = self.output_raster.open_gdal_source_write()
            target_band = target_ds.GetRasterBand(1)

            # Do calculations per block
            for idx, window, block_row in self.output_raster.iter_window(min_block_size=min_block_size):
                if mode == "MODE_WLVL":
                    block_out = self.block_calculate_delauney_wlvl(window)
                if mode == "MODE_WDEPTH":
                    block_out = self.block_calculate_delauney_wdepth(window)

                target_band.WriteArray(block_out, xoff=window[0], yoff=window[1])
            target_band.FlushCache()  # close file after writing
            target_band = None
            target_ds = None

    def __enter__(self):
        """With GridToRaster(**args) as x. will call this func."""
        self.cache = {}
        return self

    def __exit__(self, *args):
        self.cache = {}


if __name__ == "__main__":
    from hhnk_threedi_tools import Folders

    OVERWRITE = True

    folder_path = r"E:\02.modellen\23_Katvoed"
    folder = Folders(folder_path)
    threedi_result = folder.threedi_results.one_d_two_d["ghg_blok_t1000"]

    # grid_gdf = gpd.read_file(threedi_result.path/"grid_raw.gpkg", driver="GPKG")
    grid_gdf = threedi_result.full_path("grid_corr.gpkg").load()

    calculator_kwargs = {
        "dem_path": folder.model.schema_base.rasters.dem.base,
        "grid_gdf": grid_gdf,
        "wlvl_column": "wlvl_max_orig",
    }

    # Init calculator
    with GridToRaster(**calculator_kwargs) as self:
        # self.run(output_file=threedi_result.full_path("wlvl_orig.tif")
        #             mode="MODE_WLVL",
        #             overwrite=OVERWRITE)

        self.run(output_file=threedi_result.full_path("wdepth_orig.tif"), mode="MODE_WDEPTH", overwrite=OVERWRITE)
        print("Done.")
