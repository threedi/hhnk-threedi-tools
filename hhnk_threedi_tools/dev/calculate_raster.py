# %%
"""
Most functions taken and edited from pip package: threedidepth.
Modified to have more flexibility in input and calculation
"""

# -*- coding: utf-8 -*-


import numpy as np
from scipy.interpolate import LinearNDInterpolator
from scipy.spatial import qhull

from osgeo import gdal
from threedigrid.admin.gridresultadmin import GridH5ResultAdmin
from threedigrid.admin.constants import SUBSET_2D_OPEN_WATER
from threedigrid.admin.constants import NO_DATA_VALUE
from threedidepth.fixes import fix_gridadmin
from threedidepth import morton


import hhnk_research_tools as hrt
import os

import geopandas as gpd

MODE_COPY = "copy"
MODE_NODGRID = "nodgrid"
MODE_CONSTANT_S1 = "constant-s1"
MODE_LINEAR_S1 = "linear-s1"
MODE_LIZARD_S1 = "lizard-s1"
MODE_CONSTANT = "constant"
MODE_LINEAR = "linear"
MODE_LIZARD = "lizard"



class BaseCalculatorGPKG:
    """Calculate interpolated rasters from a grid. The grid_gdf is 
    created using the class ThreediGrid. Which converts the NetCDF 
    into a gpkg. 

    It is possible to calculate the wlvl and wdepth.
    """

    PIXEL_MAP = "pixel_map"
    LOOKUP_WLVL = "lookup_wlvl"
    INTERPOLATOR = "interpolator"
    DELAUNAY = "delaunay"
    def __init__(self, output_folder,
                        output_raster_name,
                        dem_path,
                        grid_gdf,
                        wlvl_column,):

        self.output_raster = hrt.Raster(os.path.join(output_folder, output_raster_name))
        self.nodeid_raster = hrt.Raster(os.path.join(output_folder, "nodeid.tif"))
        self.dem_raster = hrt.Raster(dem_path)

        self.grid_gdf = grid_gdf
        self.wlvl_column = wlvl_column #Column to use in calculation


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

        `delaunay` is a qhull.Delaunay object, and `s1` is an array of
        waterlevels for the corresponding delaunay vertices.
        """
        # try:
        #     return self.cache[self.DELAUNAY]
        # except KeyError:
        if True:
            points_grid = np.array(self.grid_gdf.centroid.apply(lambda x: [x.x, x.y]).to_list())

            # reorder a la lizard
            points_grid, wlvl = morton.reorder(points_grid, self.wlvl_raw)
            delaunay = qhull.Delaunay(points_grid)
            self.cache[self.DELAUNAY] = delaunay, wlvl
            return delaunay, wlvl


    def _get_points_mesh(self, window):
        """Create mesh grid points with coordinates every 0.5m with the input window.
        Point are created in the centre of the cell (0.5)
        Args:
            indices (tuple): ((i1, j1), (i2, j2)) subarray indices
        """
        #i1=leftx, i2=rightx (or y.. not sure)
        (j1, i1), (j2, i2) = (window[0], window[1]), (window[0] + window[2], window[1] + window[3])
        #Create meshgrid with window bounds
        local_ji = np.mgrid[i1:i2, j1:j2].reshape(2, -1)[::-1].transpose()
        xstart, xres, b, ystart, c, yres = self.output_raster.metadata.georef

        #0.5*res gives a point in the centre of the cell. 
        return local_ji * [xres, yres] + [xstart + 0.5 * xres, ystart + 0.5 * -yres]


    def block_calculate_delauney_wlvl(self, window):
        """ Return waterlevel array. of the given window

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
        used."""
        # start with the constant level result
        nodeid_block = self.nodeid_raster._read_array(window=window)
        # start with the constant level result
        #node_id_grid is 1d array of the node ids in the mesh grid
        nodeid_arr = nodeid_block.ravel()
        #the waterlevel is known per nodeid. This loopuptable gets the waterlevel 
        #per point in the mesh grid 
        level = self.lookup_wlvl[nodeid_arr]

        # determine result raster cell centers and in which triangle they are
        points_mesh = self._get_points_mesh(window)
        delaunay, wlvl = self.delaunay
        simplices=delaunay.find_simplex(points_mesh)

        # determine which points will use interpolation
        in_gridcell = nodeid_arr != 0
        in_triangle = simplices != -1
        in_interpol = in_gridcell & in_triangle
        points_int = points_mesh[in_interpol]

        # get the nodes and the transform for the corresponding triangles
        transform = delaunay.transform[simplices[in_interpol]]
        vertices = delaunay.vertices[simplices[in_interpol]]

        # calculate weight, see print(spatial.Delaunay.transform.__doc__) and
        # Wikipedia about barycentric coordinates
        weight = np.empty(vertices.shape)
        weight[:, :2] = np.sum(
            transform[:, :2] * (points_int - transform[:, 2])[:, np.newaxis], 2
        )
        weight[:, 2] = 1 - weight[:, 0] - weight[:, 1]

        # set weight to zero when for inactive nodes
        nodelevel = wlvl[vertices]
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
        level[in_interpol_and_suitable] = np.sum(weight * nodelevel, axis=1)

        #Return interpolated mesh grid
        return level.reshape(nodeid_block.shape)


    def calculate_delauney_depth(self, window):

        wlvl_block = self.calculate_delauney_wlvl(window)

        return wlvl_block
        
    def create_nodeid_raster(self):
        """Create raster of nodeids with same res as dem."""
        if not self.nodeid_raster.exists:
            hrt.gdf_to_raster(gdf=self.grid_gdf,
                value_field="id",
                raster_out=self.nodeid_raster.source_path,
                nodata=0,
                metadata=self.dem_raster.metadata,
                datatype=gdal.GDT_Int32,
                read_array=False)


    def run(self, mode="MODE_WLVL", min_block_size=1024):
        #Create rasters
        self.create_nodeid_raster()
        self.output_raster.create(metadata=self.dem_raster.metadata, nodata=NO_DATA_VALUE)

        self.wlvl_raw = np.array(self.grid_gdf[self.wlvl_column]).copy()

        
        #Open outputraster
        # target_ds=gdal.Open(str(self.output_raster.source_path), gdal.GA_Update)
        # target_band = target_ds.GetRasterBand(1)


        #Do calculations per block
        self.output_raster.min_block_size = min_block_size
        self.output_raster.generate_blocks_geometry()
        for idx, block_row in self.output_raster.blocks.iterrows():
            window=block_row['window_readarray']

            if mode=="MODE_WLVL":
                block_out = self.block_calculate_delauney_wlvl(window)
            if mode=="MODE_WDEPTH":
                block_out = self.block_calculate_delauney_depth(window)
            break

            # target_band.WriteArray(block_out, xoff=window[0], yoff=window[1])
        # target_band.FlushCache()  # close file after writing
        target_band = None
        target_ds = None


    def __enter__(self):
        self.cache = {}
        return self

    def __exit__(self, *args):
        self.cache = {}


if __name__ == "__main__":
    from hhnk_threedi_tools import Folders

    folder_path = r"E:\02.modellen\23_Katvoed"
    folder = Folders(folder_path)

    threedi_result = folder.threedi_results.one_d_two_d['ghg_blok_t1000']
 

    grid_gdf = gpd.read_file(threedi_result.pl/"grid_raw.gpkg", driver="GPKG")


    calculator_kwargs = {"output_folder":threedi_result.pl,
                                "output_raster_name":"depth_test.tif",
                                "dem_path":folder.model.schema_base.rasters.dem.path,
                                "grid_gdf":grid_gdf, 
                                "wlvl_column":"wlvl_max_orig"}

    #Init calculator
    with BaseCalculatorGPKG(**calculator_kwargs) as self:
        self.run(mode="MODE_WLVL")

        print("Done.")
# %%
