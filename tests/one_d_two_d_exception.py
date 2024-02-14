# %% reproduce exception
import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
from threedigrid.admin.constants import NO_DATA_VALUE

from hhnk_threedi_tools.core.checks.one_d_two_d import OneDTwoDTest
from hhnk_threedi_tools.core.result_rasters.calculate_raster import BaseCalculatorGPKG
from hhnk_threedi_tools.core.result_rasters.netcdf_to_gridgpkg import NetcdfToGPKG
from tests.config import FOLDER_NEW, FOLDER_TEST

# Globals
REVISION = "BWN bwn_test #6 1d2d_test"

# test before Exception
check_1d2d = OneDTwoDTest(folder=FOLDER_TEST, revision=REVISION)
check_1d2d.output_fd.create(parents=True)
output_fd = check_1d2d.folder.output.one_d_two_d[check_1d2d.revision]

# here exception is raised
# check_1d2d.run_wlvl_depth_at_timesteps(overwrite=True)

overwrite = True
self = check_1d2d

# def run_wlvl_depth_at_timesteps(self, overwrite=False):
#     """Transform netcdf to grid gpkg and apply wlvl correction
#     Then create waterlevel and depth rasters at 3 timesteps:
#     1h : start rain
#     3h : end rain
#     15h : end calculation
#     """
netcdf_gpkg = NetcdfToGPKG.from_folder(folder=self.folder, threedi_result=self.result_fd)

# Convert netcdf to grid gpkg
netcdf_gpkg.run(
    output_file=self.output_fd.grid_nodes_2d,
    timesteps_seconds=[T * 3600 for T in self.TIMESTEPS],
    overwrite=True,
)

# Create depth and wlvl rasters for each timestep.
grid_gdf = gpd.read_file(self.output_fd.grid_nodes_2d.path)

T = 1
raster_calc = BaseCalculatorGPKG(
    dem_path=self.folder.model.schema_base.rasters.dem,
    grid_gdf=grid_gdf,
    wlvl_column=f"wlvl_{T}h",
)
output_file = getattr(self.output_fd, f"waterdiepte_T{T}")
mode = "MODE_WLVL"
min_block_size = 1024
# %%
self = raster_calc

self.output_raster = hrt.Raster(output_file)
self.nodeid_raster = self.output_raster.parent.full_path("nodeid.tif")

create = hrt.check_create_new_file(output_file=self.output_raster.path, overwrite=overwrite)

self.cache = {}
# %%
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


# %%
# def block_calculate_delauney_wlvl(self, window):
#     """Return waterlevel array. of the given window

#     This uses both the grid layout from the constant level method and the
#     triangulation from the linear method.

#     Interpolation is used to determine the waterlevel for a result cell if
#     all of the following requirements are met:
#     - The point is inside a grid cell
#     - The point is inside the triangulation
#     - The sum of weights of active (not 'no data' nodes) is more than half
#       of the total weight of all nodes. Only active nodes are included in
#       the interpolation.

#     In all other cases, the waterlevel from the constant level method is
#     used.
#     """
# start with the constant level result
nodeid_block = self.nodeid_raster._read_array(window=window)
# start with the constant level result
# node_id_grid is 1d array of the node ids in the mesh grid
nodeid_arr = nodeid_block.ravel()
# the waterlevel is known per nodeid. This loopuptable gets the waterlevel
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
vertices = delaunay.vertices[simplices[in_interpol]]

# calculate weight, see print(spatial.Delaunay.transform.__doc__) and
# Wikipedia about barycentric coordinates
weight = np.empty(vertices.shape)
weight[:, :2] = np.sum(transform[:, :2] * (points_int - transform[:, 2])[:, np.newaxis], 2)
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

# Exception is here!
level[in_interpol_and_suitable] = np.sum(weight * nodelevel, axis=1)

# %%
# def delaunay(self):
#     """
#     Return a (delaunay, s1) tuple.

#     `delaunay` is a qhull.Delaunay object, and `s1` is an array of
#     waterlevels for the corresponding delaunay vertices.
#     """
from scipy.spatial import qhull
from threedidepth import morton

try:
    return self.cache[self.DELAUNAY]
except KeyError:
    points_grid = np.array(self.grid_gdf.centroid.apply(lambda x: [x.x, x.y]).to_list())

    # reorder a la lizard
    points_grid, wlvl = morton.reorder(points_grid, self.wlvl_raw)
    delaunay = qhull.Delaunay(points_grid)
    self.cache[self.DELAUNAY] = delaunay, wlvl
#    return delaunay, wlvl
# %%
