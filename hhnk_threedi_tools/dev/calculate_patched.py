"""
@author: Wietse van Gerwen

File was copied from pip package: threedidepth.
Modified to also calculate raster for the max depth.
"""

# -*- coding: utf-8 -*-

from itertools import product
from os import path

try:
    import netCDF4
except ImportError:
    netCDF4 = None
import numpy as np
from scipy.interpolate import LinearNDInterpolator
from scipy.spatial import qhull

from osgeo import gdal
from threedigrid.admin.gridresultadmin import GridH5ResultAdmin
from threedigrid.admin.constants import SUBSET_2D_OPEN_WATER
from threedigrid.admin.constants import NO_DATA_VALUE
from threedidepth.fixes import fix_gridadmin
from threedidepth import morton

MODE_COPY = "copy"
MODE_NODGRID = "nodgrid"
MODE_CONSTANT_S1 = "constant-s1"
MODE_LINEAR_S1 = "linear-s1"
MODE_LIZARD_S1 = "lizard-s1"
MODE_CONSTANT = "constant"
MODE_LINEAR = "linear"
MODE_LIZARD = "lizard"


class BaseCalculator:
    """Depth calculator using constant waterlevel in a grid cell.

    Args:
        gridadmin_path (str): Path to gridadmin.h5 file.
        results_3di_path (str): Path to results_3di.nc file.
        calculation_step (int): Calculation step. OR value="MAX" to use the maximum wlvl
        dem_shape (int, int): Shape of the dem array.
        dem_geo_transform: (tuple) Geo_transform of the dem.
    """

    PIXEL_MAP = "pixel_map"
    LOOKUP_S1 = "lookup_s1"
    INTERPOLATOR = "interpolator"
    DELAUNAY = "delaunay"

    def __init__(
        self,
        gridadmin_path,
        results_3di_path,
        calculation_step,
        dem_shape,
        dem_geo_transform,
    ):
        self.gridadmin_path = gridadmin_path
        self.results_3di_path = results_3di_path
        self.calculation_step = calculation_step
        self.dem_shape = dem_shape
        self.dem_geo_transform = dem_geo_transform

    def __call__(self, indices, values, no_data_value):
        """Return result values array.

        Args:
            indices (tuple): ((i1, j1), (i2, j2)) subarray indices
            values (array): source values for the calculation
            no_data_value (scalar): source and result no_data_value

        Override this method to implement a calculation. The default
        implementation is to just return the values, effectively copying the
        source.

        Note that the no_data_value for the result has to correspond to the
        no_data_value argument.
        """
        raise NotImplementedError

    @staticmethod
    def _depth_from_water_level(dem, fillvalue, waterlevel):
        # determine depth
        depth = np.full_like(dem, fillvalue)
        dem_active = dem != fillvalue
        waterlevel_active = waterlevel != NO_DATA_VALUE
        active = dem_active & waterlevel_active
        depth_1d = waterlevel[active] - dem[active]

        # paste positive depths only
        negative_1d = depth_1d <= 0
        depth_1d[negative_1d] = fillvalue
        depth[active] = depth_1d

        return depth

    @property
    def lookup_s1(self):
        """
        Return the lookup table to find waterlevel by cell id.

        Both cells outside any defined grid cell and cells in a grid cell that
        are currently not active ('no data') will return the NO_DATA_VALUE as
        defined in threedigrid.
        """
        try:
            return self.cache[self.LOOKUP_S1]
        except KeyError:
            nodes = self.gr.nodes.subset(SUBSET_2D_OPEN_WATER)
            
            if self.calculation_step!="MAX": #TODO option to calculate raster based on maximum wlvl added.
                timeseries = nodes.timeseries(indexes=[self.calculation_step])
            else:
                timeseries = nodes.timeseries(indexes=slice(0, -1))

            data = timeseries.only("s1", "id").data
            lookup_s1 = np.full((data["id"]).max() + 1, NO_DATA_VALUE)

            if self.calculation_step!="MAX":
                s1 = data["s1"][0]
            else:
                s1_all = data["s1"]
                s1_max_ind = s1_all.argmax(axis=0)
                s1 = np.round(
                    [row[s1_max_ind[enum]] for enum, row in enumerate(s1_all.T)], 5
                )            
            lookup_s1[data["id"]] = s1
            self.cache[self.LOOKUP_S1] = lookup_s1
        return lookup_s1

    @property
    def interpolator(self):
        try:
            return self.cache[self.INTERPOLATOR]
        except KeyError:
            nodes = self.gr.nodes.subset(SUBSET_2D_OPEN_WATER)

            if self.calculation_step!="MAX":
                timeseries = nodes.timeseries(indexes=[self.calculation_step])
            else:
                timeseries = nodes.timeseries(indexes=slice(0, -1))

            data = timeseries.only("s1", "coordinates").data
            points = data["coordinates"].transpose()

            if self.calculation_step!="MAX":
                s1 = data["s1"][0]
            else:
                s1_all = data["s1"]
                s1_max_ind = s1_all.argmax(axis=0)
                s1 = np.round(
                    [row[s1_max_ind[enum]] for enum, row in enumerate(s1_all.T)], 5
                )     

            values = s1

            interpolator = LinearNDInterpolator(
                points, values, fill_value=NO_DATA_VALUE
            )
            self.cache[self.INTERPOLATOR] = interpolator
            return interpolator

    @property
    def delaunay(self):
        """
        Return a (delaunay, s1) tuple.

        `delaunay` is a qhull.Delaunay object, and `s1` is an array of
        waterlevels for the corresponding delaunay vertices.
        """
        try:
            return self.cache[self.DELAUNAY]
        except KeyError:
            nodes = self.gr.nodes.subset(SUBSET_2D_OPEN_WATER)

            if self.calculation_step!="MAX":
                timeseries = nodes.timeseries(indexes=[self.calculation_step])
            else:
                timeseries = nodes.timeseries(indexes=slice(0, -1))

            data = timeseries.only("s1", "coordinates").data
            points = data["coordinates"].transpose()

            if self.calculation_step!="MAX":
                s1 = data["s1"][0]
            else:
                s1_all = data["s1"]
                s1_max_ind = s1_all.argmax(axis=0)
                s1 = np.round(
                    [row[s1_max_ind[enum]] for enum, row in enumerate(s1_all.T)], 5
                )     

            # reorder a la lizard
            points, s1 = morton.reorder(points, s1)

            delaunay = qhull.Delaunay(points)
            self.cache[self.DELAUNAY] = delaunay, s1
            return delaunay, s1

    def _get_nodgrid(self, indices):
        """Return node grid.

        Args:
            indices (tuple): ((i1, j1), (i2, j2)) subarray indices
        """
        (i1, j1), (i2, j2) = indices

        # note that get_nodgrid() starts counting rows from the bottom
        h = self.dem_shape[0]
        i1, i2 = h - i2, h - i1

        # note that get_nodgrid() expects a columns-first bbox
        return self.gr.cells.get_nodgrid(
            [j1, i1, j2, i2], subset_name=SUBSET_2D_OPEN_WATER
        )

    def _get_points(self, indices):
        """Return points array.

        Args:
            indices (tuple): ((i1, j1), (i2, j2)) subarray indices
        """
        (i1, j1), (i2, j2) = indices
        local_ji = np.mgrid[i1:i2, j1:j2].reshape(2, -1)[::-1].transpose()
        p, a, b, q, c, d = self.dem_geo_transform
        return local_ji * [a, d] + [p + 0.5 * a, q + 0.5 * d]

    def __enter__(self):
        self.gr = GridH5ResultAdmin(self.gridadmin_path, self.results_3di_path)
        self.cache = {}
        return self

    def __exit__(self, *args):
        self.gr = None
        self.cache = None


class CopyCalculator(BaseCalculator):
    def __call__(self, indices, values, no_data_value):
        """Return input values unmodified."""
        return values


class NodGridCalculator(BaseCalculator):
    def __call__(self, indices, values, no_data_value):
        """Return node grid."""
        return self._get_nodgrid(indices)


class ConstantLevelCalculator(BaseCalculator):
    def __call__(self, indices, values, no_data_value):
        """Return waterlevel array."""
        return self.lookup_s1[self._get_nodgrid(indices)]


class LinearLevelCalculator(BaseCalculator):
    def __call__(self, indices, values, no_data_value):
        """Return waterlevel array."""
        points = self._get_points(indices)
        return self.interpolator(points).reshape(values.shape)


class LizardLevelCalculator(BaseCalculator):
    def __call__(self, indices, values, no_data_value):
        """ Return waterlevel array.

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
        nodgrid = self._get_nodgrid(indices).ravel()
        level = self.lookup_s1[nodgrid]

        # determine result raster cell centers and in which triangle they are
        points = self._get_points(indices)
        delaunay, s1 = self.delaunay
        simplices = delaunay.find_simplex(points)

        # determine which points will use interpolation
        in_gridcell = nodgrid != 0
        in_triangle = simplices != -1
        in_interpol = in_gridcell & in_triangle
        points = points[in_interpol]

        # get the nodes and the transform for the corresponding triangles
        transform = delaunay.transform[simplices[in_interpol]]
        vertices = delaunay.vertices[simplices[in_interpol]]

        # calculate weight, see print(spatial.Delaunay.transform.__doc__) and
        # Wikipedia about barycentric coordinates
        weight = np.empty(vertices.shape)
        weight[:, :2] = np.sum(
            transform[:, :2] * (points - transform[:, 2])[:, np.newaxis], 2
        )
        weight[:, 2] = 1 - weight[:, 0] - weight[:, 1]

        # set weight to zero when for inactive nodes
        nodelevel = s1[vertices]
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
        return level.reshape(values.shape)


class ConstantLevelDepthCalculator(ConstantLevelCalculator):
    def __call__(self, indices, values, no_data_value):
        """Return waterdepth array."""
        waterlevel = super().__call__(indices, values, no_data_value)
        return self._depth_from_water_level(
            dem=values, fillvalue=no_data_value, waterlevel=waterlevel
        )


class LinearLevelDepthCalculator(LinearLevelCalculator):
    def __call__(self, indices, values, no_data_value):
        """Return waterdepth array."""
        waterlevel = super().__call__(indices, values, no_data_value)
        return self._depth_from_water_level(
            dem=values, fillvalue=no_data_value, waterlevel=waterlevel
        )


class LizardLevelDepthCalculator(LizardLevelCalculator):
    def __call__(self, indices, values, no_data_value):
        """Return waterdepth array."""
        waterlevel = super().__call__(indices, values, no_data_value)
        return self._depth_from_water_level(
            dem=values, fillvalue=no_data_value, waterlevel=waterlevel
        )


class GeoTIFFConverter:
    """Convert tiff, applying a calculating function to the data.

    Args:
        source_path (str): Path to source GeoTIFF file.
        target_path (str): Path to target GeoTIFF file.
        band_count (int): Number of bands in the target file.
        progress_func: a callable.

        The progress_func will be called multiple times with values between 0.0
        amd 1.0.
    """

    def __init__(
            self,
            source_path,
            target_path,
            band_count=1,
            progress_func=None,
    ):
        self.source_path = source_path
        self.target_path = target_path
        self.band_count = band_count
        self.progress_func = progress_func

        if path.exists(self.target_path):
            raise OSError("%s already exists." % self.target_path)

    def __enter__(self):
        """Open datasets.
        """
        self.source = gdal.Open(self.source_path, gdal.GA_ReadOnly)
        block_x_size, block_y_size = self.block_size
        options = ["compress=deflate", "blockysize=%s" % block_y_size]
        if block_x_size != self.raster_x_size:
            options += ["tiled=yes", "blockxsize=%s" % block_x_size]

        self.target = gdal.GetDriverByName("gtiff").Create(
            self.target_path,
            self.raster_x_size,
            self.raster_y_size,
            self.band_count,
            self.source.GetRasterBand(1).DataType,
            options=options,
        )
        self.target.SetProjection(self.projection)
        self.target.SetGeoTransform(self.geo_transform)
        self.target.GetRasterBand(1).SetNoDataValue(self.no_data_value)

        return self

    def __exit__(self, *args):
        """Close datasets.

        """
        self.source = None
        self.target = None

    @property
    def projection(self):
        return self.source.GetProjection()

    @property
    def geo_transform(self):
        return self.source.GetGeoTransform()

    @property
    def no_data_value(self):
        return self.source.GetRasterBand(1).GetNoDataValue()

    @property
    def raster_x_size(self):
        return self.source.RasterXSize

    @property
    def raster_y_size(self):
        return self.source.RasterYSize

    @property
    def block_size(self):
        return self.source.GetRasterBand(1).GetBlockSize()

    def __len__(self):
        block_size = self.block_size
        blocks_x = -(-self.raster_x_size // block_size[0])
        blocks_y = -(-self.raster_y_size // block_size[1])
        return blocks_x * blocks_y

    def partition(self):
        """Return generator of band_no, (xoff, xsize), (yoff, ysize) values.
        """
        def offset_size_range(stop, step):
            for start in range(0, stop, step):
                yield start, min(step, stop - start)

        # make y the outer loop, tiled tiff writing is much faster row-wise...
        raster_size = self.raster_y_size, self.raster_x_size
        block_size = self.block_size[::-1]
        generator = product(*map(offset_size_range, raster_size, block_size))

        total = len(self)
        for count, (y_part, x_part) in enumerate(generator, start=1):
            # ...and in the result put x before y
            yield x_part, y_part
            if self.progress_func is not None:
                self.progress_func(count / total)

    def convert_using(self, calculator, band):
        """Convert data writing it to tiff.

        Args:
            calculator (BaseCalculator): Calculator implementation instance
            band (int): Which band to write to.
        """
        no_data_value = self.no_data_value
        for (xoff, xsize), (yoff, ysize) in self.partition():
            # read
            values = self.source.ReadAsArray(
                xoff=xoff, yoff=yoff, xsize=xsize, ysize=ysize
            )
            indices = (yoff, xoff), (yoff + ysize, xoff + xsize)

            # calculate
            result = calculator(
                indices=indices,
                values=values,
                no_data_value=no_data_value,
            )

            # write - note GDAL counts bands starting at 1
            self.target.GetRasterBand(band + 1).WriteArray(
                array=result, xoff=xoff, yoff=yoff,
            )


class NetcdfConverter(GeoTIFFConverter):
    """Convert NetCDF4 according to the CF-1.6 standards."""

    def __init__(
            self,
            source_path,
            target_path,
            gridadmin_path,
            results_3di_path,
            calculation_steps,
            **kwargs
    ):
        if netCDF4 is None:
            raise ImportError("Could not import netCDF4")
        kwargs["band_count"] = len(calculation_steps)
        super().__init__(source_path, target_path, **kwargs)

        self.gridadmin_path = gridadmin_path
        self.results_3di_path = results_3di_path
        self.calculation_steps = calculation_steps

    def __enter__(self):
        """Open datasets"""
        self.source = gdal.Open(self.source_path, gdal.GA_ReadOnly)
        self.gridadmin = GridH5ResultAdmin(
            self.gridadmin_path, self.results_3di_path
        )

        self.target = netCDF4.Dataset(self.target_path, "w", format="NETCDF4")
        self._set_lat_lon()
        self._set_time()
        self._set_meta_info()
        self._create_variable()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close datasets"""
        self.source = None
        self.gridadmin.close()
        self.target.close()

    def _set_meta_info(self):
        """Set meta info in the root group"""
        self.target.Conventions = "CF-1.6"
        self.target.institution = "3Di Waterbeheer"
        self.target.model_slug = self.gridadmin.model_slug
        self.target.result_type = "Derived water depth"
        self.target.references = "http://3di.nu"

    def _set_time(self):
        """Set time"""
        self.target.createDimension("time", self.band_count)
        time = self.target.createVariable("time", "f4", ("time",))
        time[:] = self.gridadmin.nodes.timestamps[self.calculation_steps]
        time.standard_name = "time"
        time.units = self.gridadmin.time_units.decode("utf-8")
        time.calendar = "standard"
        time.axis = "T"

    def _set_lat_lon(self):
        geotransform = self.source.GetGeoTransform()

        self.target.createDimension("lat", self.raster_y_size)
        latitudes = self.target.createVariable("lat", "f4", ("lat",))

        # In CF-1.6 the coordinates are cell centers, while GDAL interprets
        # them as the upper-left corner.
        y_upper_left = geotransform[3] + geotransform[5] / 2
        latitudes[:] = np.arange(
            y_upper_left,
            y_upper_left + geotransform[5] * self.raster_y_size,
            geotransform[5]
        )
        latitudes.standard_name = "latitude"
        latitudes.long_name = "latitude"
        latitudes.units = "degrees_north"
        latitudes.axis = "Y"

        self.target.createDimension("lon", self.raster_x_size)
        longitude = self.target.createVariable("lon", "f4", ("lon",))

        # CF 1.6 coordinates are cell center, while GDAL interprets
        # them as the upper-left corner.
        x_upper_left = geotransform[0] + geotransform[1] / 2
        longitude[:] = np.arange(
            x_upper_left,
            x_upper_left + geotransform[1] * self.raster_x_size,
            geotransform[1]
        )
        longitude.standard_name = "longitude"
        longitude.long_name = "longitude"
        longitude.units = "degree_east"
        longitude.axis = "X"

        projection = self.target.createVariable(
            "projected_coordinate_system", "i4"
        )
        projection.EPSG_code = f"EPSG:{self.gridadmin.epsg_code}"
        projection.epsg = self.gridadmin.epsg_code
        projection.long_name = "Spatial Reference"

    def _create_variable(self):
        water_depth = self.target.createVariable(
            "water_depth",
            "f4",
            ("time", "lat", "lon",),
            fill_value=-9999,
            zlib=True
        )
        water_depth.long_name = "water depth"
        water_depth.units = "m"

    def convert_using(self, calculator, band):
        """Convert data writing it to netcdf4."""

        no_data_value = self.no_data_value
        for (xoff, xsize), (yoff, ysize) in self.partition():
            # read
            values = self.source.ReadAsArray(
                xoff=xoff, yoff=yoff, xsize=xsize, ysize=ysize
            )

            # calculate
            indices = (yoff, xoff), (yoff + ysize, xoff + xsize)
            result = calculator(
                indices=indices,
                values=values,
                no_data_value=no_data_value,
            )

            # write
            water_depth = self.target['water_depth']
            water_depth[band, yoff:yoff + ysize, xoff:xoff + xsize] = result


class ProgressClass:
    """ Progress function and calculation step iterator in one.

    Args:
        calculation_steps (list(int)): Calculation steps
        progress_func: a callable.

    The main purpose is iterating over the calculation steps, but inside the
    iteration progress_class() can be passed values between 0 and 1 to record
    the partial progress for a calculation step. The supplied `progress_func`
    will be receive increasing values up to 1 for the complete iteration.
    """
    def __init__(self, calculation_steps, progress_func):
        self.progress_func = progress_func
        self.calculation_steps = calculation_steps

    def __iter__(self):
        """ Generator of (band_no, calculation_step) """
        for band_no, calculation_step in enumerate(self.calculation_steps):
            self.current = band_no
            yield band_no, calculation_step
        del self.current

    def __call__(self, progress):
        """ Progress method for the current calculation step. """
        self.progress_func(
            (self.current + progress) / len(self.calculation_steps)
        )


calculator_classes = {
    MODE_COPY: CopyCalculator,
    MODE_NODGRID: NodGridCalculator,
    MODE_CONSTANT_S1: ConstantLevelCalculator,
    MODE_LINEAR_S1: LinearLevelCalculator,
    MODE_LIZARD_S1: LizardLevelCalculator,
    MODE_CONSTANT: ConstantLevelDepthCalculator,
    MODE_LINEAR: LinearLevelDepthCalculator,
    MODE_LIZARD: LizardLevelDepthCalculator,
}

# Was def calculate_waterdepth
def calculate_waterdepth(
    gridadmin_path,
    results_3di_path,
    dem_path,
    waterdepth_path,
    calculation_steps=None,
    mode=MODE_LIZARD,
    progress_func=None,
    netcdf=False,
):
    """Calculate waterdepth and save it as GeoTIFF.

    Args:
        gridadmin_path (str): Path to gridadmin.h5 file.
        results_3di_path (str): Path to results_3di.nc file.
        dem_path (str): Path to dem.tif file.
        waterdepth_path (str): Path to waterdepth.tif file.
        calculation_steps (list(int)): Calculation step (default: [-1] (last))
        mode (str): Interpolation mode.
    """
    if calculation_steps is None:
        calculation_steps = [-1]
    if calculation_steps== "MAX":
        calculation_steps = ["MAX"]

    try:
        CalculatorClass = calculator_classes[mode]
    except KeyError:
        raise ValueError("Unknown mode: '%s'" % mode)

    # TODO remove at some point, newly produced gridadmins don't need it
    fix_gridadmin(gridadmin_path)

    progress_class = ProgressClass(
        calculation_steps=calculation_steps, progress_func=progress_func,
    )
    converter_kwargs = {
        "source_path": dem_path,
        "target_path": waterdepth_path,
        "progress_func": None if progress_func is None else progress_class,
    }
    if netcdf:
        converter_class = NetcdfConverter
        converter_kwargs['gridadmin_path'] = gridadmin_path
        converter_kwargs['results_3di_path'] = results_3di_path
        converter_kwargs['calculation_steps'] = calculation_steps
    else:
        converter_class = GeoTIFFConverter
        converter_kwargs['band_count'] = len(calculation_steps)

    with converter_class(**converter_kwargs) as converter:
        calculator_kwargs_except_step = {
            "gridadmin_path": gridadmin_path,
            "results_3di_path": results_3di_path,
            "dem_geo_transform": converter.geo_transform,
            "dem_shape": (converter.raster_y_size, converter.raster_x_size),
        }
        for band, calculation_step in progress_class:
            calculator_kwargs = {
                "calculation_step": calculation_step,
                **calculator_kwargs_except_step,
            }

            with CalculatorClass(**calculator_kwargs) as calculator:
                converter.convert_using(calculator=calculator, band=band)
