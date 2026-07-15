from shapely.ops import nearest_points
def nearest_intersect(boundary_geom,line_geom,point_geom):
    """
    Find intersection between line and boundary that is closest 
    to given point.
    """

    if boundary_geom.intersects(line_geom):
        intersection = nearest_points(
                point_geom,
                boundary_geom.intersection(line_geom)
        )[1]
        nearest_dist = intersection.distance(point_geom)
    else:
        intersection = None
        nearest_dist = None
    
    return intersection, nearest_dist



def nearest_intersect_exclude(boundary_geom,line_geom,point_geom,exclude_point):
    """
    Find intersection between line and boundary that is closest 
    to given point, but not the exclude point.
    """
    intersection = None
    nearest_dist = None
    valid_points = []
    valid_dists = []

    if exclude_point is None:
              
        return intersection, nearest_dist

    elif boundary_geom is not None:
        if boundary_geom.intersects(line_geom):
            intersections = boundary_geom.intersection(line_geom)
            if intersections.geom_type == 'MultiPoint':
                for p in intersections.geoms:
                    dist = p.distance(exclude_point)
                    if dist > 0.01:
                        valid_points.append(p)
                        valid_dists.append(dist)
                if len(valid_dists) > 0: 
                    nearest_dist = min(valid_dists)
                    for p in intersections.geoms:
                        dist = p.distance(exclude_point)
                        if dist == nearest_dist: # kan ook met .index?
                            intersection = p         
            elif intersections.geom_type == 'Point':
                intersection = intersections
                nearest_dist = intersections.distance(point_geom)

    return intersection, nearest_dist

from shapely.geometry import LineString 
from math import sin,cos,atan2

def perpendicular_lines(p1,p2,distance,side):
  '''
  Takes two points and calculates points perpendicular  to first point and draws line between them.

  Parameters
  ----------
  p1 = point 1 (x,y in list)

  p2 = point 2 (x,y in list)

  distance = distance new points from p1

  side = 'left' or 'right' 

  Returns
  -------
  Perpendicular line to first point (LineString)
  '''
  
  # # shifted coordinate gives None error
  # if p2 is None:
  #   perp_line = None
  # Delta x
  dx = p2[0] - p1[0]
  # Delta y
  dy = p2[1] - p1[1]
  # Angle between p1 and p2 in rad
  angle = atan2(dy, dx)
  # Displacement of points
  lx = sin(angle) * distance
  ly = cos(angle) * distance

  if side == 'right':
    perp_coords = [p1[0] - lx, p1[1] + ly]
  if side == 'left':
    perp_coords = [p1[0] + lx, p1[1] - ly]
  
  perp_line = LineString([p1, perp_coords])
  
  return perp_line


import numpy as np
def get_nearest_non_nodata(rioxarray_raster,point_gdf,tolerance=0.5,pixelsize=0.5,mode='min'):
  """
  Finds nearest (pixel interval) non-nodata pixel within tolerance.
  Tries at exact location first (within pixelsize), then searches further.
  If multiple pixels at same distance 'mode' desides.

  With high tolerance a lot of overhead as .sel is perfomed on all valid pixels
  whitin each while loop. Could be improved by somehow storing known values in 
  dataframe, but for small tolerance this is quicker.

  Output is converted to numpy.float
  """
  dist = 0
  pixelsize = 0.5
  x = point_gdf.x
  y = point_gdf.y
  result = rioxarray_raster.sel(x=x,y=y, method='nearest',tolerance=pixelsize).to_numpy()[0]
  x_set = {x}
  y_set = {y}
  while np.isnan(result) and dist < tolerance:
    dist = dist + pixelsize
    x_set.add(x-dist)
    x_set.add(x+dist)
    y_set.add(y-dist)
    y_set.add(y+dist)

    data = []
    for i in x_set:
      for j in y_set:
        data.append(rioxarray_raster.sel(x=i,y=j, method='nearest',tolerance=pixelsize).to_numpy()[0])
    
    if np.isnan(data).all():
       result = np.nan
    elif mode == 'min':
       result = np.nanmin(data)
    elif mode == 'max':
       result = np.nanmax(data)
    elif mode == 'med':
       result = np.nanmedian(data)
    elif mode == 'mean':
       result = np.nanmean(data)

  return result


import numpy as np
def get_min_value_in_polygon(rioxarray_raster, polygon,mode='min'):
    """
    Finds non-nodata value within polygon.
    If multiple pixels whotin polygon 'mode' desides.

    Basic zonal statistics without rasterstats module.

    Not as quick or accurate as `get_nearest_non_nodata`
    """
    # Mask the raster with the polygon
    masked_rioxarray_raster = rioxarray_raster.rio.clip([polygon], rioxarray_raster.rio.crs)
    # Get the data array
    data = masked_rioxarray_raster.data
    # Mask the data array to ignore nodata values
    data = np.ma.masked_equal(data, rioxarray_raster.rio.nodata)
    # Return the desired value
    if mode == 'min':
        result = np.nanmin(data)    
    elif mode == 'max':
       result = np.nanmax(data)
    elif mode == 'med':
       result = np.nanmedian(data)
    elif mode == 'mean':
       result = np.nanmean(data)
    return result