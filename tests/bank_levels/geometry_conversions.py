from shapely.geometry import Point
from ...variables.database_aliases import df_geo_col

def extract_boundary_from_polygon(polygon):
    """Extract the boundaries from a multipolygon. This way the intersection with the boundaries can be found."""
    try:
        lines_gdf = polygon.explode()
        lines_gdf[df_geo_col] = lines_gdf[df_geo_col].boundary
    except Exception as e:
        raise e from None
    return lines_gdf

def point_geometries_to_wkt(points):
    coords = []
    for point_x, point_y in zip(points[0], points[1]):
        coords.append(Point([point_x, point_y]))
    return coords
