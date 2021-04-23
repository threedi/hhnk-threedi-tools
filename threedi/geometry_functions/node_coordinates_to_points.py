import numpy as np
from shapely.geometry import Point

def coordinates_to_points(nodes):
    res_crds = nodes.coordinates
    crds_lst = np.vstack(res_crds.T)
    # convert to shapely format so we can create a geodataframe
    crds = [Point(crd) for crd in crds_lst]
    return crds
