from shapely.geometry import LineString

def line_geometries_to_coords(lines):
    """
    Coordinates read from threedi results netcdf can't be used as is in geodataframe
    Usage: lines = results.lines.channels.line_geometries where results = GridH5ResultAdmin object
    """
    coords = []
    for line in lines:
        if len(line) >= 4:
            x_coords = line[:int(line.size / 2)].tolist()
            y_coords = line[int(line.size / 2):].tolist()
        else:
            # Fill in dummy coords
            x_coords = [0.0, 25000]
            y_coords = [0.0, 25000]
        line_list = []
        for x, y in zip(x_coords, y_coords):
            line_list.append([x, y])
        # Creates set of ([x1, y1], [x2, y2] ...., [xn, yn])
        coords.append(LineString(line_list))
    return coords
