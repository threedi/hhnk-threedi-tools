import gdal

#   Output file types: to prevent typo's and in case of remapping
TIF = 'tif'
CSV = 'csv'
QML = 'qml'
TEXT = 'txt'
SHAPE = 'shp'
SQL = 'sql'
GEOTIFF = 'GTiff'
GPKG = 'gpkg'
H5 = 'h5'
NC = 'nc'
SQLITE = 'sqlite'
GDB = 'gdb'
SHX = 'shx'
DBF = 'dbf'
PRJ = 'prj'
GDAL_DATATYPE = gdal.GDT_Float32
file_types_dict = {'csv': '.csv',
                   'txt': '.txt',
                   'shp': '.shp',
                   'sql': '.sql',
                   'sqlite': '.sqlite',
                   'tif': '.tif',
                   'gdb': '.gdb',
                   'gpkg': '.gpkg',
                   'qml': '.qml',
                   'h5': '.h5',
                   'nc': '.nc',
                   'shx': '.shx',
                   'dbf': '.dbf',
                   'prj': '.prj'}
UTF8 = 'utf-8'

