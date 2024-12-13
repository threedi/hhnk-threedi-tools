"""Function to convert Hydroobjecten from DAMO to HyDAMO"""
from pathlib import Path
import datetime
## TODO temporary way of running sub Github modules, before releasing tools
import sys
sys.path.append("D:/github/evanderlaan/GIS-2-HyDAMO/refactor")

from reader import Reader
from converter import Converter
from attributefilterer import AttributeFilterer

from connector import Connector
from writer import Writer


FOLDER =  Path("D:/github/evanderlaan/hhnk-threedi-tools/hhnk_threedi_tools/core/schematisation_builder")
now = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
results_directory = log_file = FOLDER / 'output_hydamoconverter' / now
log_file = results_directory / 'log.txt'

reader = Reader(
	config_files_directory = FOLDER / 'input'/'config_files', 
	DAMO_version = '2.3',
)

converter = Converter(
	geodata_bundles = reader.geodata_bundles,
	fillna_with_default_values = True,
)


connector = Connector(
	mapped_geodataframes = converter.mapped_geodataframes,
	missing_value_tracking_geodataframes = converter.missing_value_tracking_geodataframes,
)

writer = Writer(
	mapped_geodataframes = connector.mapped_geodataframes,
	missing_value_tracking_geodataframes = connector.missing_value_tracking_geodataframes,
	profiellijnen_with_multiple_hydroobject_intersections_gdf = spatialfilterer.profiellijnen_with_multiple_hydroobject_intersections_gdf,
	results_directory = results_directory,
	DAMO_version = '2.3',
)