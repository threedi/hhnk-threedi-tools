"""
exceptions.py
-------------
Custom exceptions for the submodels module.
"""


class GeoPackageFileNotFoundError(Exception):
    """No .gpkg file found in the schematisation directory."""


class SQLiteFileNotFoundError(Exception):
    """No .sqlite file found in the schematisation directory."""


class RastersDirectoryNotFoundError(Exception):
    """No 'rasters' sub-folder found in the schematisation directory."""


class SchematisationFileNotFoundError(Exception):
    """A required input file does not exist."""


class LayerNotFoundError(Exception):
    """A required layer is missing from a GeoPackage."""


class SubareaLayerEmptyError(Exception):
    """The sub-areas layer contains no features."""


class FieldNameNotFoundError(Exception):
    """The requested field name is not present in the sub-areas layer."""


class SubareaNamesNotUniqueError(Exception):
    """Sub-area names in the field column are not unique."""


class NoCalcGridCellsSelectedError(Exception):
    """No calculation-grid cells intersect a given sub-area."""
