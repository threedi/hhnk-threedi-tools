import geopandas as gpd
from ...sql_interaction.sql_functions import execute_sql_selection
from ...variables.definitions import ESRI_DRIVER
from ...queries.tests.sqlite_tests.quick_tests_selection_queries import impervious_surface_query
from ...variables.database_variables import id_col

def calc_surfaces_diff(db_imp_surface, polygon_imp_surface):
    db_surface = int(db_imp_surface.sum() / 10000)
    polygon_surface = int(polygon_imp_surface.area.values[0] / 10000)
    area_diff = db_surface - polygon_surface
    return db_surface, polygon_surface, area_diff

def check_imp_surface_area(test_env):
    polder_shapefile = test_env.src_paths['polder_shapefile']
    model_path = test_env.src_paths['model']
    try:
        imp_surface_db = execute_sql_selection(query=impervious_surface_query,
                                               database_path=model_path,
                                               index_col=id_col) #conn=test_params.conn, index_col=DEFAULT_INDEX_COLUMN
        polygon_imp_surface = gpd.read_file(polder_shapefile, driver=ESRI_DRIVER) #read_file(polder_file, driver=ESRI_DRIVER)
        db_surface, polygon_surface, area_diff = calc_surfaces_diff(imp_surface_db, polygon_imp_surface)
        result_txt = f"Totaal ondoorlatend oppervlak: {db_surface} ha\n" \
                     f"Gebied polder: {polygon_surface} ha\n" \
                     f"Verschil: {area_diff} ha\n"
        return result_txt
    except Exception as e:
        raise e from None

