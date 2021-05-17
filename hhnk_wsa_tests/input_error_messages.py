# These are messages displayed to users if something is wrong with their input

invalid_model_path = "Pad naar model is ongeldig {}"
no_bank_levels_1d2d_backup = "Geen 1d2d bank levelsbackup gevonden"
no_manholes_backup = "Geen manholes backup gevonden"
invalid_datachecker_path = "Pad naar datachecker is ongeldig {}"
invalid_threedi_result = "Gegeven 3di resultaat is ongeldig {}"
no_output_folder = "Geen map gespecificeerd voor opslag resultaten"
no_result_selected = "Geen 3di resultaat geselecteerd"
invalid_dem_path = "Ongeldig pad naar DEM gespecificeerd {}"

# -------------------------------------------------------------------------------------------
# Sqlite tests
# -------------------------------------------------------------------------------------------
dem_needed = invalid_dem_path + " (nodig voor {} test)"
datachecker_needed = "Ongeldig pad naar datachecker geodatabase gespecificeerd " \
                                "(nodig voor {} test)"
invalid_shapefile = "Gegeven shapefile is ongeldig (pad bestaat niet of .shx, .dbf, " \
                    ".prj bestanden ontbreken)"
channel_shape_needed_watersurface = invalid_shapefile + '\n' + "Nodig voor oppervlaktewater test"
hdb_needed_controlled_structs = "Ongeldig pad gespecificeerd voor HDB {}, " \
                                "nodig voor gestuurde kunstwerken test"
damo_needed = "Ongeldig pad gespecificeerd voor DAMO {}, " \
              "nodig voor {} test"
polder_shapefile_needed_imp_surface = invalid_shapefile + '\n' + "Nodig voor ondoorlatend oppervlak test"
no_tests_selected = "Geen tests geselecteerd om uit te voeren"

# -------------------------------------------------------------------------------------------
# Model state conversion
# -------------------------------------------------------------------------------------------
from_and_to_states_same = "Begin staat en gekozen nieuwe staat kunnen niet hetzelfde zijn"

# -------------------------------------------------------------------------------------------
# Create new project
# -------------------------------------------------------------------------------------------

invalid_character_in_filename = "Opgegeven project naam is niet valide"
folder_exists_already = "Project pad al in gebruik"

