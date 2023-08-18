# %%
import sys
import shutil
import pandas as pd
import matplotlib.pyplot as plt
import os
import json
import re
import datetime
from pathlib import Path
import hhnk_research_tools as hrt
import hhnk_threedi_tools.core.schematisation.upload as upload

INFILTRATION_COLS = [
    "infiltration_rate",
    "infiltration_rate_file",
    "infiltration_surface_option",
    "max_infiltration_capacity_file",
    "display_name",
]

RASTER_FILES = [
    "dem_file",
    "frict_coef_file",
    "infiltration_rate_file",
    "max_infiltration_capacity_file",
    "initial_waterlevel_file",
]

class ModelSchematisations:
    def __init__(self, folder, modelsettings_path=None):
        self.folder = folder
        self.settings_loaded = False

        if modelsettings_path is None:
            modelsettings_path=folder.model.settings
        else:
            modelsettings_path=hrt.File(modelsettings_path)

        if modelsettings_path.exists():
            self.settings_df = pd.read_excel(modelsettings_path.base, engine="openpyxl")
            self.settings_df = self.settings_df[self.settings_df['name'].notna()]
            self.settings_df.set_index("name", drop=False, inplace=True)
            self.settings_loaded = True
        else:
            self.settings_df = None

        if self.folder.model.settings_default.exists():
            self.settings_default_series = pd.read_excel(
                self.folder.model.settings_default.base, engine="openpyxl"
            ).iloc[0]  # Series, only has one row.
        else:
            self.settings_default_series = None
            self.settings_loaded = False

            
        self.folder.model.set_modelsplitter_paths()


        if self.settings_loaded:
            self._sanity_check()


    def _sanity_check(self):
        """Sanity check settings tables"""
        inter = self.settings_df.keys().intersection(self.settings_default_series.keys())
        if len(inter) > 0:
            print(
                f"""Er staan kolommen zowel in de defaut als in de andere modelsettings.
        Dat lijkt me een slecht plan. Kolommen: {inter.values}"""
            )


    def get_latest_local_revision_str(self) -> str:
        """Str with most recent revision"""
        folder_list = [x for x in self.folder.model.revisions.path.glob("*/")]
        if folder_list == []:
            local_rev_str = f"no previous local revision for:      {self.folder.name}"
        else:
            file = max(folder_list, key=os.path.getctime)
            file_modify_date = datetime.datetime.fromtimestamp(file.lstat().st_mtime)
            local_rev_str = str(f"Most recent local revision:    {file.name} ({file_modify_date.strftime('%y-%m-%d %H:%M:%S')})" )
        return local_rev_str
    

    def get_model_revision_info(self, name, api_key):
        upload.threedi.set_api_key(api_key)
        name=name        
        row = self.settings_df.loc[name]
        schematisation = row["schematisation_name"]
        threedimodel = upload.threedi.api.threedimodels_list(revision__schematisation__name=schematisation)

        if threedimodel.results == []:
            model_revision_str = f"No previous model(s) available for: {schematisation}"
        else:
            schema_id = threedimodel.to_dict()['results'][0]['schematisation_id']
            latest_revision = upload.threedi.api.schematisations_latest_revision(schema_id)
            rev_model = threedimodel.to_dict()['results'][0]['name']
            model_revision_str = f"Most recent model revision:      {rev_model} - {latest_revision.commit_message}"
        return model_revision_str


    def create_schematisation(self, name):
        """Create a schematisation based on the modelsettings.
        Some schematisations (0d1d_test) have some extra changed that are not
        only the globalsettings"""
        row = self.settings_df.loc[name].copy()

        # schema_name = self.folder.model._add_modelpath(name)

        # Copy the files that are in the global settings.
        # This menas rasters that are not defined are not added to the schematisation.
        schema_base = self.folder.model.schema_base
        schema_new = getattr(self.folder.model, f"schema_{name}")
        schema_new.create()
        
        database_base = schema_base.database

        # Write the sqlite and rasters to new folders.

        # Copy sqlite
        dst = schema_new.full_path(database_base.name)
        shutil.copyfile(src=database_base.base, dst=dst.base)

        #If database_new is defined before sqlite exists, it will not work properly
        database_new = schema_new.database 

        schema_new.rasters.create(parents=False)
        # Copy rasters that are defined in the settings file
        for raster_file in RASTER_FILES:
            if not pd.isnull(row[raster_file]):
                src = schema_base.full_path(row[raster_file])
                if src.exists():
                    dst = schema_new.full_path(row[raster_file])
                    shutil.copyfile(src=src.base, dst=dst.base)
                else:
                    print(f"Couldnt find     raster:\t{row[raster_file]}")
                    raise TypeError(f"No {raster_file} in base schematisation")

        # Edit the SQLITE
        table_names = ["v2_global_settings", "v2_simple_infiltration"]
        for table_name in table_names:
            print(f"\tUpdate {table_name}")
            # Set the id in the v2_simple_iniltration to the id defined in global settings.
            if table_name == "v2_simple_infiltration":
                row["id"] = row["simple_infiltration_settings_id"]
            else:
                row["id"] = 1

            # Clear the table
            database_new.execute_sql_changes(query=f"""DELETE FROM {table_name}""")
            # hrt.execute_sql_changes(
            #     query=f"""DELETE FROM {table_name}""", database=database_path_new
            # )

            # Create new value and column pairs. The new values are used from the settings.xlsx file.
            # Dont create v2_simple infiltration if the id is not defined
            if not pd.isnull(row["id"]):

                df_table = database_new.read_table(table_name=table_name)
                # df_table = hrt.sqlite_table_to_df(
                #     database_path=database_path_new, table_name=table_name
                # )
                columns = []
                values = []
                for key in df_table.keys():
                    columns.append(key)
                    if key in row:
                        value = row[key]
                    elif key in self.settings_default_series:
                        value = self.settings_default_series[key]
                    else:
                        value = None
                        print(f"Column {key} not defined")
                    if pd.isnull(value):
                        value = None

                    # Exceptions
                    # startdate is interpreted as timestamp by pandas but we only need YYYY-MM-DD format.
                    if key == "start_date":
                        try:
                            value = str(value)[:10]
                        except:
                            pass
                    values.append(value)

                # Make sure None is interpreted as NULL by sqlite.
                columns = tuple(columns)
                values = str(tuple(values)).replace("None", "NULL")

                # Prepare insert query
                query = f"""INSERT INTO {table_name} {columns}
                VALUES {values}"""

                # Insert new row
                database_new.execute_sql_changes(query=query)
                # hrt.execute_sql_changes(query, database=database_path_new)

        # Additional model changes for different model types
        if name == "0d1d_test":
            # Set every channel to isolated
            database_new.execute_sql_changes(query="UPDATE v2_channel SET calculation_type=101")
            # hrt.execute_sql_changes(
            #     query="UPDATE v2_channel SET calculation_type=101",
            #     database=database_path_new,
            # )

            # Set controlled weirs to 10x width because we dont use controlled strcutures in hyd test.
            # To get the weir with we use the base database, so we cant accidentally run this twice.
            controlled_weirs_selection_query = f"""
                SELECT
                v2_weir.cross_section_definition_id as cross_def_id,
                v2_weir.code as weir_code,
                v2_weir.id as id,
                v2_cross_section_definition.width as width
                FROM v2_weir
                INNER JOIN v2_cross_section_definition ON v2_weir.cross_section_definition_id = v2_cross_section_definition.id
                INNER JOIN v2_control_table ON v2_weir.id = v2_control_table.target_id
                """
            controlled_weirs_df = database_base.execute_sql_selection(query=controlled_weirs_selection_query)
            # controlled_weirs_df = hrt.execute_sql_selection(
            #     controlled_weirs_selection_query, database_path=database_path_base
            # )

            controlled_weirs_df.insert(
                controlled_weirs_df.columns.get_loc("width") + 1,
                "width_new",
                controlled_weirs_df["width"].apply(lambda x: round((float(x) * 10), 3)),
            )

            query = hrt.sql_create_update_case_statement(
                df=controlled_weirs_df,
                layer="v2_cross_section_definition",
                df_id_col="cross_def_id",
                db_id_col="id",
                old_val_col="width",
                new_val_col="width_new",
            )

            database_new.execute_sql_changes(query=query)
            # hrt.execute_sql_changes(query=query, database=database_path_new)

        #execute additional SQL code that is stored in 02_schematisation/model_sql.json.
        if self.folder.model.model_sql.exists():
            model_sql = self.folder.model.model_sql.read_json()

            if name in model_sql.keys():
                for _, query in model_sql[name].items():
                    database_new.execute_sql_changes(query=query)
                    print(f"executed additional query on {name}: {query}")


    def upload_schematisation(self, name, commit_message, api_key, organisation_uuid):             
        """
        possible raster_names
        [ dem_file, equilibrium_infiltration_rate_file, frict_coef_file,
        initial_groundwater_level_file, initial_waterlevel_file, groundwater_hydro_connectivity_file,
        groundwater_impervious_layer_level_file, infiltration_decay_period_file, initial_infiltration_rate_file,
        leakage_file, phreatic_storage_capacity_file, hydraulic_conductivity_file, porosity_file, infiltration_rate_file,
        max_infiltration_capacity_file, interception_file ]
        """
    
        schema_new = getattr(self.folder.model, f"schema_{name}")
        row = self.settings_df.loc[name]

        upload.threedi.set_api_key(api_key)

        raster_names = {
            "dem_file": schema_new.rasters.dem.path_if_exists,
            "frict_coef_file": schema_new.rasters.friction.path_if_exists,
            "infiltration_rate_file": schema_new.rasters.infiltration.path_if_exists,
            "max_infiltration_capacity_file": schema_new.rasters.storage.path_if_exists,
            "initial_waterlevel_file": schema_new.rasters.initial_wlvl_2d.path_if_exists,
        }

        sqlite_path = schema_new.database.base
        schematisation_name = row["schematisation_name"]
        tags = [schematisation_name, self.folder.name]
        # organisation_uuid="48dac75bef8a42ebbb52e8f89bbdb9f2"

        upload.upload_and_process(
            schematisation_name=schematisation_name,
            organisation_uuid=organisation_uuid,
            sqlite_path=sqlite_path,
            raster_paths=raster_names,
            schematisation_create_tags=tags,
            commit_message=commit_message,
        )


    def create_local_sqlite_revision(self, commit_message):
        """Create local revision of sqlite. Will both be made when splitting and 
        uploading a model through the plugin"""
        commit_message = re.sub('[^a-zA-Z0-9() \n\.]', '', commit_message)

        if len(self.folder.model.schema_base.sqlite_names) != 1:
            return print("0 or >1 .sqlite file in 00_basis")
        
        if len(self.folder.model.schema_base.sqlite_names) == 1:
            rev_count = len(self.folder.model.revisions.content)
            sqlite_path = self.folder.model.schema_base.sqlite_paths[0]
            mod_settings_file = self.folder.model.settings.path_if_exists 
            mod_settings_default = self.folder.model.settings_default.path_if_exists 
            model_sql = self.folder.model.model_sql.path_if_exists 

            target_path = self.folder.model.revisions.full_path(f"rev_{rev_count+1} - {commit_message[:25]}")
            target_path.create()

            for f in [sqlite_path, mod_settings_file, mod_settings_default, model_sql]:
                try:
                    shutil.copy(f, target_path.base)
                except:
                    print(f"{f} not found")

            return print(f"succes copy {Path(sqlite_path).name} + {Path(mod_settings_file).name} + {Path(mod_settings_default).name} + {Path(model_sql).name} to: {target_path.base}")

       
        
# %%

if __name__ == "__main__":
    from hhnk_threedi_tools.core.folders import Folders

    path = r"E:\02.modellen\LangeWeere_NS"

    folder = Folders(path)
    name = "1d2d_glg"

    self = ModelSchematisations(
        folder=folder, modelsettings_path=folder.model.settings.path
    )
    self.create_schematisation(name=name)
    self.upload_schematisation(
        organisation_uuid="48dac75bef8a42ebbb52e8f89bbdb9f2",
        name=name,
        commit_message="testtest",
        api_key="",
    )


# %%
