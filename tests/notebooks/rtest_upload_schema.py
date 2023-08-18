# %%
import hhnk_research_tools as hrt

from hhnk_threedi_tools.utils.notebooks.notebook_setup import setup_notebook
notebook_data = setup_notebook()

import hhnk_threedi_tools.core.api.upload_model.upload as upload


api_keys = hrt.read_api_file(notebook_data["api_keys_path"])
upload.threedi.set_api_key(api_key=api_keys['threedi'])


# %% Test model creation
#Delete the model manually first before creating..
def test_create_threedimodel(schematisation_pk, revision_id):
    revision = hrt.call_threedi_api(func=upload.threedi.api.schematisations_revisions_read, 
                                            id=revision_id, schematisation_pk=schematisation_pk)
    schematisation = hrt.call_threedi_api(func=upload.threedi.api.schematisations_read, 
                                        id=schematisation_pk)
    upload.create_threedimodel(revision=revision, schematisation=schematisation)


if __name__ == "__main__":
    schematisation_pk = 5746
    revision_id = 51153
    test_create_threedimodel(schematisation_pk, revision_id)

# %%
threedimodels = hrt.call_threedi_api(func=upload.threedi.api.threedimodels_list, 
                                            revision__schematisation__name="model_test_v2__0d1d_test")