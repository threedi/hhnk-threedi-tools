# %%

import os
from pathlib import Path

import hhnk_research_tools as hrt
import threedi_api_client
from core.schema_scenario_builder.builder import ScenarioSettings
from core.schematisation import upload
from git import Optional

logger = hrt.logging.get_logger(__name__)


# class RanaSchematisationApiService:
class RanaSchematisationApiService:
    """Service for uploading schematisations to 3Di and requesting information on revisions."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.api = upload.threedi
        self.api.set_api_key(api_key)

    @classmethod
    def from_env(cls) -> "RanaSchematisationApiService":
        """Create an instance of RanaSchematisationApiService using the THREEDI_API_KEY environment variable."""
        api_key = os.environ.get("THREEDI_API_KEY")
        if not api_key:
            raise ValueError("THREEDI_API_KEY environment variable is not set.")
        return cls(api_key=api_key)

    def upload(self, scenario_folder: Path, name: str, commit_message: str) -> None:
        """Upload scenario to 3Di."""
        pass

    def get_revision_info(self, schematisation_name: str) -> Optional[dict]:
        """Get information on the latest revision for a given schematisation name.
        Prnts the model revision information and returns the latest revision object if available, otherwise returns None.
        """
        threedimodel_response = upload.threedi.api.threedimodels_list(
            revision__schematisation__name=schematisation_name
        )

        if threedimodel_response.results == []:
            logger.info(f"No previous model(s) available for: {schematisation_name}")
            return None
        else:
            schema_id = threedimodel_response.to_dict()["results"][0]["schematisation_id"]
            latest_revision = upload.threedi.api.schematisations_latest_revision(schema_id)
            rev_model = threedimodel_response.to_dict()["results"][0]["name"]
            logger.info(
                f"model name: {rev_model}"
                f"\nschematisation_id: {schema_id}"
                f"\nCommit message: {latest_revision.commit_message}"
            )
            return latest_revision


if __name__ == "__main__":
    from tests.config import FOLDER_NEW, FOLDER_TEST

    service = RanaSchematisationApiService.from_env()

    scenarios = ScenarioSettings.from_folder(FOLDER_TEST).load_json()

    schematisation_name = scenarios["1d2d_ghg"]["schematisation_name"]
    schematisation_name = "model_test_v2__1d2d_ghg"
    info = service.get_revision_info(schematisation_name=schematisation_name)
    print(info)
