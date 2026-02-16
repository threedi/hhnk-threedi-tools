# %%


# TODO place here
# class RanaSchematisationApiService:
class RanaSchematisationApiService:
    """Service for uploading schematisations to 3Di and requesting information on revisions."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.api = upload.threedi
        self.api.set_api_key(api_key)

    def upload(self, scenario_folder: Path, name: str, commit_message: str) -> None:
        """Upload scenario to 3Di."""
        pass

    def get_revision_info(self, name: str) -> str:
        return ""
