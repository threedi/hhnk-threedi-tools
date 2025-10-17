from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converter import RawExportToDAMOConverter


class PeilgebiedConverter(RawExportToDAMOConverter):
    """Peilgebied-specific converter implementation."""

    def __init__(self, raw_export_converter: RawExportToDAMOConverter):
        self.data = raw_export_converter.data
        self.logger = raw_export_converter.logger

    def run(self):
        """Run the converter to update the peilgebied layer."""
        if self.has_executed():
            self.logger.debug("Skipping PeilgebiedConverter, already executed.")
            return

        self.logger.info("Running PeilgebiedConverter...")
        self.update_peilgebied_layer()
        self.mark_executed()
        self.logger.info("PeilgebiedConverter run completed.")

    def update_peilgebied_layer(self):
        self.logger.info("Updating peilgebied layer...")
        # self.data._ensure_loaded(["peilgebiedpraktijk"], previous_method="load_layers")
        self.logger.warning("Update peilgebied layer not implemented yet")
        # TODO implement logic later
