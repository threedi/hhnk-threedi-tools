from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converter import RawExportToDAMOConverter


class PeilgebiedConverter(RawExportToDAMOConverter):
    """Peilgebied-specific converter implementation."""

    def run(self):
        """Run the converter to update the peilgebied layer."""
        if self.has_executed():
            self.logger.debug("Skipping PeilgebiedConverter, already executed.")
            return

        self.logger.info("Running PeilgebiedConverter...")
        self.load_layers()  # STEP 1
        # self.update_peilgebied_layer()  # STEP 2
        self.write_outputs()  # STEP 3
        self.mark_executed()
        self.logger.info("PeilgebiedConverter run completed.")

    def load_layers(self):
        self.logger.info("Loading peilgebied-specific layers...")
        peilgebiedpraktijk = self._load_and_validate(self.raw_export_file_path, "PEILGEBIEDPRAKTIJK")
        self.data.peilgebiedpraktijk = peilgebiedpraktijk.explode(index_parts=False).reset_index(drop=True)

    def update_peilgebied_layer(self):
        self.logger.info("Updating peilgebied layer...")
        self.data._ensure_loaded(["peilgebiedpraktijk"], previous_method="load_layers")

        # TODO implement logic later
