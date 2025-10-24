from pathlib import Path
from typing import Any

import fiona
import ipywidgets as widgets
from ipywidgets import HBox, VBox, Button, ToggleButtons, Checkbox, Text, HTML
from IPython.display import display, clear_output

from hhnk_threedi_tools.core.vergelijkingstool.utils import get_model_info
from hhnk_threedi_tools.core.vergelijkingstool import config, main


class VergelijkingstoolGUI:
    def __init__(self):
        #  Styling 
        desc_style = {"description_width": "160px"}  
        text_w = "95%"

        self.output_box = widgets.Output()

        # Model folder
        self.model_base_path_input = Text(
            value="",
            placeholder=r"e.g. E:\02.modellen\castricum",
            description="Model folder:",
            layout=widgets.Layout(width=text_w),
            style=desc_style,
        )

        # Output widgets
        self.output_folder_text = Text(
            value="",
            description="Output folder:",
            disabled=True,
            layout=widgets.Layout(width=text_w),
            style=desc_style,
        )
        self.output_name_input = Text(
            value=".gpkg",
            description="Output name:",
            layout=widgets.Layout(width=text_w),
            style=desc_style,
        )
        self.output_file_path = Text(
            value="",
            description="Full path:",
            disabled=True,
            layout=widgets.Layout(width=text_w),
            style=desc_style,
        )

        # Compare options
        self.compare_title = HTML("<b>Which database do you want to compare with?</b>")
        self.compare_buttons = ToggleButtons(
            options=["Compare with Damo", "Compare with 3Di", "Both"],
            value=None,
            layout=widgets.Layout(width="auto"),
        )

        # Checkboxes 
        self.select_layer_damo = Checkbox(
            value=False,
            description="Select specific DAMO/DAMO layers?",
            indent=False,
        )
        self.select_layer_3di = Checkbox(
            value=False,
            description="Select specific 3Di/DAMO layers?",
            indent=False,
        )

        # run button
        self.run_button = Button(description="Run Comparison", button_style="success")

        # Layout 
        output_section = VBox(
            [
                HTML("<b>Output configuration</b>"),
                self.output_folder_text,
                self.output_name_input,
                self.output_file_path,
            ]
        )
        self.main_box = VBox(
            [
                self.model_base_path_input,
                self.compare_title,
                self.compare_buttons,
                output_section,
                self.output_box, 
            ]
        )
        display(self.main_box)

        # Wire events
        self.model_base_path_input.observe(self._update_output_folder, names="value")
        self.output_name_input.observe(self._update_output_file_path, names="value")
        self.compare_buttons.observe(self._on_compare_change, names="value")

        # check if the checkbox is changed
        self.select_layer_damo.observe(self._on_damo_checkbox_change, names="value")
        self.select_layer_3di.observe(self._on_3di_checkbox_change, names="value")

        # Inits
        self._update_output_folder({"new": self.model_base_path_input.value})
        self._update_output_file_path()

    #  helpers 
    def _safe_get_model_info(self):
        base = self.model_base_path_input.value.strip()
        if not base:
            return None
        try:
            return get_model_info(base)
        except Exception:
            return None

    def _update_output_folder(self, change: Any):
        mi = self._safe_get_model_info()
        self.output_folder_text.value = str(mi.output_folder) if mi else "model could not be read"
        self._update_output_file_path()

    def _update_output_file_path(self, change: Any = None):
        try:
            folder = Path(self.output_folder_text.value)
            name = self.output_name_input.value or "comparison_output.gpkg"
            self.output_file_path.value = str(folder / name)
        except Exception:
            self.output_file_path.value = ""

    #  UI logic 
    def _on_compare_change(self, change: Any):
        """deploy bottons"""
        with self.output_box:
            clear_output()
            val = change["new"]

            # Limpia handlers viejos del botón
            self.run_button._click_handlers.callbacks = []

            if val == "Compare with Damo":
                display(self.select_layer_damo, self.run_button)
                # Handler por defecto (todas las capas)
                self.run_button.on_click(self._run_damo_all)

            elif val == "Compare with 3Di":
                display(self.select_layer_3di, self.run_button)
                self.run_button.on_click(self._run_3di_all)

            elif val == "Both":
                # Both no muestra checkboxes; siempre corre todo
                display(HTML("<i>Both comparisons will run using all layers.</i>"), self.run_button)
                self.run_button.on_click(self._run_both_all)

            else:
                # Nada seleccionado
                pass

    #  DAMO branch 
    def _on_damo_checkbox_change(self, change: Any):
        """Show llist of layers only if the checkbox is activated"""
        if self.compare_buttons.value != "Compare with Damo":
            return

        with self.output_box:
            clear_output()
            display(self.select_layer_damo, self.run_button)

            # Limpia y vuelve a poner handler por defecto
            self.run_button._click_handlers.callbacks = []
            self.run_button.on_click(self._run_damo_all)

            if not change["new"]:
                # Si desmarcan, no mostramos selección de capas
                return

            # Mostrar selección de capas
            mi = self._safe_get_model_info()
            if not mi:
                print("Enter a valid model folder first.")
                return

            damo_layers = fiona.listlayers(mi.fn_damo_new)
            hdb_layers = fiona.listlayers(mi.fn_hdb_new)

            self._damo_buttons = [widgets.ToggleButton(description=l, layout=widgets.Layout(width="48%")) for l in damo_layers]
            self._hdb_buttons = [widgets.ToggleButton(description=l, layout=widgets.Layout(width="48%")) for l in hdb_layers]

            def two_rows(buttons):
                half = len(buttons) // 2
                return VBox(
                    [
                        HBox(buttons[:half], layout=widgets.Layout(flex_flow="row wrap", gap="6px")),
                        HBox(buttons[half:], layout=widgets.Layout(flex_flow="row wrap", gap="6px")),
                    ]
                )

            display(HTML("<b>DAMO layers:</b>"), two_rows(self._damo_buttons))
            display(HTML("<b>HDB layers:</b>"), two_rows(self._hdb_buttons))

            # Reemplaza handler: ahora corre con selección
            self.run_button._click_handlers.callbacks = []
            self.run_button.on_click(self._run_damo_selected)

    def _run_damo_all(self, _):
        with self.output_box:
            clear_output()
            mi = self._safe_get_model_info()
            if not mi:
                print("Enter a valid model folder.")
                return
            print("Comparing DAMO/DAMO using ALL layers...")
            main.main(
                model_info=mi,
                fn_DAMO_selection=mi.damo_selection,
                fn_damo_new=mi.fn_damo_new,
                fn_hdb_new=mi.fn_hdb_new,
                fn_damo_old=mi.fn_damo_old,
                fn_hdb_old=mi.fn_hdb_old,
                fn_threedimodel=mi.fn_threedimodel,
                fn_DAMO_comparison_export=self.output_file_path.value,
                fn_threedi_comparison_export=self.output_file_path.value,
                compare_with="Compare with Damo",
                layer_selection=False,
                layers_input_hdb_selection=[],
                layers_input_damo_selection=fiona.listlayers(mi.fn_damo_new),
                threedi_layer_selector=False,
                threedi_structure_selection=[],
                damo_structure_selection=[],
                structure_codes=[],
            )
            print("Finished.")

    def _run_damo_selected(self, _):
        with self.output_box:
            clear_output()
            mi = self._safe_get_model_info()
            if not mi:
                print("Enter a valid model folder.")
                return
            sel_damo = [b.description for b in getattr(self, "_damo_buttons", []) if b.value]
            sel_hdb = [b.description for b in getattr(self, "_hdb_buttons", []) if b.value]
            print(f"DAMO: {sel_damo}\nHDB: {sel_hdb}\n Running...")
            main.main(
                model_info=mi,
                fn_DAMO_selection=mi.damo_selection,
                fn_damo_new=mi.fn_damo_new,
                fn_hdb_new=mi.fn_hdb_new,
                fn_damo_old=mi.fn_damo_old,
                fn_hdb_old=mi.fn_hdb_old,
                fn_threedimodel=mi.fn_threedimodel,
                fn_DAMO_comparison_export=self.output_file_path.value,
                fn_threedi_comparison_export=self.output_file_path.value,
                compare_with="Compare with Damo",
                layer_selection=True,
                layers_input_hdb_selection=sel_hdb,
                layers_input_damo_selection=sel_damo,
                threedi_layer_selector=False,
                threedi_structure_selection=[],
                damo_structure_selection=[],
                structure_codes=[],
            )
            print("Finished.")

    #  3Di branch 
    def _on_3di_checkbox_change(self, change: Any):
        if self.compare_buttons.value != "Compare with 3Di":
            return

        with self.output_box:
            clear_output()
            display(self.select_layer_3di, self.run_button)

            self.run_button._click_handlers.callbacks = []
            self.run_button.on_click(self._run_3di_all)

            if not change["new"]:
                return

            mi = self._safe_get_model_info()
            if not mi:
                print("Enter a valid model folder first.")
                return

            threedi_layers = config.THREEDI_STRUCTURE_LAYERS
            damo_layers = config.DAMO_HDB_STRUCTURE_LAYERS
            codes = config.STRUCTURE_CODES

            self._threedi_buttons = [widgets.ToggleButton(description=l, layout=widgets.Layout(width="48%")) for l in threedi_layers]
            self._damo_struct_buttons = [widgets.ToggleButton(description=l, layout=widgets.Layout(width="48%")) for l in damo_layers]
            self._code_buttons = [widgets.ToggleButton(description=c, layout=widgets.Layout(width="48%")) for c in codes]

            def two_rows(buttons):
                half = len(buttons) // 2
                return VBox(
                    [
                        HBox(buttons[:half], layout=widgets.Layout(flex_flow="row wrap", gap="6px")),
                        HBox(buttons[half:], layout=widgets.Layout(flex_flow="row wrap", gap="6px")),
                    ]
                )

            display(HTML("<b>3Di structure layers:</b>"), HBox(self._threedi_buttons, layout=widgets.Layout(flex_flow="row wrap", gap="6px")))
            display(HTML("<b>DAMO/HDB layers:</b>"), two_rows(self._damo_struct_buttons))
            display(HTML("<b>Structure codes:</b>"), HBox(self._code_buttons, layout=widgets.Layout(flex_flow="row wrap", gap="6px")))

            self.run_button._click_handlers.callbacks = []
            self.run_button.on_click(self._run_3di_selected)

    def _run_3di_all(self, _):
        with self.output_box:
            clear_output()
            mi = self._safe_get_model_info()
            if not mi:
                print("Enter a valid model folder.")
                return
            print("Comparing 3Di/DAMO using ALL structures...")
            main.main(
                model_info=mi,
                fn_DAMO_selection=mi.damo_selection,
                fn_damo_new=mi.fn_damo_new,
                fn_hdb_new=mi.fn_hdb_new,
                fn_damo_old=mi.fn_damo_old,
                fn_hdb_old=mi.fn_hdb_old,
                fn_threedimodel=mi.fn_threedimodel,
                fn_DAMO_comparison_export=self.output_file_path.value,
                fn_threedi_comparison_export=self.output_file_path.value,
                compare_with="Compare with 3Di",
                layer_selection=False,
                layers_input_hdb_selection=[],
                layers_input_damo_selection=[],
                threedi_layer_selector=True,
                threedi_structure_selection=config.THREEDI_STRUCTURE_LAYERS,
                damo_structure_selection=config.DAMO_HDB_STRUCTURE_LAYERS,
                structure_codes=config.STRUCTURE_CODES,
            )
            print("Finished.")

    def _run_3di_selected(self, _):
        with self.output_box:
            clear_output()
            mi = self._safe_get_model_info()
            if not mi:
                print("Enter a valid model folder.")
                return
            sel_3di = [b.description for b in getattr(self, "_threedi_buttons", []) if b.value]
            sel_damo_struct = [b.description for b in getattr(self, "_damo_struct_buttons", []) if b.value]
            sel_codes = [b.description for b in getattr(self, "_code_buttons", []) if b.value]
            print(f"3Di: {sel_3di}\nDAMO/HDB: {sel_damo_struct}\nCodes: {sel_codes}\n⚙️ Running...")
            main.main(
                model_info=mi,
                fn_DAMO_selection=mi.damo_selection,
                fn_damo_new=mi.fn_damo_new,
                fn_hdb_new=mi.fn_hdb_new,
                fn_damo_old=mi.fn_damo_old,
                fn_hdb_old=mi.fn_hdb_old,
                fn_threedimodel=mi.fn_threedimodel,
                fn_DAMO_comparison_export=self.output_file_path.value,
                fn_threedi_comparison_export=self.output_file_path.value,
                compare_with="Compare with 3Di",
                layer_selection=False,
                layers_input_hdb_selection=[],
                layers_input_damo_selection=[],
                threedi_layer_selector=True,
                threedi_structure_selection=sel_3di,
                damo_structure_selection=sel_damo_struct,
                structure_codes=sel_codes,
            )
            print("Finished.")

    #  Both 
    def _run_both_all(self, _):
        with self.output_box:
            clear_output()
            mi = self._safe_get_model_info()
            if not mi:
                print("Enter a valid model folder.")
                return
            print("Running BOTH (all layers)...")
            # DAMO
            main.main(
                model_info=mi,
                fn_DAMO_selection=mi.damo_selection,
                fn_damo_new=mi.fn_damo_new,
                fn_hdb_new=mi.fn_hdb_new,
                fn_damo_old=mi.fn_damo_old,
                fn_hdb_old=mi.fn_hdb_old,
                fn_threedimodel=mi.fn_threedimodel,
                fn_DAMO_comparison_export=self.output_file_path.value,
                fn_threedi_comparison_export=self.output_file_path.value,
                compare_with="Compare with Damo",
                layer_selection=False,
                layers_input_hdb_selection=[],
                layers_input_damo_selection=fiona.listlayers(mi.fn_damo_new),
                threedi_layer_selector=False,
                threedi_structure_selection=[],
                damo_structure_selection=[],
                structure_codes=[],
            )
            # 3Di
            main.main(
                model_info=mi,
                fn_DAMO_selection=mi.damo_selection,
                fn_damo_new=mi.fn_damo_new,
                fn_hdb_new=mi.fn_hdb_new,
                fn_damo_old=mi.fn_damo_old,
                fn_hdb_old=mi.fn_hdb_old,
                fn_threedimodel=mi.fn_threedimodel,
                fn_DAMO_comparison_export=self.output_file_path.value,
                fn_threedi_comparison_export=self.output_file_path.value,
                compare_with="Compare with 3Di",
                layer_selection=False,
                layers_input_hdb_selection=[],
                layers_input_damo_selection=[],
                threedi_layer_selector=True,
                threedi_structure_selection=config.THREEDI_STRUCTURE_LAYERS,
                damo_structure_selection=config.DAMO_HDB_STRUCTURE_LAYERS,
                structure_codes=config.STRUCTURE_CODES,
            )
            print("Both finished.")
