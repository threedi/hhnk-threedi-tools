# %%
import os

# %%
from pathlib import Path

import numpy as np
import rasterio

from hhnk_threedi_tools import Folders

base_folder = Path(r"H:\02.modellen")
raster_name = "wdepth_corr_idw.tif"

rainfall_scenarios = {
    "piek": ["piek_ghg_T10", "piek_ghg_T100", "piek_ghg_T1000"],
    # "blok": ["blok_ghg_T10", "blok_ghg_T100", "blok_ghg_T1000"],
}


def raster_difference(reference_path, variant_path, output_path):
    with rasterio.open(reference_path) as src_ref, rasterio.open(variant_path) as src_var:
        if src_ref.crs != src_var.crs or src_ref.transform != src_var.transform or src_ref.shape != src_var.shape:
            raise ValueError(f"Rasters are not aligned:\n{reference_path}\n{variant_path}")

        ref = src_ref.read(1, masked=True)
        var = src_var.read(1, masked=True)

        # NoData is treated as zero (dry cells).
        difference = var.filled(0) - ref.filled(0)

        profile = src_var.profile.copy()
        profile.update(dtype="float32", nodata=-9999, compress="lzw")

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(difference.astype("float32"), 1)


huidig_folder = Folders(base_folder / "bergen_noord_huidig_situatie_JA")

for variant in [2, 3]:
    folder = Folders(base_folder / f"bergen_noord_variant_{variant}_JA")

    for rainfall_type, rainfall_names in rainfall_scenarios.items():
        # Variant batch uses "block", current situation uses "blok".
        variant_type = "block" if rainfall_type == "blok" else "piek"
        batch = f"variant_{variant}_results_{variant_type}_new_dem"

        variant_downloads = Path(folder.threedi_results.batch[batch].downloads.path)

        huidig_outputs = (
            Path(huidig_folder.threedi_results.batch[f"huidig_{rainfall_type}"].path) / "02_output_rasters"
        )

        for rainfall in rainfall_names:
            variant_raster = variant_downloads / rainfall / raster_name
            huidig_raster = huidig_outputs / rainfall / "max_wdepth_corr_idw.tif"

            output_name = f"DIFF_wdepth__V{variant}_MINUS_HUIDIG__{rainfall.upper()}.tif"

            output_path = base_folder / f"bergen_noord_variant_{variant}_JA" / "raster_differences" / output_name

            if not variant_raster.exists() or not huidig_raster.exists():
                print(f"Missing raster: {rainfall}, V{variant}")
                continue

            raster_difference(huidig_raster, variant_raster, output_path)
            print(f"Created: {output_name}")
#  %%
