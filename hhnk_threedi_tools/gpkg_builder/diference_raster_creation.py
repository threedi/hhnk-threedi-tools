import os
import re

import numpy as np
import rasterio

from hhnk_threedi_tools import Folders


def get_scenario_key(name):
    """
    Extract scenario characteristics.

    Examples
    --------
    '14 piek_ghg_T100' -> ('piek', 'ghg', 'T100')
    'blok_ghg_T100'    -> ('blok', 'ghg', 'T100')
    """

    name_lower = name.lower()

    # Event type
    if "blok" in name_lower or "block" in name_lower:
        event_type = "blok"

    elif "piek" in name_lower:
        event_type = "piek"

    else:
        return None

    # Groundwater condition
    if "ghg" in name_lower:
        groundwater_condition = "ghg"

    elif "glg" in name_lower:
        groundwater_condition = "glg"

    else:
        groundwater_condition = None

    # Return period
    match = re.search(
        r"T\d+",
        name,
        flags=re.IGNORECASE,
    )

    if match is None:
        return None

    return_period = match.group().upper()

    return (
        event_type,
        groundwater_condition,
        return_period,
    )


def create_difference_raster(
    reference_path,
    variant_path,
    output_path,
):
    """
    Create difference raster.

    Difference = variant - current situation

    Positive values:
        more water depth in variant

    Negative values:
        less water depth in variant
    """

    with rasterio.open(reference_path) as src_reference:
        reference = src_reference.read(
            1,
            masked=True,
        ).astype(np.float32)

        profile = src_reference.profile.copy()

        reference_shape = src_reference.shape
        reference_transform = src_reference.transform
        reference_crs = src_reference.crs

    with rasterio.open(variant_path) as src_variant:
        variant = src_variant.read(
            1,
            masked=True,
        ).astype(np.float32)

        if src_variant.shape != reference_shape:
            raise ValueError(f"Raster shape differs:\nReference: {reference_shape}\nVariant:   {src_variant.shape}")

        if src_variant.transform != reference_transform:
            raise ValueError(f"Raster transform differs:\nReference: {reference_path}\nVariant:   {variant_path}")

        if src_variant.crs != reference_crs:
            raise ValueError(f"Raster CRS differs:\nReference: {reference_path}\nVariant:   {variant_path}")

    difference = variant - reference

    nodata = -9999.0

    profile.update(
        dtype="float32",
        nodata=nodata,
        compress="deflate",
    )

    with rasterio.open(
        output_path,
        "w",
        **profile,
    ) as dst:
        dst.write(
            difference.filled(nodata),
            1,
        )


# --------------------------------------------------
# Models
# --------------------------------------------------

paths = [
    r"H:\02.modellen\bergen_noord_huidig_situatie_JA",
    r"H:\02.modellen\bergen_noord_variant_1_JA",
    r"H:\02.modellen\bergen_noord_variant_2_JA",
    r"H:\02.modellen\bergen_noord_variant_3_JA",
]


variant_labels = {
    "bergen_noord_variant_1_JA": "V1",
    "bergen_noord_variant_2_JA": "V2",
    "bergen_noord_variant_3_JA": "V3",
}


# --------------------------------------------------
# Collect CURRENT SITUATION rasters
# --------------------------------------------------

reference_folder = Folders(paths[0])

reference_batch_path = reference_folder.threedi_results.batch.path

reference_rasters = {}


for results in os.listdir(reference_batch_path):
    output_raster_path = reference_batch_path / results / "02_output_rasters"

    if not output_raster_path.exists():
        continue

    for scenario in os.listdir(output_raster_path):
        scenario_path = output_raster_path / scenario

        if not scenario_path.is_dir():
            continue

        scenario_key = get_scenario_key(scenario)

        if scenario_key is None:
            continue

        raster_path = scenario_path / "max_wdepth_corr_idw.tif"

        if not raster_path.exists():
            continue

        reference_rasters[scenario_key] = raster_path

        print("REFERENCE FOUND")
        print(f"  Scenario: {scenario_key}")
        print(f"  Raster:   {raster_path}")
        print()


# --------------------------------------------------
# Variants
# --------------------------------------------------

for path in paths[1:]:
    folder = Folders(path)

    variant_label = variant_labels[folder.name]

    batch_path = folder.threedi_results.batch.path

    for results in os.listdir(batch_path):
        output_raster_path = batch_path / results / "02_output_rasters"

        if not output_raster_path.exists():
            continue

        for scenario in os.listdir(output_raster_path):
            scenario_path = output_raster_path / scenario

            if not scenario_path.is_dir():
                continue

            scenario_key = get_scenario_key(scenario)

            if scenario_key is None:
                continue

            variant_raster = scenario_path / "max_wdepth_corr_idw.tif"

            if not variant_raster.exists():
                continue

            # --------------------------------------------------
            # Find EXACTLY matching current scenario
            # --------------------------------------------------

            if scenario_key not in reference_rasters:
                print("NO MATCHING HUIDIG RASTER")
                print(f"  Variant:  {variant_label}")
                print(f"  Scenario: {scenario_key}")
                print()

                continue

            reference_raster = reference_rasters[scenario_key]

            # --------------------------------------------------
            # Explicit output name
            # --------------------------------------------------

            event_type, groundwater_condition, return_period = scenario_key

            scenario_name = "_".join(
                value.upper()
                for value in (
                    event_type,
                    groundwater_condition,
                    return_period,
                )
                if value is not None
            )

            output_difference = scenario_path / (f"DIFF_wdepth__{variant_label}_MINUS_HUIDIG__{scenario_name}.tif")

            # --------------------------------------------------
            # Print comparison
            # --------------------------------------------------

            print("CREATING DIFFERENCE RASTER")

            print(f"  Variant:   {variant_label}")

            print(f"  Scenario:  {scenario_name}")

            print(f"  HUIDIG:    {reference_raster}")

            print(f"  {variant_label}:        {variant_raster}")

            print(f"  DIFFERENCE: {output_difference}")

            print()

            # --------------------------------------------------
            # Calculate
            # --------------------------------------------------

            create_difference_raster(
                reference_path=reference_raster,
                variant_path=variant_raster,
                output_path=output_difference,
            )

# %%
