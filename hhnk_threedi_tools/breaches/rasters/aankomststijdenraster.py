# %%
import os
from pathlib import Path
from typing import Sequence, Tuple, Union

import numpy as np
import rasterio

# raster path location
input_dir = Path(r"Y:\personen\jacosta\kaarten_20cm\Aankomsttijdenrasters HHNK\Resultaat")

# list with max water depth raster and time step
raster_files = [
    ("waterdiepte_max_t02h.tif", 2),
    ("waterdiepte_max_t04h.tif", 4),
    ("waterdiepte_max_t08h.tif", 8),
    ("waterdiepte_max_t12h.tif", 12),
    ("waterdiepte_max_t24h.tif", 24),
    ("waterdiepte_max_t36h.tif", 36),
    ("waterdiepte_max_t48h.tif", 48),
]


def aankmoststijden_raster(
    input_dir: Union[str, Path],
    raster_files: Sequence[Tuple[str, int]],
) -> None:
    """Create arrival-time rasters from depth rasters.

    For each region in input_dir/*/output_merged, writes a raster where each
    pixel value is the first hour (from raster_files) where depth >= 0.20.
    Parameters: input_dir (Path|str), raster_files (Sequence[(filename, hour)]).
    """

    # list all region folders inside the provided input directory
    regions = os.listdir(input_dir)

    for region in regions:
        # build the path to the merged output folder for this region
        region_path = Path(input_dir) / region / "output_merged"

        if not region_path.is_dir():
            print(f" for the region  {region}: there is no  output_merged")
            continue

        print(f"\n doing region: {region}")

        # use the first raster from the provided list to read profile/shape
        first_raster = region_path / raster_files[0][0]

        if not first_raster.exists():
            print(f" raster not found ({first_raster}), skipping region.")
            continue

        with rasterio.open(first_raster) as src0:
            profile = src0.profile.copy()
            shape = (src0.height, src0.width)

        # integer array to hold the arrival hour for each pixel (0 = unknown)
        arrival = np.zeros(shape, dtype=np.uint8)

        # loop rasters in the order given; first matching depth sets the hour
        for fname, hour in raster_files:
            path = region_path / fname

            if not path.exists():
                arrival = None
                break

            with rasterio.open(path) as src:
                # read the first band and convert to float for comparison
                data = src.read(1).astype("float32")

                # valid pixels: finite numbers and depth >= 0.20
                valid = np.isfinite(data) & (data >= 0.20)

                # set hour for pixels that are still 0 (unset) and valid now
                arrival[(arrival == 0) & valid] = hour

        if arrival is None:
            continue

        # write the resulting arrival raster back to disk
        output_file = region_path / "aankomsttijd_20cm.tif"

        profile.update(dtype=rasterio.uint8, count=1, nodata=0, compress="lzw")

        with rasterio.open(output_file, "w", **profile) as dst:
            dst.write(arrival, 1)

    # %%
