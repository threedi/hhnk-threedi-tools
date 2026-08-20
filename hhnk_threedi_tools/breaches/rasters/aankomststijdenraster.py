# %%
from pathlib import Path

import numpy as np
import rasterio

input_dir = Path(
    r"Y:\personen\jacosta\kaarten_20cm\Aankomsttijdenrasters HHNK\Resultaat\Noord-Holland Zuid-Oost, Doorbraak dijktraject 1306, 13-7, 13-8 en 13-9\output_merged"
)

raster_files = [
    "waterdiepte_max_t02h.tif",
    "waterdiepte_max_t04h.tif",
    "waterdiepte_max_t08h.tif",
    "waterdiepte_max_t12h.tif",
    "waterdiepte_max_t24h.tif",
    "waterdiepte_max_t36h.tif",
    "waterdiepte_max_t48h.tif",
]

prev_valid = None
prev_name = None

for fname in raster_files:
    path = input_dir / fname
    with rasterio.open(path) as src:
        data = src.read(1)
        if src.nodata is not None:
            valid = data != src.nodata
        else:
            valid = ~np.isnan(data)

    if prev_valid is not None:
        inconsistent = prev_valid & (~valid)
        n_inconsistent = inconsistent.sum()
        print(f"{prev_name} -> {fname}: {n_inconsistent} wrong pixels")

    prev_valid = valid
    prev_name = fname
# %%

import os
from pathlib import Path

import numpy as np
import rasterio

# Carpeta raíz donde están todas las regiones
input_dir = Path(r"Y:\personen\jacosta\kaarten_20cm\Aankomsttijdenrasters HHNK\Resultaat")

raster_files = [
    ("waterdiepte_max_t02h.tif", 2),
    ("waterdiepte_max_t04h.tif", 4),
    ("waterdiepte_max_t08h.tif", 8),
    ("waterdiepte_max_t12h.tif", 12),
    ("waterdiepte_max_t24h.tif", 24),
    ("waterdiepte_max_t36h.tif", 36),
    ("waterdiepte_max_t48h.tif", 48),
]

regions = os.listdir(input_dir)

for region in regions:
    region_path = input_dir / region / "output_merged"

    if not region_path.is_dir():
        print(f" for the region  {region}: there is no  output_merged")
        continue

    print(f"\n doing region: {region}")

    first_raster = region_path / raster_files[0][0]

    if not first_raster.exists():
        print(f" raster not found ({first_raster}), skipping region.")
        continue

    with rasterio.open(first_raster) as src0:
        profile = src0.profile.copy()
        shape = (src0.height, src0.width)

    arrival = np.zeros(shape, dtype=np.uint8)

    for fname, hour in raster_files:
        path = region_path / fname

        if not path.exists():
            arrival = None
            break

        with rasterio.open(path) as src:
            data = src.read(1).astype("float32")

            valid = np.isfinite(data) & (data >= 0.20)

            # update pixel with no time yet, but valid, to the current hour
            arrival[(arrival == 0) & valid] = hour

    if arrival is None:
        continue

    output_file = region_path / "aankomsttijd_20cm.tif"

    profile.update(dtype=rasterio.uint8, count=1, nodata=0, compress="lzw")

    with rasterio.open(output_file, "w", **profile) as dst:
        dst.write(arrival, 1)


# %%
