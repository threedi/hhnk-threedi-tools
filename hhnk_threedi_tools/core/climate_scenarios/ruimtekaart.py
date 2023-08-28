# %% debug

"""
Compute the sum of 12 rasterfiles in polygons given by a shapefile and compute
the "ruimte-indicator". Optionally, a mask shapefile can be provided.
"""

import logging
import numpy as np

logger = logging.getLogger(__name__)


SCENARIOS = (
    "blok_ggg_T10",
    "blok_ghg_T10",
    "blok_ggg_T100",
    "blok_ghg_T100",
    "blok_ggg_T1000",
    "blok_ghg_T1000",
)

ATTRS = {
    "damage_0": "dggt10",
    "damage_1": "dght10",
    "damage_2": "dggt100",
    "damage_3": "dght100",
    "damage_4": "dggt1000",
    "damage_5": "dght1000",
    "volume_0": "mggt10",
    "volume_1": "mght10",
    "volume_2": "mggt100",
    "volume_3": "mght100",
    "volume_4": "mggt1000",
    "volume_5": "mght1000",
    "deuro_per_dm3_0": "ekght10",
    "deuro_per_dm3_1": "ekggt100",
    "deuro_per_dm3_2": "ekght100",
    "deuro_per_dm3_3": "ekggt1000",
    "deuro_per_dm3_4": "ekght1000",
    "deuro_per_dm3_norm_0": "nekght10",
    "deuro_per_dm3_norm_1": "nekggt100",
    "deuro_per_dm3_norm_2": "nekght100",
    "deuro_per_dm3_norm_3": "nekggt1000",
    "deuro_per_dm3_norm_4": "nekght1000",
    "indicator": "indicator",
}

MAX_NUM_REGIONS = 2**16 - 1  # uint16 dtype + nodata value


def create_ruimtekaart(pgb_file, output_path, batch_fd):
    """Calcualtion of ruimtekaart. Calculates the total volume
    and damage per region for multiple calculations and calculates
    an indicator whether it is relatively cheap the store extra
    water in that region."""
    pgb_gdf = pgb_file.load()
    num_regions = len(pgb_gdf)

    # aggregate the 12 input rasters
    damages_euro = np.zeros((num_regions, 6), dtype=float)
    volumes_m3 = np.zeros((num_regions, 6), dtype=float)

    # Aggregate sum per region for each result for both the depth and damage rasters

    # DEPTH
    labels_raster = batch_fd.output.temp.peilgebieden_diepte
    labels_index = pgb_gdf["index"].values
    for i, fn in enumerate(SCENARIOS):
        input_raster = getattr(batch_fd.downloads, fn).depth_max

        logger.info("Aggregating '{}'".format(input_raster.base))

        # calculate sum per region.
        volumes_m3[:, i] = input_raster.sum_labels(
            labels_raster=labels_raster, labels_index=labels_index
        )
        volumes_m3[:, i] *= input_raster.pixelarea  # take pixelsize into account.

    # DAMAGE
    labels_raster = batch_fd.output.temp.peilgebieden_schade
    labels_index = pgb_gdf["index"].values
    for i, fn in enumerate(SCENARIOS):
        input_raster = getattr(batch_fd.downloads, fn).damage_total

        logger.info("Aggregating '{}'".format(input_raster.base))

        # calculate sum per region.
        damages_euro[:, i] = input_raster.sum_labels(
            labels_raster=labels_raster, labels_index=labels_index
        )

    # add the total sum per raster to the last row
    m3 = np.concatenate([volumes_m3, np.sum(volumes_m3, axis=0)[np.newaxis]], axis=0)
    euro = np.concatenate(
        [damages_euro, np.sum(damages_euro, axis=0)[np.newaxis]], axis=0
    )

    # take the forward differential
    d_euro = np.diff(euro, axis=1)
    d_m3 = np.diff(m3, axis=1)

    # how many euros extra per m3 extra?
    mask = d_m3 != 0  # .all(axis=1)
    d_euro_per_m3 = np.full_like(d_m3, np.nan)
    d_euro_per_m3[mask] = np.clip(d_euro[mask] / d_m3[mask], 0, 1e100)

    # normalize on the total sum
    d_euro_per_m3_norm = (d_euro_per_m3[-1] - d_euro_per_m3) / d_euro_per_m3[-1]

    # compute the indicator, ignoring NaNs
    weights = np.tile([[1.0, 2.0, 2.0, 5.0, 5.0]], (d_euro_per_m3_norm.shape[0], 1))
    weights[np.isnan(d_euro_per_m3_norm)] = 0

    # toevoeging 2021-03-02 Als er wel toename in m3 maar maar afname in schade of andersom over de som van alle peilgebieden gaat
    # gaat de hele berekening plat. Daarom worden deze scenarios niet mee genomen in de berekening van de indicator. Voor zijpe noord
    # zijpe zuid was dit het geval.
    weights[np.isinf(d_euro_per_m3_norm)] = 0
    weights_total = weights.sum(1)
    weights_total[weights_total == 0] = np.nan
    indicator = np.nansum(d_euro_per_m3_norm * weights, axis=1) / weights_total

    # add the results to the output file
    # set the total damages
    for j in range(6):
        pgb_gdf[ATTRS["damage_{}".format(j)]] = euro[:-1, j]

    # set the total volumes
    for j in range(6):
        pgb_gdf[ATTRS["volume_{}".format(j)]] = m3[:-1, j]

    # set the incremental euro/m3
    for j in range(5):
        pgb_gdf[ATTRS["deuro_per_dm3_{}".format(j)]] = d_euro_per_m3[:-1, j]

    # set the normalized incremental euro/m3
    for j in range(5):
        pgb_gdf[ATTRS["deuro_per_dm3_norm_{}".format(j)]] = d_euro_per_m3_norm[:-1, j]

    # set the indicator
    pgb_gdf[ATTRS["indicator"]] = indicator[:-1]

    # pgb_gdf = pgb_gdf.loc[unique_labels] #TODO Drop regions that do that fall within the rasters.
    pgb_gdf["relmmt1000"] = np.round(pgb_gdf["mght1000"] * 1000 / sum(pgb_gdf.area), 2)

    pgb_gdf.to_file(output_path, index_col=False)


# %%
