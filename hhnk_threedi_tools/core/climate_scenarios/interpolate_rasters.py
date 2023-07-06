import numpy as np

import hhnk_research_tools as hrt


"""Functies die de 18 water depth rasters inladen van een klimaatscenario en daarmee """


def stack_raster_arrays(raster_classes, window):
    """Inladen waterdieptes van alle 18 diepte rasters voor het gegeven window"""
    
    window = list(map(int, window)) # make everything a integer for reading the array.
    stacked_array = []
    for raster in raster_classes:
        depth_array = raster._read_array(window=window)
        stacked_array.append(depth_array)

    # Stapel de arrays tot een 3D-array
    stacked_array = np.stack(stacked_array)
    stacked_array[stacked_array == raster.nodata] = 0.0
    return stacked_array


def interpoleer_deel(int_frequentie, waterdieptes, frequenties, min_value=0):
    # zet de frequenties die horen bij de waterdieptes in dezelfde 3d array als de waterdieptes
    fr = np.array([l * f for l, f in zip(np.ones(waterdieptes.shape), frequenties)])

    # Sorteer de waterdieptes van klein naar groot. Als de waterdieptes gelijke waarden hebben (tied) dan is er een
    # secundaire sortering op de frequenties (vaan hoog naar laag, daarom wordt hieronder 1/fr gedaan)
    # Hiermee komt een T10 frequentie eerder in de tabel te staan dan een T100 frequentie op het moment dat de waterdieptes gelijk zijn.
    order = np.lexsort((1 / fr, waterdieptes), axis=0)

    # Sorteer de waterdieptes
    waterdieptes.sort(axis=0)

    # Bepaal overschrijdingsfrequenties door te sorteren en op te tellen
    orderedfrequenties = frequenties[order]
    ovfreq = np.cumsum(orderedfrequenties[::-1], axis=0)[::-1]

    # Bepaal de interpolatie indices
    onderindex = np.maximum((int_frequentie < ovfreq).sum(axis=0) - 1, 0)
    bovenindex = np.minimum(onderindex + 1, len(ovfreq) - 1)

    # Wanneer ovfreq > freq alleen maar False is, is de overschrijdingsfrequentie
    # zo frequent dat we ervan uitgaan dat er geen problemen optreden. De
    # inundatiediepte of het risico is dan ook 0.
    geen_waarde = onderindex == 0
    # Tegenovergesteld geldt dat de frequentie zo klein (zeldzaam) kan zijn dat de
    # deze groter is dan alle meegenomen buien. We nemen dan de maximale
    # inundatiediepte aan
    maximale_waarde = onderindex == len(ovfreq) - 1
    onderindex[maximale_waarde] = bovenindex[maximale_waarde] - 1
    # Bepaal de grootste frequentie waarbij inundatie voorkomt. Als deze kleiner is dan
    # de interpolatiefrequentie moet de waterdiepte naar 0
    maxovfreq = ovfreq.copy()
    if min_value is not None:
        maxovfreq[waterdieptes <= min_value] = min_value
    maxovfreq = maxovfreq.max(axis=0)
    wlev_nul = maxovfreq < int_frequentie

    ysize, xsize = waterdieptes[0].shape
    k, j = np.meshgrid(np.arange(xsize), np.arange(ysize))

    # Converteer frequenties naar logaritmische schaal
    log_ovfreq = np.log(1.0 / ovfreq)

    # logaritmische interpolatie
    frac = (np.log(1.0 / int_frequentie) - log_ovfreq[onderindex, j, k]) / (
        log_ovfreq[bovenindex, j, k] - log_ovfreq[onderindex, j, k]
    )

    int_waterdiepte = waterdieptes[onderindex, j, k] + frac * (
        waterdieptes[bovenindex, j, k] - waterdieptes[onderindex, j, k]
    )

    # Vul randwaarden aan
    int_waterdiepte[geen_waarde] = 0.0
    int_waterdiepte[maximale_waarde] = waterdieptes.max(axis=0)[maximale_waarde]

    # Zet waterstanden op nul waarbij de interpolatiefrequentie groter is dat de maximale frequentie
    # waarbij inundatie optreedt
    int_waterdiepte[wlev_nul] = 0.0

    return int_waterdiepte


def interpoleer_raster_window(
    idx,
    part,
    raster_classes,
    dem_raster,
    int_frequentie,
    frequenties,
    extra_nodata_value,
    output_nodata,
):
    """Interpolatie van rasters voor een berekening over meerdere cores"""
    # Bepaal window
    window = part["window_readarray"]

    # Laad waterdieptes
    stacked_raster_array = stack_raster_arrays(
        raster_classes, window=window
    )  # laad 18 resultaten en zet in een array

    # Bepaal geïnterpoleerde waterdiepte
    int_raster_array = interpoleer_deel(
        int_frequentie, stacked_raster_array, frequenties
    )

    # Zet de gemaskeerde pixels op de nodata waarde (-9999.00)
    mask = int_raster_array == raster_classes[0].nodata
    int_raster_array[mask] = output_nodata

    # Bij de waterdieptes worden eventuele 'natte' pixels op de watergangen vervangen door een nodata value
    mask = dem_raster._read_array(window=window)
    mask = mask == 10.0
    int_raster_array[mask] = output_nodata

    # Waterdieptes van 0m omzetten in nodata value (werkt beter in lizard)
    mask = int_raster_array == extra_nodata_value
    int_raster_array[mask] = output_nodata

    return (part, int_raster_array)


def main_interpolate_rasters(
    T,
    output_file,
    rasters,
    frequenties,
    output_nodata,
    dem_path,
    extra_nodata_value=None,
):
    """Interpoleer 18 rasters samen met de frequentietabel tot 3 rasters met de T10, T100 en T1000 kans.
    Dit wordt gedaan voor de waterdiepterasters en de schaderasters"""

    if not output_file.exists():
        dem_raster = hrt.Raster(dem_path)

        # Inladen rasters als class
        raster_classes = [hrt.Raster(r) for r in rasters]

        depth_raster = raster_classes[0]
        parts = depth_raster.generate_blocks()
        array_out = (
            np.ones([depth_raster.shape[0], depth_raster.shape[1]]) * output_nodata
        )

        # #Loop over windows and calculate results
        for idx, part in parts.iterrows():
            part, int_raster_array = interpoleer_raster_window(
                idx=idx,
                part=part,
                raster_classes=raster_classes,
                dem_raster=dem_raster,
                int_frequentie=1.0 / T,
                frequenties=frequenties,
                extra_nodata_value=extra_nodata_value,
                output_nodata=output_nodata,
            )

            array_out[
                part.window[1] : part.window[3], part.window[0] : part.window[2]
            ] = int_raster_array

        hrt.save_raster_array_to_tiff(
            output_file=output_file.path,
            raster_array=array_out,
            nodata=output_nodata,
            metadata=depth_raster.metadata,
        )
        print(f"{output_file.path} created")

    else:
        print(f"{output_file.path} already exists")
