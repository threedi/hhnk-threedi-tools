# %%
"""
Flood Breach Map - Noord-Holland  |  Version 3
===============================================
Layers:
  - Primary Dikes     (red polylines)
  - Regional Dikes    (grey polylines)
  - Breaches ROR      (clustered markers)
  - Breaches IPO      (clustered markers)

Key features:
  - Popup with IFrame: 2-column field grid + 2 side-by-side images
  - Image cache: PNGs are converted to JPEG once and reused on reruns
  - Legend with HHNK company logo
  - 4-layer checkboxes in LayerControl
"""

import base64
import math
import os

import branca.colormap as cm
import folium
import geopandas as gpd
from folium.plugins import MarkerCluster
from PIL import Image

# =============================================================================
#  CONFIGURATION  <-- EDIT THESE PATHS
# =============================================================================
PROJECT_DIR = r"G:\02_Werkplaatsen\06_HYD\Projecten\HKC25007 Ontsluiten Overstromingsbeelden\folium_map"

# Paths to input data (GPKG) and output (HTML + PNG cache)
BREACHES_GPKG = os.path.join(PROJECT_DIR, "layers", "merged_breaches_combined.gpkg")
KERINGEN_GPKG = os.path.join(PROJECT_DIR, "layers", "merged_keringen_combined.gpkg")

PNG_BASE = os.path.join(PROJECT_DIR, "png_kaarten_folium")
ROR_OVERSTROM_DIR = os.path.join(PNG_BASE, "Primaire Keringen", "overstromigen")
ROR_SCHADE_DIR = os.path.join(PNG_BASE, "Primaire Keringen", "schade")
IPO_OVERSTROM_DIR = os.path.join(PNG_BASE, "Regionale Keringen", "overstromingen")
IPO_SCHADE_DIR = os.path.join(PNG_BASE, "Regionale Keringen", "schade")

# --- Image cache ---
# Processed JPEGs are stored here and reused on subsequent runs.
# A file is only reprocessed if the source PNG is newer than the cached JPEG.
IMG_CACHE_DIR = os.path.join(PNG_BASE, "_cache")
IMG_CACHE_MAX_PX = 350
IMG_CACHE_QUALITY = 80

# column values to filter and label breaches
VARIANT_PRIMAIRE = "Primaire Keringen"
VARIANT_REGIONALE = "Normering regionale keringen"
DOEL_ROR = "Actualisatie aanlevering ROR."
DOEL_IPO = "Normering regionale keringen"

# --- Other paths ---
LOGO_PATH = os.path.join(PNG_BASE, "2. HHNK blauw liggend RGB.jpg")
OUTPUT_HTML = os.path.join(PROJECT_DIR, "overstromingen_kaarten.html")

# Klimaatatlas link (same for all popups)
KLIMAAT_URL = (
    "https://hhnk.klimaatmonitor.net/?simpleurldata="
    "%7B%22view%22%3A%22public%22%2C%22viewport%22%3A%5B4.823%2C52.67%2C11%5D"
    "%2C%22layers%22%3A%5B%22e606e666-7c1c-4f22-845a-ff9363e5776f%22%5D"
    "%2C%22theme%22%3A%22Overstromingen%22%7D"
)

# =============================================================================
#  IMAGE CACHE
# =============================================================================
os.makedirs(IMG_CACHE_DIR, exist_ok=True)


def encode_image(src_path):
    """Return a base64-encoded JPEG string ready to embed in HTML.

    Cache logic:
      - The processed JPEG is saved to IMG_CACHE_DIR.
      - If the cached JPEG exists and is newer than the source PNG,
        it is read directly (fast, no reprocessing).
      - Otherwise the PNG is resized, saved to cache, then encoded.
    Result: images are only processed once; subsequent runs are fast.
    """
    if not src_path or not os.path.exists(src_path):
        return ""
    cache_name = os.path.splitext(os.path.basename(src_path))[0] + ".jpg"
    cache_path = os.path.join(IMG_CACHE_DIR, cache_name)
    if os.path.exists(cache_path) and os.path.getmtime(cache_path) >= os.path.getmtime(src_path):
        with open(cache_path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    try:
        img = Image.open(src_path).convert("RGB")
        img.thumbnail((IMG_CACHE_MAX_PX, IMG_CACHE_MAX_PX))
        img.save(cache_path, format="JPEG", quality=IMG_CACHE_QUALITY, optimize=True)
        with open(cache_path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    except Exception as e:
        print(f"  [encode_image] Error  {src_path}: {e}")
        return ""


# =============================================================================
#  HELPERS
# =============================================================================
def step_color(return_period):
    """Return a CSS color based on the exceedance return period."""
    colormap = cm.StepColormap(
        ["lightgreen", "green", "yellow", "orange", "red", "darkred"],
        vmin=10,
        vmax=200_000,
        index=[10, 100, 1_000, 3_000, 7_000, 100_000],
    )
    try:
        return colormap(float(return_period))
    except Exception:
        return "yellow"


def add_kering_lines(gdf, feature_group, color, label_prefix):
    """Add dike polylines to a FeatureGroup.

    One PolyLine is created per individual geometry segment to avoid
    straight connector lines between non-contiguous parts of the same dike
    (which happens when you flatten all coordinates of a group into one list).

    Uses sc_naam as the name column (confirmed by diagnostic output).
    """
    for _, row in gdf.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue
        naam = row.get("sc_naam", "")
        tooltip_txt = label_prefix + ": " + str(naam)

        if geom.geom_type == "MultiLineString":
            for line in geom.geoms:
                pts = [(c[1], c[0]) for c in line.coords]
                if pts:
                    folium.PolyLine(
                        locations=pts,
                        color=color,
                        weight=3,
                        opacity=1.0,
                        tooltip=tooltip_txt,
                    ).add_to(feature_group)
        elif geom.geom_type == "LineString":
            pts = [(c[1], c[0]) for c in geom.coords]
            if pts:
                folium.PolyLine(
                    locations=pts,
                    color=color,
                    weight=3,
                    opacity=1.0,
                    tooltip=tooltip_txt,
                ).add_to(feature_group)


def build_popup(row, img_flood_path, img_damage_path):
    """Build a folium Popup (via IFrame) matching the reference layout:
    - Title: 'Breach: {Scenarionaam}'
    - 2-column grid of technical fields
    - Two side-by-side images (flood depth map + damage map)
    - Link to Klimaatatlas
    """
    b64_flood = encode_image(img_flood_path)
    b64_damage = encode_image(img_damage_path)

    def img_block(b64, path):
        if b64:
            img_html = (
                '<img src="data:image/jpeg;base64,' + b64 + '" '
                'style="width:100%;max-height:200px;object-fit:contain;'
                'border:1px solid #ddd;border-radius:4px;">'
            )
        else:
            img_html = '<p style="color:#aaa;font-size:11px;">[imagen no disponible]</p>'
        link_html = (
            '<a href="' + str(path) + '" download '
            'style="font-size:11px;color:#2980b9;">'
            "Om afbeelding te downloaden: klik met de rechtermuisknop "
            "en open een nieuw tabblad.</a>"
        )
        return '<div style="flex:1;text-align:center;min-width:0;">' + img_html + "<br>" + link_html + "</div>"

    try:
        schade_val = float(str(row.get("Total_Schade_Cost", 0)).replace("EUR", "").replace(",", ".").strip())
        schade_fmt = "EUR {:,.0f}".format(schade_val).replace(",", ".")
    except Exception:
        schade_fmt = str(row.get("Total_Schade_Cost", ""))

    # Grid 2 columnas — exactamente como la imagen de referencia
    fields_left = [
        ("Scenarionaam", str(row.get("Scenarionaam", ""))),
        ("Bresdiepte", str(row.get("Bresdiepte", "")) + " m"),
        ("Initiele Bresbreedte", str(row.get("Initiele bresbreedte", "")) + " m"),
        ("Initiele kruinhoogte [m+NAP]", str(row.get("Initial Crest [m+NAP]", "")) + " m+NAP"),
        ("Maximaal bresdebiete", str(row.get("Maximaal bresdebiet", "")) + " m\u00b3/s"),
        ("Overschrijdingsfrequentie", str(row.get("Overschrijdingsfrequentie", ""))),
    ]
    fields_right = [
        ("Dijk Traject", str(row.get("Naam waterkering", ""))),
        ("Duur Bresgroei", str(row.get("Duur bresgroei in verticale richting", "")) + " h"),
        ("Maximale bresbreedte", str(row.get("Maximale bresbreedte", "")) + " m"),
        ("Laagste kruinhoogte", str(row.get("Lowest crest", "")) + " m+NAP"),
        ("Maximale buitenwaterstand", str(row.get("Maximale buitenwaterstand", "")) + " m"),
        ("Schade Kosten", schade_fmt),
    ]

    def field_cell(label, value):
        return '<div style="padding:2px 0;font-size:13px;"><strong>' + label + ":</strong> " + value + "</div>"

    left_html = "".join(field_cell(l, v) for l, v in fields_left)
    right_html = "".join(field_cell(l, v) for l, v in fields_right)

    html = (
        '<div style="font-family:Arial,sans-serif;padding:14px;'
        'width:700px;line-height:1.5;">'
        # Titel
        '<h3 style="color:#2c3e50;margin:0 0 10px 0;'
        'padding-bottom:8px;border-bottom:2px solid #eee;font-size:15px;">'
        "Bres: " + str(row.get("Scenarionaam", "")) + "</h3>"
        # Grid 2 kolommen
        '<div style="display:grid;grid-template-columns:1fr 1fr;'
        'gap:2px 24px;margin-bottom:14px;">' + left_html + right_html + "</div>"
        # 2 afbeeldingen naast elkaar
        '<div style="display:flex;gap:14px;margin-bottom:12px;">'
        + img_block(b64_flood, img_flood_path)
        + img_block(b64_damage, img_damage_path)
        + "</div>"
        # Klimaatatlas link
        '<div style="text-align:center;font-size:13px;">'
        '<a href="' + KLIMAAT_URL + '" target="_blank" style="color:#2980b9;">'
        "Bekijk meer in de Klimaatatlas</a>"
        "</div>"
        "</div>"
    )

    iframe = folium.IFrame(html=html, width=740, height=560)
    return folium.Popup(iframe, max_width=760)


# =============================================================================
#  LOAD DATA
# =============================================================================
print("Loading data...")
breaches = gpd.read_file(BREACHES_GPKG).to_crs(epsg=4326)
keringen = gpd.read_file(KERINGEN_GPKG).to_crs(epsg=4326)

primaire_kering = keringen[keringen["Variant_Type"] == VARIANT_PRIMAIRE].copy()
regionale_kering = keringen[keringen["Variant_Type"] == VARIANT_REGIONALE].copy()

print(f"  Bressen:          {len(breaches)}")
print(f"  Primaire Kering:  {len(primaire_kering)} segments")
print(f"  Regionale Kering: {len(regionale_kering)} segments")
print(f"  ROR: {(breaches['Doel'] == DOEL_ROR).sum()}  |  IPO: {(breaches['Doel'] == DOEL_IPO).sum()}")

# =============================================================================
#  MAPA BASE
# =============================================================================
m = folium.Map(location=[52.6483, 4.8629], tiles="cartodbpositron", zoom_start=10)

# =============================================================================
#  LAYER 1: PRIMARY DIKES  (red)
# =============================================================================
print("Adding Primary Dikes...")
fg_primary = folium.FeatureGroup(name="Primaire Kering", show=True)
add_kering_lines(primaire_kering, fg_primary, "#FF0000", "Primaire Kering")
fg_primary.add_to(m)

# =============================================================================
#  LAYER 2: REGIONAL DIKES  (grey)
# =============================================================================
print("Adding Regional Dikes...")
fg_regional = folium.FeatureGroup(name="Regionale Kering", show=True)
add_kering_lines(regionale_kering, fg_regional, "#838383", "Regionale Kering")
fg_regional.add_to(m)

# =============================================================================
#  LAYERS 3 & 4: BREACH MARKERS  (ROR and IPO, clustered)
# =============================================================================
fg_ror = folium.FeatureGroup(name="Bressen ROR", show=True)
fg_ipo = folium.FeatureGroup(name="Bressen IPO", show=True)
mc_ror = MarkerCluster().add_to(fg_ror)
mc_ipo = MarkerCluster().add_to(fg_ipo)

print("Adding breach markers (first run may be slow due to image caching)...")
skipped = 0

for idx, row in breaches.iterrows():
    geom = row.geometry
    if geom is None or geom.is_empty:
        skipped += 1
        continue
    try:
        if math.isnan(geom.x) or math.isnan(geom.y):
            skipped += 1
            continue
    except Exception:
        skipped += 1
        continue

    doel = str(row.get("Doel", "")).strip()

    if doel == DOEL_ROR:
        base_name = str(row.get("Scenarionaam", ""))
        img_flood = os.path.join(ROR_OVERSTROM_DIR, "overstroming_" + base_name + ".png")
        img_damage = os.path.join(ROR_SCHADE_DIR, "schade_" + base_name + ".png")
        target_mc = mc_ror
    elif doel == DOEL_IPO:
        base_name = str(row.get("Scenario Identificatie", row.get("Scenarionaam", "")))
        img_flood = os.path.join(IPO_OVERSTROM_DIR, base_name + "_max_wdiepte.png")
        img_damage = os.path.join(IPO_SCHADE_DIR, base_name + "_damage.png")
        target_mc = mc_ipo
    else:
        print(f"  [skip] idx={idx} unknown Doel value: {repr(doel)}")
        skipped += 1
        continue

    try:
        rp = float(row.get("Overschrijdingsfrequentie", 1000))
    except (ValueError, TypeError):
        rp = 1000

    popup = build_popup(row.to_dict(), img_flood, img_damage)

    folium.CircleMarker(
        location=[geom.y, geom.x],
        radius=8,
        color="black",
        fill=True,
        fill_color=step_color(rp),
        fill_opacity=1.0,
        opacity=0.85,
        tooltip=base_name,
        popup=popup,
    ).add_to(target_mc)

fg_ror.add_to(m)
fg_ipo.add_to(m)
print(f"  Done. Skipped: {skipped}")

# =============================================================================
#  LEYENDA CON LOGO HHNK
# =============================================================================
logo_html = ""
if LOGO_PATH and os.path.exists(LOGO_PATH):
    with open(LOGO_PATH, "rb") as f:
        logo_b64 = base64.b64encode(f.read()).decode("utf-8")
    logo_html = (
        '<img src="data:image/jpeg;base64,' + logo_b64 + '" '
        'style="width:100%;height:auto;display:block;margin-bottom:8px;">'
    )
    print("  Logo loaded OK")
else:
    print(f"  WARNING: logo not found at {LOGO_PATH}")


def dot(color):
    return (
        '<i style="background:' + color + ";width:14px;height:14px;"
        "border-radius:50%;display:inline-block;margin-right:6px;"
        'border:1px solid #ccc;vertical-align:middle;"></i>'
    )


def line_sym(color):
    return (
        '<i style="background:' + color + ";width:22px;height:3px;"
        'display:inline-block;margin-right:6px;vertical-align:middle;"></i>'
    )


legend_html = (
    '<div style="position:fixed;bottom:50px;left:50px;width:195px;'
    "border:1px solid #bbb;border-radius:6px;z-index:9999;font-size:12px;"
    'background:white;padding:10px;box-shadow:2px 2px 6px rgba(0,0,0,0.15);">'
    + logo_html
    + "<b>Legend:</b><br><b>Terugkeerperiode</b><br>"
    + dot("lightgreen")
    + "10<br>"
    + dot("green")
    + "100<br>"
    + dot("yellow")
    + "1.000<br>"
    + dot("orange")
    + "3.000<br>"
    + dot("red")
    + "10.000<br>"
    + dot("darkred")
    + "100.000<br><br>"
    + line_sym("#FF0000")
    + "Primaire Kering<br>"
    + line_sym("#838383")
    + "Regionale Kering"
    + "</div>"
)

m.get_root().html.add_child(folium.Element(legend_html))

# =============================================================================
#  LAYER CONTROL  (4 checkboxes)
#  =============================================================================
folium.LayerControl(collapsed=False).add_to(m)

# =============================================================================
#  SAVE
# =============================================================================
m.save(OUTPUT_HTML)
print(f"\nMap saved to:\n  {OUTPUT_HTML}")
print(f"Image cache: {IMG_CACHE_DIR}")
# %%
