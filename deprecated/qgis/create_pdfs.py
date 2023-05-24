#!/usr/bin/env python
# coding: utf-8

# In[4]:


import os
import pandas as pd

"""
Met de klimaatsommen van de watersysteemanalyse worden rasterbestanden aangemaakt. Dit script zet deze rasterbestanden
om in pdf. 
"""

output_path = QgsProject.instance().readPath("./")  # same as qgis project loc


polder_folder = output_path.split("/")[-3]  # 10.tHoekje
polder = polder_folder.split(".")[1]
if "rev" in output_path.split("/")[-1]:
    revisie = output_path.split("/")[-1].split("rev")[
        -1
    ]  # TODO: toevoegen controle of dit de revisie is..
else:
    revisie = output_path.split("/")[-1].split("_")[
        -1
    ]  # TODO: toevoegen controle of dit de revisie is..

subtitle = "Maatregel model {} (rev{})".format(polder, revisie)

# Select print composer
dic = {}
# Schadekaarten
dic[0] = {
    "composer_name": "wsa_kaarten",
    "pdf_name": "{}_rev{}_cw_schade_totaal.pdf".format(polder, revisie),
    "theme": "schade_totaal",
    "title": "Schade",
    "legenda": "legenda_schade",
}
dic[1] = {
    "composer_name": "wsa_kaarten",
    "pdf_name": "{}_rev{}_cw_schade_plas.pdf".format(polder, revisie),
    "theme": "schade_plas",
    "title": "Schade plasvorming",
    "legenda": "legenda_schade",
}
dic[2] = {
    "composer_name": "wsa_kaarten",
    "pdf_name": "{}_rev{}_cw_schade_overlast.pdf".format(polder, revisie),
    "theme": "schade_overlast",
    "title": "Schade watersysteem",
    "legenda": "legenda_schade",
}

# #Gecorrigeerde schadekaarten
dic[3] = {
    "composer_name": "wsa_kaarten",
    "pdf_name": "{}_rev{}_cw_schade_totaal_corr.pdf".format(polder, revisie),
    "theme": "schade_totaal_corr",
    "title": "Schade",
    "legenda": "legenda_schade",
}
dic[4] = {
    "composer_name": "wsa_kaarten",
    "pdf_name": "{}_rev{}_cw_schade_plas_corr.pdf".format(polder, revisie),
    "theme": "schade_plas_corr",
    "title": "Schade plasvorming",
    "legenda": "legenda_schade",
}
dic[5] = {
    "composer_name": "wsa_kaarten",
    "pdf_name": "{}_rev{}_cw_schade_overlast_corr.pdf".format(polder, revisie),
    "theme": "schade_overlast_corr",
    "title": "Schade watersysteem",
    "legenda": "legenda_schade",
}


# Inundatiekaarten
dic[6] = {
    "composer_name": "wsa_kaarten",
    "pdf_name": "{}_rev{}_inundatie_totaal.pdf".format(polder, revisie),
    "theme": "inundatie_totaal",
    "title": "Inundatie",
    "legenda": "legenda_inundatie",
}
dic[7] = {
    "composer_name": "wsa_kaarten",
    "pdf_name": "{}_rev{}_inundatie_plas.pdf".format(polder, revisie),
    "theme": "inundatie_plas",
    "title": "Inundatie plasvorming",
    "legenda": "legenda_inundatie",
}
dic[8] = {
    "composer_name": "wsa_kaarten",
    "pdf_name": "{}_rev{}_inundatie_overlast.pdf".format(polder, revisie),
    "theme": "inundatie_overlast",
    "title": "Inundatie watersysteem",
    "legenda": "legenda_inundatie",
}

# Ruimtekaart
dic[9] = {
    "composer_name": "wsa_kaarten",
    "pdf_name": "{}_rev{}_ruimtekaart.pdf".format(polder, revisie),
    "theme": "ruimtekaart",
    "title": "Ruimtekaart",
    "legenda": "legenda_ruimtekaart",
}

# Landgebruik
dic[10] = {
    "composer_name": "wsa_kaarten",
    "pdf_name": "{}_rev{}_landgebruik.pdf".format(polder, revisie),
    "theme": "landgebruik",
    "title": "Landgebruik",
    "legenda": "legenda_landgebruik",
}


# Set dict in dataframe that will be looped.
df = pd.DataFrame(columns=["composer_name", "pdf_name", "theme", "title", "legenda"])
df = df.append([dic[a] for a in dic], ignore_index=True)

df["pdf_path"] = df["pdf_name"].apply(lambda x: os.path.join(output_path, x))

# legenda_ids = df['legenda'].unique().tolist()
legenda_ids = [
    "legenda_schade",
    "legenda_inundatie",
    "legenda_ruimtekaart",
    "legenda_landgebruik",
]

# Connect to project and al print composers
projectInstance = QgsProject.instance()
layoutmanager = projectInstance.layoutManager()


def create_pdf_from_composer(
    composer_name, title, subtitle, legenda_ids, selected_legenda, theme, output_file
):
    layout_item = layoutmanager.layoutByName(composer_name)  # test is the layout name

    # -------------------------------------------------------------------------------------
    # Change layout settings
    # -------------------------------------------------------------------------------------
    label_item = layout_item.itemById("titel")
    label_item.setText(title)

    label_item = layout_item.itemById("subtitel")
    label_item.setText(subtitle)

    # Hide all legend items, only show selected legend.
    for legenda_id in legenda_ids:
        legenda_item = layout_item.itemById(legenda_id)
        legenda_item.setVisibility(False)
        if legenda_id == selected_legenda:
            legenda_item.setVisibility(True)

    map = layout_item.referenceMap()
    map.setFollowVisibilityPresetName(theme)

    # Poging om extent goed te zetten, maar handmatig is beter.
    # layer = QgsProject.instance().mapLayersByName('schade_per_peilgebied')[0]
    # map.setExtent(layer.extent())
    # map.attemptResize(QgsLayoutSize(250,200,QgsUnitTypes.LayoutMillimeters))

    # -------------------------------------------------------------------------------------
    # Export
    # -------------------------------------------------------------------------------------
    pdf_settings = QgsLayoutExporter.PdfExportSettings()
    pdf_settings.textRenderFormat = (
        QgsRenderContext.TextFormatAlwaysText
    )  # If not changed the labels will be ugly in the pdf

    image_settings = QgsLayoutExporter.ImageExportSettings()

    export = QgsLayoutExporter(layout_item)
    result = export.exportToPdf(output_file, pdf_settings)
    if result == 0:
        print("pdf aangemaakt: {}".format(row["pdf_name"]))
    else:
        print("pdf niet gelukt: {}".format(row["pdf_name"]))
    return result


for index, row in df.iterrows():
    result = create_pdf_from_composer(
        composer_name=row["composer_name"],
        title=row["title"],
        subtitle=subtitle,
        legenda_ids=legenda_ids,
        selected_legenda=row["legenda"],
        theme=row["theme"],
        output_file=row["pdf_path"],
    )


# In[ ]:
