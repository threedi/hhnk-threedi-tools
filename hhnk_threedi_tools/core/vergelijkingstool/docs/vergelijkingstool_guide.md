## **Vergelijkingstool**
Binnen HHNK worden 3Di modellen opgebouwd met brongegevens uit DAMO en de lokale HDB-database. Deze gegevens worden via FME geëxporteerd. Met de vergelijkingstool kunnen modelleurs verschillen inzichtelijk maken tussen een recente DAMO- en HDB-export en een bestaand 3Di model. Daarnaast kan dezelfde export worden vergeleken met een oudere DAMO- en HDB-dataset. De tool vergelijkt onder andere:

| Onderdeel | Controle |
| --- | --- |
| Watergangen | Aanwezigheid, geometrie en attributen |
| Kunstwerken | Verschillen in ligging en kenmerken |
| Peilgebieden | Controle van actuele grenzen en waterpeilen |
| Modelinput | Vergelijking tussen de bestaande modelgegevens en de nieuwe DAMO en HDB export |

Het doel is niet om automatisch te bepalen wat “goed” of “fout” is, maar om verschillen zichtbaar te maken, zodat daarna een inhoudelijke beoordeling kan worden uitgevoerd.


## Workflow

Op basis van de vergelijking kan worden beoordeeld of het bestaande model nog geschikt is voor hergebruik, met enkele aanpassingen kan worden bijgewerkt, of opnieuw moet worden opgebouwd.

Het onderstaande stroomschema toont de algemene workflow en de bijbehorende besluitvorming. Het jaar 2014 wordt hierin alleen gebruikt als voorbeeld van een oudere brondata-export. Het diagram laat zien hoe de resultaten van de vergelijkingstool kunnen worden gebruikt om een onderbouwde keuze te maken tussen hergebruik, aanpassing of nieuwbouw van het model.

![Workflow](../../images/4_gebruik_plugin/f_vergelijkingstool//workflow_vergelijkingstool.png)

## Benodigde input

De vergelijkingstool verwacht dat de invoerbestanden volgens een vaste mappenstructuur zijn opgeslagen. Het model moet minimaal de volgende mappen bevatten:
```text
model_folder/
    ├── 00_config
    ├── 01_source_data
    ├── 02_schematisation
    ├── 03_3di_results
    ├── 04_test_results
    └── Notebooks
```

Binnen deze structuur leest de tool de gegevens uit de volgende locaties:
```text
model_folder/
    ├── 01_source_data/
    │   ├── polder_polygon.gpkg
    │   ├── DAMO.gpkg                        ← Oude DAMO-export
    │   ├── HDB.gpkg                         ← Oude HDB-export
    │   └── vergelijkingstool/
    │       ├── input_nieuwe_export/
    │       │   ├── DAMO.gpkg                ← Nieuwe DAMO-export
    │       │   └── HDB.gpkg                 ← Nieuwe HDB-export
    │       └── output/
    │           └── vergelijkingstool_output.gpkg
    └── 02_schematisation/
        └── 00_basis/
            └── HUB.gpkg                     ← Bestaand 3Di model
```
      
Om de vergelijkingstool goed te laten werken, moet eerst vanuit FME een nieuwe export van DAMO en HDB worden gemaakt. Deze meest recente export wordt opgeslagen in de map `input_nieuwe_export`. Voor het model HUB is dat de volgende locatie:

<p align="center">
<code>H:\02.modellen\HUB\01_source_data\vergelijkingstool\input_nieuwe_export</code>
</p>

Daarnaast moeten ook de oude DAMO en HDB bestanden in `01_source_data` aanwezig zijn en moet het bestaande 3Di model beschikbaar zijn in de map van de basisschematisatie:

<p align="center">
<code>H:\02.modellen\HUB\02_schematisation\00_basis</code>
</p>


Deze vaste mappenstructuur is noodzakelijk voor de werking van de vergelijkingstool, ongeacht of de vergelijking **`Damo Updated vs 3Di model`** of **`Damo Updated vs Damo Old`** wordt uitgevoerd. Als de bestanden niet volgens deze structuur zijn opgeslagen, kan de vergelijkingstool de benodigde invoer niet correct vinden en zal de tool niet goed werken.

## Gebruik van de Vergelijkingstool

> **Belangrijk**
>
> De Vergelijkingstool verwacht dat alle vereiste invoerbestanden aanwezig zijn en volgens de voorgeschreven mappenstructuur zijn opgeslagen. Als de invoerbestanden ontbreken of niet op de juiste locatie binnen de mappenstructuur zijn geplaatst, kan de Vergelijkingstool de benodigde gegevens niet vinden en zal de tool niet correct functioneren.
>
> Controleer daarom eerst de paragraaf **Benodigde input** voordat de Vergelijkingstool wordt uitgevoerd.

## Stap 1. De Vergelijkingstool starten

Open de map waarin de Vergelijkingstool is opgeslagen.

```text
D:\vergelijkingstool
```

Start de Vergelijkingstool door te dubbelklikken op **start_vergelijkingstool**.

![Step One Run bat file](../../images/4_gebruik_plugin/f_vergelijkingstool/step_1.png)

Hiermee wordt JupyterLab geopend en wordt de notebook van de Vergelijkingstool automatisch geladen.

Klik vervolgens op **Run** om de notebook uit te voeren. Na enkele seconden wordt de grafische interface van de Vergelijkingstool weergegeven.

![Step One Run Jupiter](../../images/4_gebruik_plugin/f_vergelijkingstool/step_1_run_notebook.png)

---

### Stap 2. De modellenmap selecteren

Controleer of voer het pad in bij **Enter the Model folder path**.

De Vergelijkingstool bepaalt automatisch de benodigde invoer- en uitvoermappen op basis van deze locatie.

---

### Stap 3. Het type vergelijking selecteren

Kies één van de beschikbare vergelijkingen:

- **Damo Updated vs 3Di model**
- **Damo Updated vs Damo Old**
- **Both**

---

### Stap 4. De vergelijking uitvoeren

Klik op **Run Comparison**.

De Vergelijkingstool vergelijkt de geselecteerde datasets en genereert een GeoPackage (`.gpkg`) met de gevonden verschillen.

---

### Stap 5. De resultaten openen

Het volledige pad naar het gegenereerde uitvoerbestand wordt weergegeven in het veld **Full path**.

Klik op **Open Output Folder** om de map met het gegenereerde `.gpkg`-bestand te openen.

Het resultaat kan vervolgens in QGIS worden geopend voor een inhoudelijke beoordeling.
