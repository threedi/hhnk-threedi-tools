# Vergelijkingstool DAMO / modelgegevens

Met de vergelijkingstool zijn de verschillen tussen actuele DAMO-data te vergelijken met de gegevens in een bestaand model of met een oude DAMO-data-set. De tool helpt modelleurs de actualiteit van het model te beoordelen. De tool vergelijkt onder andere:

| Onderdeel | Controle |
| --- | --- |
| Watergangen | Aanwezigheid, geometrie en attributen |
| Kunstwerken | Verschillen in ligging en kenmerken |
| Peilgebieden | Controle van actuele grenzen en waterpeilen |
| Modelinput | Vergelijking tussen de bestaande modelgegevens en de nieuwe DAMO-export |

Het doel is niet om automatisch te bepalen wat “goed” of “fout” is, maar om verschillen zichtbaar te maken, zodat daarna een inhoudelijke beoordeling kan worden uitgevoerd.

---

## Workflow

# TODO @JUAN de afbeelding bevat een spelfout. aanpass moet 'pas het bestaande model aan' zijn. Een waarom gebruik je specifiek 2014? Dat kan elk jaar zijn toch? Ik vind de figuur niet zo duidelijk. Kun je wat tekst schrijven wat er in staat en waarom het nuttig is?

De vergelijkingstool maakt het mogelijk om de verschillen te analyseren tussen de gegevens uit een recente DAMO export en een bestaand model dat is opgebouwd op basis van een oudere DAMO export. Op basis van deze vergelijking kan worden bepaald of het nodig is om een nieuw model te bouwen, of dat het bestaande model met enkele aanpassingen nog gebruikt kan worden.

Het onderstaande stroomschema toont de workflow en de besluitvorming voor een situatie waarin wordt uitgegaan van een model dat is opgebouwd met gegevens uit 2014. Hierin worden de stappen weergegeven die nodig zijn om te bepalen of er een nieuw model moet worden ontwikkeld, of dat het bestaande model kan worden aangepast.

![Workflow](workflow_vergelijkingstool.png)

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
    │   ├── DAMO.gpkg                        ← Nieuwe DAMO-export
    │   ├── HDB.gpkg                         ← Nieuwe HDB-export
    │   └── vergelijkingstool/
    │       ├── input_data_old/
    │       │   ├── DAMO.gpkg                ← Oude DAMO-export
    │       │   └── HDB.gpkg                 ← Oude HDB-export
    │       └── output/
    │           └── vergelijkingstool_output.gpkg
    └── 02_schematisation/
        └── 00_basis/
            └── HUB.gpkg                     ← Bestaand 3Di model
```
      
Voordat de bestanden in `01_source_data` worden bijgewerkt, moeten de huidige DAMO en HDB bestanden eerst worden gekopieerd naar `vergelijkingstool/input_data_old/`. Voor het model HUB ziet dat er als volgt uit:

De oude bestanden worden gekopieerd naar:

<p align="center">
<code>H:\02.modellen\HUB\01_source_data\vergelijkingstool\input_data_old</code>
</p>

Daarna moet de nieuwe DAMO en HDB export worden geplaatst in de map:
<p align="center">
<code>H:\02.modellen\HUB\01_source_data</code>
</p>

Vanuit deze map leest de vergelijkingstool automatisch de geactualiseerde gegevens in. Het bestaande 3Di model wordt gelezen vanuit de basisschematisatie:
<p align="center">
<code>H:\02.modellen\HUB\02_schematisation\00_basis</code>
</p>

De oude DAMO en HDB bestanden in `input_data_old` zijn nodig om de oorspronkelijke situatie vast te leggen. De nieuwe DAMO en HDBbestanden in `01_source_data` worden gebruikt als geactualiseerde invoer. Het bestaande model in `02_schematisation/00_basis` wordt gebruikt als referentie voor de vergelijking met het 3Di model.

Deze structuur is nodig voor zowel de vergelijking **`Damo Updated vs Damo Old`** als voor de vergelijking **`Damo Updated vs 3Di model`**. Als de bestanden niet op deze manier zijn opgeslagen, kan de vergelijkingstool de benodigde invoer niet correct vinden en zal de tool niet goed werken.
---

## **Handleiding**

De `vergelijkingstool` wordt gebruikt vanuit de QGIS-plugin en wordt vervolgens uitgevoerd vanuit JupyterLab.

De workflow bestaat uit twee hoofdonderdelen:

1. JupyterLab openen vanuit de QGIS-plugin.
2. De `vergelijkingstool` uitvoeren vanuit het notebook.

---

### 1. JupyterLab openen vanuit de QGIS-plugin

Het eerste deel van het proces wordt uitgevoerd vanuit het hoofdtabblad van de plugin.

![Workflow plugin](workflow_plugin.png)

#### **Stap 1. De modellenmap selecteren**

In het veld *“Modellen folder”* moet de hoofdmap worden geselecteerd waarin de modellen zijn opgeslagen. Deze map is de basismap van waaruit de plugin de beschikbare modellen zoekt.

#### **Stap 2. De polder of het model selecteren**

In het veld *“Polder”* moet de polder of het model worden geselecteerd waarmee gewerkt gaat worden. Nadat de modellenmap is geselecteerd, toont de plugin de beschikbare opties in deze lijst.

#### **Stap 3. De API keys controleren**

Voordat verder wordt gegaan, moet worden gecontroleerd of de benodigde *API keys* correct zijn ingesteld en opgeslagen in de juiste map. Deze API keys zijn nodig om de notebooks verbinding te laten maken met de vereiste services en correct uit te voeren.

#### **Stap 4. De Jupyter Notebook Server openen**

Klik vervolgens op de knop *“Open Jupyter Notebook Server”*. Deze knop start de Jupyter Notebook Server en opent JupyterLab in de standaardbrowser.

---

### 2. De vergelijkingstool uitvoeren vanuit JupyterLab

Zodra JupyterLab in de browser is geopend, moet het bestand worden geopend en uitgevoerd dat hoort bij de `vergelijkingstool`.

![Workflow notebook](workflow_notebook.png)

#### **Stap 1. Het bestand `06_vergelijkingstool.py` openen**

Zoek in het linkerpaneel van JupyterLab het bestand `06_vergelijkingstool.py`.

Om het bestand correct te laten werken, moet het met **Jupytext** als notebook worden geopend.

Doe dit als volgt:

* Klik met de rechtermuisknop op `06_vergelijkingstool.py`
* Selecteer **Open With**
* Selecteer **Jupytext Notebook**

Hiermee wordt het `.py` bestand geopend als een uitvoerbaar notebook binnen JupyterLab.

#### **Stap 2. Het notebook uitvoeren**

Zodra het bestand als notebook is geopend, klik je op de knop *Run*. Deze knop voert de hoofdcel uit en toont de grafische interface van de `vergelijkingstool` binnen het notebook.

#### **Stap 3. Het modelpad controleren**

In het veld *“Enter the Model folder path”* moet de locatie van het model waarmee gewerkt wordt worden gecontroleerd of ingevoerd. Dit pad moet overeenkomen met het model dat eerder in de QGIS-plugin is geselecteerd.

#### **Stap 4. Het type vergelijking selecteren**

Selecteer vervolgens de optie: **`Damo Updated vs 3Di model`**

Deze optie vergelijkt de geactualiseerde DAMO en HDB bestanden met het bestaande 3Di model. De nieuwe DAMO en HDB-export wordt automatisch gelezen vanuit de map `01_source_data`. Het bestaande 3Di model wordt gelezen vanuit de basisschematisatie van het model, op de volgende locatie:

<p align="center">
<code>H:\02.modellen\HUB\02_schematisation\00_basis</code>
</p>

Voordat de DAMO en HDB bestanden in `01_source_data` worden geactualiseerd, moeten de oude bestanden eerst worden gekopieerd of verplaatst naar de map:

<p align="center">
<code>H:\02.modellen\HUB\01_source_data\vergelijkingstool\input_data_old</code>
</p>

Deze structuur is noodzakelijk om de vergelijkingstool correct te laten werken. Als de mappenstructuur niet correct is ingericht, kan de `vergelijkingstool` de benodigde bestanden niet vinden en zal de tool niet goed werken.

# TODO WAAR STAAT HET MODEL? WORDT STANDAARD NAAR HET BASISMODEL GEKEKEN? WAT IS DAMO UPDATED? IS DAT NIEUW? BIJ DEZE OPTIE HEB JE DUS GEEN DAMO OLD NODIG? HET IS VERWARREND ALS DAMO NIEUW UIT DE SOURCE DATA KOMT EN HET MODEL UIT DE BASIS SCHEMATISATIE. DAN MOET JE DUS EEN MODELMAP HEBBEN WAARIN ZOWEL NIEUWE ALS OUDE GEGEVENS STAAN, DAT HEB JE NOOIT EN IS HEEL ERG VERWARREND. KAN DIT OOK ALLEMAAL IN DE APARTE MAP?

#### **Stap 6. De vergelijking uitvoeren**

Wanneer alle instellingen klaarstaan, klik je op de knop *“Run Comparison”*. De tool voert de vergelijking tussen de geselecteerde databases uit en genereert het outputbestand. De volledige locatie van het gegenereerde bestand wordt weergegeven in het veld *“Full path”*. Dit `.gpkg` bestand kan daarna in QGIS worden geopend om de gevonden verschillen te beoordelen.

---

## Interpretatie van de resultaten

De output van de `vergelijkingstool` moet niet alleen worden geïnterpreteerd als een lijst met fouten.  
De resultaten dienen als ondersteuning om te beoordelen of het bestaande model nog kan worden hergebruikt, gedeeltelijk moet worden geactualiseerd of beter opnieuw kan worden opgebouwd met recentere brongegevens.

De beoordeling moet zich vooral richten op:

- Het type verschil: `attribuut verschillen` of `structuurverschillen`.
- De classificatie van het verschil: `Warning` of `Critical Error`.
- De locatie van het verschil binnen het systeem.
- De mogelijke hydraulische invloed op het model.
- De complexiteit van een eventuele handmatige correctie.

Als algemene richtlijn geldt:

| Situatie | Mogelijke beslissing |
| --- | --- |
| Er zijn geen relevante verschillen | Het bestaande model hergebruiken |
| Er zijn vooral verschillen in attributen | Het bestaande model actualiseren |
| Er zijn kritieke structurele verschillen in belangrijke delen van het systeem | Overwegen om het model opnieuw op te bouwen |
| De verschillen liggen aan de randen van het model of zijn geïsoleerde gevallen | Beoordelen of ze handmatig kunnen worden gecorrigeerd |

Ook als er bijvoorbeeld veel verschillen in de attributen zijn kan het efficiënter zijn om het model opnieuw op te bouwen. De uiteindelijke beslissing moet door de modelleur worden genomen op basis van een inhoudelijke beoordeling van de lagen die in QGIS zijn gegenereerd. Voor een uitgebreidere beschrijving van het beoordelingsproces en de vastlegging van de beslissing, zie het criteria-document:

[Document met criteria voor de vergelijkingstool](https://corphhnk-my.sharepoint.com/:w:/g/personal/j_acostabarragan_hhnk_nl/IQAnmle5aVm8QIQ1-YdJs5yTATWDOvRhuQel_Nb1JKrNoP8?e=Lrqv8t)

Ten slotte kan het zijn dat de adviseur van watersystemen reden ziet om het model aan te passen of opnieuw op te bouwen op basis van de bestaande resultaten.

---

## Aanbevolen werkwijze in het kort

1. Controleer eerst of de gebruikte DAMO export recent is.
2. Open het resultaat in QGIS.
3. Beoordeel de belangrijkste verschillen.
4. Vergelijk de gemarkeerde objecten met de oorspronkelijke modelgegevens.
5. Bespreek twijfelgevallen met de inhoudelijk verantwoordelijke adviseur.
6. Documenteer welke verschillen moeten worden gecorrigeerd en welke kunnen worden geaccepteerd.

---

## Contact

Voor vragen over de inhoud van het model:

**Modelleur:** Juan Acosta  
**Team:** Hydrologie / HHNK

Voor technische problemen met de tool kan contact worden opgenomen met de beheerder van de vergelijkingstool.
