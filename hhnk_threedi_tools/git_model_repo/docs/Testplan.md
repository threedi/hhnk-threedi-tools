# Software

Voor push naar main:
- [ ] Code opmaak (ruff)
- [ ] Testen werken
- [ ] Test coverage
- [ ] Installatie modellen repo volgens installatie handleiding

Uitrol:
- [ ] Centrale installatie op de OTA155 aanwezig
- [ ] Centrale code werkt voor alle gebruikers

# Inrichting Azure Devops

- [X] Aanmaken nieuwe modellen repo (met handleiding)
    - [X] Stappen te doorlopen
    - [ ] 3di plugin maakt structuur aan
- [X] Toegangsrechten instellen (met handleiding)
    - [X] Staan deze standaard al goed?
    - [X] Zijn de gebruikersgroepen goed ingesteld? (conform beschrijving)
- [ ] Testen toegangsrechten
- [ ] Externen toegang geven tot modellen repo (met handleiding)

# Werken met modellen repo

- [ ] Uitchecken en initialiseren model repository (met handleiding)
    - [X] Uitchecken model repository
    - [X] Initialiseren model repository
        - --> geen dingen dubbel?? - staat er nu soms meerdere keren in.
- [ ] Werken met branches / scenarios (met handleiding)
    - [X] Pushen / pullen
        - [X] Worden bestanden goed omgezet
            - --> traag, ook als er geen wijzigingen zijn, dan toch opnieuw omgezet
            - --> geopackage van 20 mb, 30 mb geojson doet er 70 seconden over, traag:
                - In welke stap zit dit
                - Kan dit sneller?
        - [ ] Worden bestanden goed opgeslagen
            - --> als geopackage wordt gecommit, worden json bestanden gemaakt. Er verschijnt een melding dat er
              bestanden zijn toegevoegd. Deze is niet helemaal duidelijk. json bestanden worden niet direct meegenomen.
              commit gaat gewoon door
            - Soms melding van missende commit message, maar na omzetten gebeurd dit niet altijd
            - wordt 2 keer uitgevoerd??
        - [ ] Worden bestanden goed teruggezet
        - [ ] Gaat het goed met de omvang van de bestanden en LFS
    - [ ] Aanmaken nieuwe branch
    - [ ] Wisselen tussen branches
    - [ ] Mergen van branches
        - [ ] Gaat samenvoegen goed van bestanden
        - [ ] Gaat samenvoegen goed van LFS bestanden
        - [ ] Gaat pull request goed in Azure Devops (met handleiding)
    - [ ] Verwijderen van branches
-


- Rechten instellen --> repo Admins

- Modellen structuur aanpassen
- 3di hhnk plugin staat nu nog hard verwezen naar gezamenlijk. Moet naar eigen repo (instelling project path)

- mamba init??
-

ignore:
tif.aux.xml

modelbuilder -
--> uitkomst 01_source_data
--> 02_schematisation\00_basis
\rasters
\<name>.sqlite

--> toevoegen aan modelbuilder?:
model_settings.xlsx
model_settings_default.json

verschillende scenario's
--binnen 3di moet dem naam hetzelfde zijn?!

--> ignore file met _

01_schematisation --> geopackage
02_schematisation\rasters_verwerkt?

-- source data diffen?

--> doorgaan bij fout. aan eind alle fouten tegelijk printen.

--> originele hooks bewaren, opnieuw te runnen

--> timen waar traagheid in zit.

