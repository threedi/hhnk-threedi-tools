# 1Achtergrond

Het Hoogheemraadschap Hollands Noorderkwartier (HHNK) heeft binnen het programma Bescherming Wateroverlast Noorderkwartier (BWN) een groot aantal poldermodellen opgebouwd. De basismodellen worden grotendeels automatisch opgebouwd uit beheergegevens. Na de opbouw van de basismodellen worden deze nog door hydrologen verder verbeterd en uitvoerig getest voordat de modellen ingezet worden voor de verschillende studies.

Door wijzigingen in het watersysteem zijn de modellen na verloop van tijd niet meer helemaal actueel. Denk hierbij aan gebiedsontwikkeling en aanpassingen van kunstwerken, peilgebieden en profielen. Na verloop van tijd is het belangrijk de modellen te updaten naar deze nieuwe werkelijkheid. Hoewel het updaten van modellen voor een groot deel geautomatiseerd is, blijft het noodzakelijk dat een modelleur deze modellen goed analyseert en waar nodig verbetert. Omdat hier de nodige tijd bij komt kijken is het belangrijk om het moment waarop een model wordt geüpdatet goed te kiezen. Te vroeg en een model wordt voor niets opnieuw opgebouwd, te laat en er worden studies uitgevoerd met een verouderd model.

Om verschillen tussen het model en de huidige situatie inzichtelijk te maken is de 'vergelijkingstool' ontwikkeld.

# 2Beschrijving

De vergelijkingstool heeft twee hoofdfuncties:

- Brondata/Brondata-vergelijking. De 3Di modellen zijn automatisch opgebouwd uit brondata. De gebruikte brondata is door HHNK opgeslagen samen met de modellen. Door de gebruikte brondata (DAMO en HDB) te vergelijken met recente brondata krijgt men inzicht in de wijzigingen in de buitenruimte.
- Model/Brondata-vergelijking. Soms zijn modellen handmatig nog geüpdatet of verbeterd, of zijn er andere aanpassingen gedaan. Door het model met brondata te vergelijking komen deze verschillen aan het licht.

De vergelijkingen die worden uitgevoerd zijn uit te splitsen in twee categorieën:

- Vergelijking op code.
  - Voor de vergelijking brondata/brondata worden in beide datasets de lagen met dezelfde naam met elkaar vergeleken. Binnen deze laag wordt gekeken naar de "code" kolom om de data van beide datasets aan elkaar te koppelen. Per aanwezige code wordt aangegeven of deze alleen in dataset A, B of in beide (AB) voorkomt.
  - Voor de vergelijking van model met brondata wordt gekeken naar de kunstwerken. Omdat deze in de modellen niet strikt gescheiden zijn per laag (duikers worden soms als culvert en soms als orifice geschematiseerd) worden alle lagen met kunstwerken beschouwd, en worden de kunstwerken gegroepeerd op kunstwerkcode. Zo worden alle kunstwerken die beginnen met 'KDU' vergeleken in de uitvoerlaag 'KDU'. Hetzelfde gebeurt met de andere type kunstwerken. Per kunstwerkcode wordt aangegeven of deze zich alleen in DAMO, alleen in het model of in beide bevindt.
- Vergelijking op attribuut
  - Door middel van configuratiebestanden kunnen attribuutvergelijkingen worden opgesteld. Dit zijn regels die verschillende kolommen in de data met elkaar vergelijken. Complexere numerieke vergelijkingen kunnen ook worden opgebouwd (verschil/som/minimum/maximum/vermenigvuldiging/deling van kolommen of waardes).
  - Per attribuutvergelijking wordt een kolom toegevoegd met de uitkomst van de vergelijking en een kolom met de prioriteit van de opgegeven attribuutvergelijking ("critical", "warning", "info")
  - Bij numerieke vergelijkingen wordt ook een kolom toegevoegd of de waardes in 1 van de twee datasets niet-ingevulde velden betreft.

# 3Invoer

- Brondata:
  - DAMO FileGeoDatabase
  - Hydrologen-database (HDB)
- Model:
  - 3Di modelschematisatie (sqlite)
- Vertaalbestanden
  - JSON-bestand(en) met vertaalregels om tabellen en kolommen ter hernoemen bij de import
- Attribuutvergelijkingen
  - JSON-bestand(en) met verschillende attribuutvergelijkingsregels
- Styling-bestanden in styling map
  - QML-bestanden met de styling voor de export naar GeoPackage. Een standaard-styling wordt aangenomen, maar als er een QML-bestand in de styling map wordt gevonden met dezelfde naamgeving als de naam van een export-laag, dan wordt deze styling aangenomen.

# 4Uitvoer

- Een GeoPackage met de resultaten van de brondata/brondata vergelijking, met alle lagen die in beide datasets aanwezig waren. Per laag statistieken over aantal, lengte of oppervlak van objecten in beide datasets.
- Een GeoPackage met de resultaten van de model/brondata vergelijking, uitgesplitst naar kunstwerktypes. Per laag statistieken over aantal, lengte of oppervlak van objecten in beide datasets.