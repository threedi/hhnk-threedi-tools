# Inrichting Azure Devops

## Azure Devops 3di project

Binnen de omgeving van HHNK is een aparte project `ThreeDiModels` aangemaakt, te bereiken via
[https://dev.azure.com/hhnk/ThreeDiModels](https://dev.azure.com/hhnk/ThreeDiModels).

## Gebruikersgroepen en rechten

<< vraag: wat is een team? >>
<< Waar kan een gebruiker zien welke rechten hij/ zij heeft >>

De volgende rechten worden onderscheiden:

- **Readers**. << heeft nu niemand?? >>  Deze kan:
    - ....
- **Contributors**. Iedereen die lid is van de groep `[HHNK]\HHNK-TrheediModels-Basic` en/of
  `[HHNKThreediModels]\ThreediModels Contributors Team`. Deze kan naast de rechten van de Readers:
    - ....
- **Repo Administrators**. Iedereen die lid is van de groep `[HHNKThreediModels]\ThreediModels Repo Team`. Deze kan
  naast de rechten van de Contributors:
    - repo's aanmaken
    - policies zetten
    - force push (rewrite history, delete branches and tags)
- **Project Administrators**. Deze kan naast de rechten van de Repo Administrators:
    - gebruikersrechten beheren
    - repo's verwijderen
    - repo's renamen

Speciaal: degene die de repository aanmaakt krijgt extra rechten (o.a. rename en verwijderen van repo's ?!). Deze
gebruiker staat dan onder de permissions van de repository. Door deze daar te verwijderen (lukt door eerst een
permission aan te passen?!) kan de gebruiker weer teruggezet worden naar de standaard rechten.

Teams

Default krijg de gebruiker rechten conform de groep waarin hij/ zij zit.
Per repository kunnen de rechten worden aangepast.

--opmerkinge azure--

Test gebied 1
Dit is een van de test repo's van de Azure DeOps HHNK / ThreeDiModels, waarmee wordt getest (en beschreven):

inrichting repo.
inrichting/ werking voor gebruiker.
de werking van de speciale inrichting om transparant te werken met 3di modellen (onderdeelnva hhnk-threedi-tools).
Azure repo is te bereiken via: dev.azure.com/HHNK/ThreediModels/ 

Inrichting repo
ProjectAdmins die mogen de gebruikers en rechten toekennen
Toegang kan voor elke repo worden ingesteld. Groepen zijn alleen in te stellen voor alle repo's.
Alle contributers mogen een repo voor een gebiedsmodel aanmaken?! --> kan ingesteld worden - wat is handig? Niet: wildgroei?!
Toegang externen nog uitzoeken.
De hoofdbranch heet altijd 'main' (wordt afgedwongen) en commits mogen niet direct op de main, maar via een branch en dan
een pullrequest.
Er is nog geen standaard merge bepaald. Bepalen wat handig is wat betreft informatie die voorhanden blijft en hoeveelheid
gegevens (squashen of niet?!)
Nieuw gebiedmodel. De stappen:

.....
Inrichting bij gebruiker/ op server
Elke gebruiker kan zelf een gebiedsmodel clonen. Dit (op de OTA155) gebeurd in e:\Github<gebruikersnaam>
Clonen kan via Github Desktop. Kies hier ónder 'add'--> 'clone repository' en dan het tabblad URL. De url is in te vinden
in Azure. Kies de repo (klik links op repo en dan bovenin welke repo). vervolgens staat onder Clone de benodigde informatie.
Kies de HTTPS en voer die in onder Github. Vervolgens kan op de sirte via 'Generate Git Credentials' de gegevens worden
gevonden het inloggen.
Werken met speciale plugin