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

