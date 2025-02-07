## Uitchecken en initialiseren model repository

De modellen staan in Azure devops. Inloggen bij Azure Devops kan via `https://dev.azure.com/hhnk/threedimodels`.
Modellen kunnen uitgecheckt worden met Github Desktop (of andere git tools of de command line). Modellen worden
per persoon uitgecheckt in de map `E:\github\<gebruikersnaam>\`.

### Azure devops

Op `https://dev.azure.com/hhnk/threedimodels` ga in de linker balk naar `repos`. Kies vervolgens boven in in de balk
met de pulldown het model dat je wilt hebben. Vervolgens verschijnen in het rechter deel van het scherm de bestanden.
Kies in dit deel van het scherm rechtsboven op `Clone`. Kies hier HTTPS en kopieer de link (deze begint met
`https://HHNK@dev.azure.com/HHNK/ThreediModels/`), deze kan zo in Github Desktop geplakt worden onder `repository URL`.
Klik vervolgens op 'Generate Git credentials' en kopieer de gebruikersnaam en wachtwoord. Deze kunnen in Github Desktop
geplakt worden onder `Username` en `Password` in de popup die verschijnt bij het clonen.

### Github Desktop

Open Github Desktop en kies `File` -> `Clone repository` en kies `URL`. Plak de link in `repository URL` en
kies de locatie waar het model moet komen te staan (in `E:\github\<gebruikersnaam>\3di\<model_name>`) Klik op `Clone` en
vul de gebruikersnaam en wachtwoord in die je van Azure Devops hebt gekopieerd.

<< verbinden kan ook via ssh met ssh-keys. Werkt dat ook lekker onder Windows? >>

### Initialiseren van een model repository

Na het uitchecken van een model repository is het nodig om deze te initialiseren. Dit voegt de benodigde git hooks
(in de folder .git/hooks), initialiseert git LFS (Large File Storage) en maakt of voegt toe aan de .gitattributes en
.gitignore file.

Start de command prompt en ga naar de root van de model repository. Voer vervolgens het volgende commando uit:

```shell
# vanuit de root van de model repository:
<path to hhnk-threedi-tools repo>\hhnk_threedi_tools\git_model_repo\bin\initialize_repo.bat
# of vanaf een andere locatie:
<root of hhnk-treedi-tools repo>\hhnk_threedi_tools\git_model_repo\bin\initialize_repo.bat <path to model repo>
```

## Nieuwe modellen repo (RepoAdmins)

### Aanmaken nieuwe modellen repo in Azure Devops

Ga naar `https://dev.azure.com/hhnk/threedimodels` en kies in de linker balk 'Repos'. Kies vervolgens boven in de balk
in de pulldown met de modellen voor '+ new repository'. Kies een naam voor de repository en kies voor 'Create'.

### Aanmaken nieuwe modellen structuur

<< zorg dat de instellingen goed staan >>

Clone de nieuwe repository naar een lokale map (zie boven) en initialiseer deze (zie boven). Run vervolgens de
vanuit de QGIS HHNK 3di plugin de tool << xxxxx >> om de structuur van de modellen repository aan te maken.

### Toegangsrechten en policies instellen

Toegangsrechten en policies van de repository kunnen worden ingesteld door in Azure Devops boven in de balk
in de pulldown met de modellen te gaan naar `Manage repositories` (<< vanaf welke rechten ??>>). Kies hier het model

#### Toegangsrechten

<< staan deze standaard al goed >>

In Azure Devops kan ingesteld worden dat de main branch niet direct gemerged kan worden, maar dat dit altijd via
een pull request moet gebeuren. Dit kan ingesteld worden in de policies van de repository.

## Werken met modellen repo

### Branches / scenarios

Als aan het (basis)model wordt gewerkt, dan gebeurd dit in een branch.
Branches van het basismodel kunnen als deze af zijn via een `pull request` worden samengevoegd met de main branch.
<< hoe werkt dit precies?? en hoe ziet dit eruit in Azure? >>

Afspraken over naamgeving van branches en scenarios:

- werk branches beginnen met `work_<basismodel>_<naam>`
- de scenario branches (als het geen workbranch is) beginnen met `scenario_<naam>`

### Main branch

De main branch is de branch waar het laatste basismodel in is opgeslagen.

<< instellen hoe naar main branch kan worden gemerged >>

### Scenario branches

<< kunnen zelfde bescherming wat betreft merge krijgen als main branch >>

De scenario

### Pull request

<< verplichten >>

### Pushen en pullen en commit messages

### Mergen van branches

### Voorbeeld van branch structuur die op den duur kan ontstaan












