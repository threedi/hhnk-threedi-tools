# HHNK Threedi Tools

[![pytests](https://github.com/threedi/hhnk-threedi-tools/actions/workflows/pytests_threedi_tools.yml/badge.svg)](https://github.com/threedi/hhnk-threedi-tools/actions/workflows/pytests_threedi_tools.yml)
[![coverage](https://img.shields.io/codecov/c/github/threedi/hhnk-threedi-tools)](https://codecov.io/github/threedi/hhnk-threedi-tools)
[![PyPI version](https://badge.fury.io/py/hhnk-threedi-tools.svg)](https://pypi.org/project/hhnk-threedi-tools/)
[![Code style](https://img.shields.io/badge/code%20style-ruff-D7FF64)](https://github.com/astral-sh/ruff)
---

Tools used for analysing 3Di-models.

Used in QGIS with this plugin:
[https://github.com/threedi/hhnk-threedi-plugin](https://github.com/threedi/hhnk-threedi-plugin)
**Documentation**: [https://threedi.github.io/hhnk-threedi-plugin](https://threedi.github.io/hhnk-threedi-plugin)


# Development Setup
Development is done in VS-Code with a Pixi environment.
To setup the env open the cloned hhnk-threedi-tools folder in VS-Code or in a terminal in the folder and execute:\
`pixi install`\
`pixi run postinstall` -> does a local install of hhnk-research-tools and HyDAMOValidatieModule. Also installs githooks to do ruff checks before push.

Use `pixi shell` to open the command prompt with the python enviroment initialized.

# Run tests locally
To run test locally:
* Copy .env.example and rename to symple .env. This will ensure tests that required database access are skipped.
* Run `pixi run tests` in terminal. Make sure postinstall has been run.

2025-06 -> migration towards python 3.12.
Not all checks work yet on 3.12. The github checks therefore run on py39

Tests this locally with
`pixi run -e py39 tests`

Not all checks work yet on 3.12. The github checks therefore run on python 3.9 (pixi environment `py39`)

2025-09: Beware that the py39 test environment uses the main from `hhnk-research-tools` and, not local editable install.

To run tests locally on python 3.9 use
`pixi run -e py39 tests`


# Model Repository

## Initialize a model repository

Run this after cloning a model repository or creating a new one. This will add the nescessary git hooks (to .get/hooks),
initialize git LFS (Large File Storage), create or append to the .gitattributes and .gitignore file.

### Windows

```shell
# from the root of the model repository
<root of hhnk-treedi-tools repo>\hhnk_threedi_tools\git_model_repo\bin\initialize_repo.bat
# or from other path
<root of hhnk-treedi-tools repo>\hhnk_threedi_tools\git_model_repo\bin\initialize_repo.bat <path to model repo>
``` 

### Or on Mac/ Linux

```shell
# from the root of the model repository
<root of hhnk-treedi-tools repo>/hhnk_threedi_tools/git_model_repo/bin/linux/initialize_repo.sh
# or from other path
<root of hhnk-treedi-tools repo>/hhnk_threedi_tools/git_model_repo/bin/linux/initialize_repo.sh <path to model repo>
```



# Jupyter setup
The interactive window in vs-code can be very unreliable when starting up a session.
Often leading to long waiting times or no connection to the kernel at all.

This can be circumvented by hosting a jupyter lab server and connecting the interactive window to this server instead.

## Initial Setup
1. Add to pixi.toml;
```
[activation.env]
PYTHONPATH = "./"
[tasks]
lab = "jupyter lab --no-browser"
```

2.  Generate a config file with;\
`pixi run jupyter lab --generate-config`

3. Navigate to `%USERPROFILE%\.jupyter` (copy paste this in windows explorer)
Open the `jupyter_lab_config.py` file and add these lines, changing the port and token;

```
c.ServerApp.port = <INSERT_YOUR_PORT_HERE> # bezette poorten (aanvullen) = [8901]
c.IdentityProvider.token = "<INSERT_RANDOM_TOKEN_HERE>"
```

4. In terminal run the command:\
`pixi run lab`

5. In interactive window top right, select a different python kernel\
-> Click: Select Another Kernel...
-> Click: Existing Jupyter Server...
-> Fill url: http://127.0.0.1:8901/lab !! use the port from jupyter_lab_config
-> Fill password: <<>> enter password from jupyter_lab_config.py
-> 127.0.0.1:<<8901>> # Add port to the name for easier selection
-> Click on the python3 (ipykernel)


## Setup (every time)
The jupyter server needs te be running, do this with;
1. In terminal run the command:\
`pixi run lab`