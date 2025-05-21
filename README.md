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

# Local installation/ development

## Install

```shell
# from root of this repo
conda env create -f envs\environment_test.yml
```

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

### Configure .env file

configure a `.env` file in the root of the hhnk-threedi-tools repo, with the content:

```
# `.env` file in the root of the hhnk-threedi-tools repo
CONDA_CMD=mamba
ACTIVE_ENV=threedipy
```

```
mamba init
```
