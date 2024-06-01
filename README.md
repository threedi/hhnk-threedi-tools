# HHNK 3DI Tools

[![tests](https://github.com/threedi/hhnk-threedi-tools/actions/workflows/tests-conda.yml/badge.svg)](https://github.com/threedi/hhnk-threedi-tools/actions/workflows/tests-conda.yml)
[![coverage](https://img.shields.io/codecov/c/github/threedi/hhnk-threedi-tools)](https://codecov.io/github/threedi/hhnk-threedi-tools)
[![PyPI version](https://badge.fury.io/py/hhnk-threedi-tools.svg)](https://badge.fury.io/py/hhnk-threedi-tools)
---

Tools used for analysing 3Di-models.

Used in qgis with this interface:
https://github.com/threedi/hhnk-threedi-plugin

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

```shell
# `.env` file in the root of the hhnk-threedi-tools repo
CONDA_CMD=micromamba # or conda or the full path to the conda executable
ACTIVE_ENV=hhnk_threedi
```
