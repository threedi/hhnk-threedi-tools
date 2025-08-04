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
Development is done in vs-code with Pixi environment.
To setup the env;
in vscode, command prompt
`pixi install -e dev`
`pixi shell -e dev`
`pixi run postinstall` -> installs hrt, validatiemodule and githooks


2025-06 -> migration towards python 3.12.
Not all checks work yet on 3.12. The github checks therefore run on py39
Tests this locally with
`pixi run -e py39 tests`