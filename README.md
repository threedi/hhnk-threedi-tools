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
Not all checks work yet on 3.12. The github checks therefore run on python 3.9 (pixi environment `py39`)

2025-09: Beware that the py39 test environment uses the main from `hhnk-research-tools` and, not local editable install.

To run tests locally on python 3.9 use
`pixi run -e py39 tests`