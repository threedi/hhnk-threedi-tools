#!/bin/bash
set -e  # Exit on error

# Optional: echo commands for debugging
# set -x

# Check if the directory ../hhnk-research-tools exists
if [ ! -d "../hhnk-research-tools" ]; then
    git clone https://github.com/HHNK/hhnk-research-tools/ ../hhnk-research-tools
fi

# Install the package in editable mode without dependencies
pip install --no-deps -e ../hhnk-research-tools