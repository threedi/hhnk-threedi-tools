#!/bin/bash
set -e  # Exit on error

# Optional: echo commands for debugging
# set -x

# Check if the directory ../HyDAMOValidatieModule exists
if [ ! -d "../HyDAMOValidatieModule" ]; then
    git clone https://github.com/HHNK/HyDAMOValidatieModule/ ../HyDAMOValidatieModule
fi

# Install the package in editable mode without dependencies
pip install --no-deps -e ../HyDAMOValidatieModule