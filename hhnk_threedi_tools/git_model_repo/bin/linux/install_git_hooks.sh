#!/bin/bash

# Stel het commando in
commando="$(dirname "$0")/python.sh"

# Voer het commando uit
"$commando" "$(dirname "$0")/../../install_hooks.py" "$@"
