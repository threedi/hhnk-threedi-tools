#!/bin/zsh

# Stel het commando in
commando="$(dirname "$0")/python.sh"

# Voer het commando uit
"$commando" "$(dirname "$0")/../../run_hook.py" "$@"
