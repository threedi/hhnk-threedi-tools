#!/bin/zsh


# Als argument leeg is, neem dan de werkdirectory van de gebruiker
if [ -z "$1" ]; then
  user_dir=$(pwd)
else
  user_dir="$1"
fi

# exit if script fails
set -e

$(dirname "$0")/check_dir_is_git_repo.sh "$user_dir"

# Voer het commando uit
# voeg directory toe vanuit waar de script wordt aangeroepen
"$(dirname "$0")/../python.sh" "$(dirname "$0")/../../../install_hooks.py" "$user_dir"
