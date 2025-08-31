#!/bin/zsh

if [ -z "$1" ]; then
  user_dir=$(pwd)
else
  user_dir="$1"
fi

# exit if script fails
set -e

$(dirname "$0")/sub/install_git_lfs.sh "$user_dir"
$(dirname "$0")/sub/install_git_hooks.sh "$user_dir"

echo "Git hooks and LFS installed"
