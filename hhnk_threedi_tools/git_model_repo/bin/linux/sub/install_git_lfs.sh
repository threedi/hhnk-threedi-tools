#!/bin/zsh

if [ -z "$1" ]; then
  user_dir=$(pwd)
else
  user_dir="$1"
fi

# exit if script fails
set -e

$(dirname "$0")/check_dir_is_git_repo.sh "$user_dir"

# make sure LFS is installed
git lfs install --force

path="$user_dir/.gitattributes"
# create an .gitattributes if not exists yet
# put it in the directory where the script is run

if [ ! -f "$path" ]; then
    echo "Creating .gitattributes file"
    # add .nc, .tiff, .gpkg, .sqlite, .xlsx to LFS
    echo "*.nc filter=lfs diff=lfs merge=lfs -text" > "$path"
    echo "*.tiff filter=lfs diff=lfs merge=lfs -text" >> "$path"
    echo "*.gpkg filter=lfs diff=lfs merge=lfs -text" >> "$path"
    echo "*.sqlite filter=lfs diff=lfs merge=lfs -text" >> "$path"
    echo "*.xlsx filter=lfs diff=lfs merge=lfs -text" >> "$path"

    git add "$path"
    git commit -m "Add .gitattributes file for LFS"
else
    echo ".gitattributes file already exists, make sure it contains the correct LFS filters"
fi
