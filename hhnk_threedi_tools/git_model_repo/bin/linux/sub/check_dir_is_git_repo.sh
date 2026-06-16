#!/bin/zsh

user_dir="$1"

# check if directory exists, otherwise ask if it should be created.
if [ ! -d "$user_dir" ]; then
    echo "Directory $user_dir does not exist, create it first and initialize the directory as git repository"
    exit 1
fi

# check if git is initialized, otherwise ask if it should be created.
if [ ! -d "$user_dir/.git" ]; then
    echo "Directory $user_dir is not a git repository, initialize it first"
    exit 1
fi
