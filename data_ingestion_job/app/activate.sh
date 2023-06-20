#!/usr/bin/env bash
if [ "$0" = "$BASH_SOURCE" ]; then
    echo "Error: Script must be sourced"
    exit 1
fi

python -m venv ./venv &&
source ./venv/bin/activate &&
pip install --upgrade pip &&
pip install -r requirements.txt