#!/bin/bash -e

if [[ (${#1} -eq 0) ]] ; then
    echo
    echo "[ERROR] missing username"
    echo
    exit 1
fi

if [[ (${#2} -eq 0) ]] ; then
    echo
    echo "[ERROR] missing password"
    echo
    exit 1
fi

pypi_username=$1
pypi_password=$2

api_version=$(cat __version__.py | grep __version__ |  awk '{split($0, a, "="); print a[2]}' | tr -d ' "')

# packages to install
pip install twine wheel pyOpenSSL
python setup.py sdist bdist_wheel

# upload
twine upload -u $pypi_username -p $pypi_password dist/*