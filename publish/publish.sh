#!/bin/bash -e

if [[ (${#1} -eq 0) ]] ; then
    echo
    echo "[ERROR] missing PyPi repo"
    echo
    exit 1
fi


if [[ (${#2} -eq 0) ]] ; then
    echo
    echo "[ERROR] missing username"
    echo
    exit 1
fi

if [[ (${#3} -eq 0) ]] ; then
    echo
    echo "[ERROR] missing password"
    echo
    exit 1
fi

pypi_repo=$1
pypi_username=$2
pypi_password=$3

cd ..

api_version=$(cat __version__.py | grep __version__ |  awk '{split($0, a, "="); print a[2]}' | tr -d ' "')

# packages to install
pip install twine wheel pyOpenSSL
python setup.py sdist bdist_wheel

# upload
twine upload -u $pypi_username -p $pypi_password dist/*

cd ..

rm -f publish.sh