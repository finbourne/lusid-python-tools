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

echo "checking if version=$api_version already exists"

status_code=$(curl -L --write-out %{http_code} --silent --output /dev/null --header 'Accept: application/json' https://pypi.org/pypi/lusidtools/$api_version/json)

if [[ "$status_code" -eq 200 ]] ; then
    echo "### WARNING: lusidtools $api_version already exists, skipping publish ###"
    exit 0
fi

# packages to install
python setup.py sdist bdist_wheel

# upload
twine upload -u $pypi_username -p $pypi_password dist/*