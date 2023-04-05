A set of useful Python utilities and scripts for interacting with the LUSID Api.

The commands can be run directly using the scripts in the root directory.

## Authentication
Authentication can be done in various ways:

Use a secrets file, see https://support.lusid.com/getting-started-with-apis-sdks

`./portfolios [SCOPE] --secrets-file secrets.json`

Use a token obtained from lusid.com

`./portfolios [SCOPE] --env token [DOMAINNAME or LUSID URL] [TOKEN]`

`./portfolios [SCOPE] --env token`   # This will prompt for the domain name/LUSID Url and token

## Installation

1. Pull down repo: git@gitlab.finbourne.com:john.higgins/Lusid-Python-Tools.git
2. install pip3: `sudo apt install python3-pip`
3. Install LUSID Python SDK: `pip3 install lusid-sdk`
4. Install pandas: `pip3 install pandas`

## Standard options

`--stat`

## Recommended
Install a nice WSL terminal https://github.com/mintty/wsltty/releases
