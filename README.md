![LUSID_by_Finbourne](./resources/Finbourne_Logo_Teal.svg)

# Python tools for LUSID

This package contains a set of utility functions and a Command Line Interface (CLI) for interacting with [LUSID by FINBOURNE](https://www.finbourne.com/lusid-technology). To use it you'll need a LUSID account. [Sign up for free at lusid.com](https://www.lusid.com/app/signup)


![PyPI](https://img.shields.io/pypi/v/lusidtools?color=blue)
![Daily build](https://github.com/finbourne/lusid-python-tools/workflows/Daily%20build/badge.svg) 
![Build and test](https://github.com/finbourne/lusid-python-tools/workflows/Build%20and%20test/badge.svg)
![Commit hook](https://github.com/finbourne/lusid-python-tools/workflows/commit-hook/badge.svg)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=finbourne_lusid-python-tools&metric=alert_status)](https://sonarcloud.io/dashboard?id=finbourne_lusid-python-tools)

For more details see the lusid-python-tools [wiki](https://github.com/finbourne/lusid-python-tools/wiki).

## Installation

The PyPi package for lusid-python-tools can installed globally on your machine using the following command:

```sh
$ pip install lusidtools
```

or if you are running as a non privileged user you may prefer to install specifically for your user account:

```sh
$ pip install --user lusidtools
```

## CLI usage

You will need to create a secrets file and supply its location using the `secrets-file` parameter (or alternatively lusidtools will pick up a file called `secrets.json` in the current directory).  The steps to do this are covered in [Getting started with the LUSID API and SDKs](https://support.finbourne.com/getting-started-with-apis-sdks).

Make sure that the Python `bin` folder is on your search path before trying the following examples.  This is typically found under following locations:

* Windows: C:\Users\[userid]\AppData\Local\Programs\Python\[version]
* macOS: /Users/[userid]/Library/Python/[version]/bin
* Linux: /usr/local/bin/python

To see a full list of the available commands, run the following:

```sh
lusidtools --help
```

### Examples:

#### List configured instrument identifier types

``` sh
lusidtools instr_id --secrets-file /path/to/secrets.json
```

#### List instruments

``` sh
lusidtools instr_list -l 10  --secrets-file /path/to/secrets.json 
```

#### List scopes

``` sh
lusidtools scopes  --secrets-file /path/to/secrets.json
```

#### List portfolios in a scope

``` sh
lusidtools portfolios "<scope>"  --secrets-file /path/to/secrets.json 
```

#### List holdings

```sh
lusidtools hld "<scope>" "<portfolio-code>"  --secrets-file /path/to/secrets.json 
```

#### List transactions

```sh
lusidtools txn "<scope>" "<portfolio-code>"  --secrets-file /path/to/secrets.json 
```

#### Reconcile holdings

```sh
lusidtools rec \
  "<scope-left>" "<portfolio-left>" "YYYY-MM-DD" \
  "<scope-right>" "<portfolio-right>" "YYYY-MM-DD"  \
  --secrets-file /path/to/secrets.json 
```

You can reconcile a portfolio against itself by specifying the same values for `<scope-left>` / `<scope-right>` and `<portfolio-left` / `<portfolio-right`> and then providing different effective dates.

## Contributing

We welcome contributions from our community. See our [contributing guide](docs/CONTRIBUTING.md) for information on how to contribute to and report issues with the Python tools for LUSID repository.

## Upgrading

To upgrade lusidtools run one of the commands below 

```sh
$ pip install lusidtools -U
```

or

```sh
$ pip install lusidtools -U --user
```
