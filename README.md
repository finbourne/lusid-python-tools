# Python tools for LUSID

This package contains a set of utility functions and installs a Command Line Interface (CLI) for interacting with [LUSID by FINBOURNE](https://www.finbourne.com/lusid-technology). To use it you'll need a LUSID account. [Sign up for free at lusid.com](https://www.lusid.com/app/signup)

![LUSID by Finbourne](https://content.finbourne.com/LUSID_repo.png)

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

### Testing it works

Once you have an LUSID account you will need to create a developer secrets file.  The steps to do this are covered in this [article](https://support.finbourne.com/getting-started-with-apis-sdks)

Check that the python `bin` folder is on your search path, you may have received a warning for this when you installed the tools above.   

Run the command below, substituting your secrets file path for the one below.,

``` sh
lusidtools instr_id --secrets-file ~/.lusid/dev-secrets.json
```

This should run and return a list if configured instrument identifier types.

## Contributing

We welcome community participation in our tools. For information on contributing see our article [here](https://github.com/finbourne/lusid-python-tools/tree/master/docs)

## Reporting Issues
If you encounter any issues please report these the Github [issues page](https://github.com/finbourne/lusid-python-tools/issues).

## Upgrading

To upgrade lusidtools run one of the commands below 

```sh
$ pip install lusidtools -U
```

or

```sh
$ pip install lusidtools -U --user
```
