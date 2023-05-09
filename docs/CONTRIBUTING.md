# Introduction

Welcome and thank you for considering contributing to the Python tools for LUSID repository.  We're delighted to have you as part of our community.

Following these guidelines will help us to manage the contribution process effectively for everyone involved. It communicates that you respect the time of our Core Team managing and developing this open source project. In return, we will reciprocate that respect by addressing your issue, assessing changes, and helping you finalize your pull requests.

## Getting in touch

**If you find a security vulnerability, please do NOT open a GitHub issue. Any security issues should be submitted directly to soc@finbourne.com.**

Please don't use the GitHub issue tracker for support questions. For help on using LUSID or Luminesce, please contact support@lusid.com.

# Contributing

We welcome contributions from our community. 

There are many ways to contribute, from writing code samples and fixing bugs, to submitting bug reports and feature requests. This section guides you through submitting a contribution to this repo.

## Code of conduct

FINBOURNE has adopted the Contributor Covenant as its [code of conduct](docs/CODE_OF_CONDUCT.md), and we expect contributors to adhere to it. Please read the full text so that you can understand what actions will and will not be tolerated.


## License

By contributing to this FINBOURNE repo, you agree that your contributions will be licensed under its [MIT license](LICENSE.md).
  

## Reporting bugs
If you think you have found a bug:

- Make sure you are testing against the latest version of the tools, as your issue may have already been fixed.
- Search all open and closed issues to see if your issue has already been answered.

If you are unable to find your issue, please raise an issue using the provided template. Try to provide as much information as possible that will help us to reproduce the problem.

</br>

## Submitting changes

**If you have a change that you would like to contribute, please find or open an issue about it first.** You might find that somebody is already working on the issue, or there may be particular issues that you should know about before implementing the change.

To submit your change:

1. Fork our repository to your own GitHub account (this [GitHub guide](https://help.github.com/en/articles/fork-a-repo) describes how to do this).
2. Clone the project to your local machine.
3. Create a new branch and clearly title it with the change.
4. Make your changes and ensure all tests pass.
5. Push your changes to your fork. We prefer for your changes to be squashed into a single commit.
6. Submit a [pull request](https://help.github.com/en/articles/about-pull-requests) to the `master` branch of our repository.
7. The Core Team will add a “/LGTM” comment to trigger the build (note this does not approve the pull request). We will then review your code and leave any comments for you to address before we merge your change into the `master` branch.

## Code review process

The Core Team reviews pull requests on a regular basis and will give feedback on the corresponding issue in the repo. Please note:

- A build will run as part of your pull request; at a minimum, this needs to pass before your contribution is accepted.
- After feedback has been given, it is expected that you respond within two weeks. The pull request may be closed if it does not show any signs of activity.

## Working with the code

We use [poetry](https://python-poetry.org/) to manage dependencies, build and package lusidtools. Install poetry using pip:

    pip install poetry

To ensure you have all the correct packages, and an editable install of lusidtools, run `poetry install` at the root of the project.

To run tests and code coverage, ensure Python 3.11 is installed and run `tox`.

To update the installed packages, run `poetry update`. This will update the poetry.lock file, which must be committed to source control.
