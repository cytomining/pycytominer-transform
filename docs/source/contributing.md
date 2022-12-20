# Contributing

First of all, thank you for contributing to pycytominer-transform! 🎉 💯

This document contains guidelines on how to most effectively contribute to the codebase.

If you are stuck, please feel free to ask any questions or ask for help.

## Code of conduct

This project is governed by our [code of conduct](code_of_conduct.md).
By participating, you are expected to uphold this code.
Please report unacceptable behavior to cytodata.info@gmail.com.

## Quick links

- Documentation: <https://cytomining.github.io/pycytominer-transform/>
- Issue tracker: <https://github.com/cytomining/pycytominer-transform/issues>
- Package Dependencies (via [Poetry configuration](https://python-poetry.org/docs/pyproject/)): <https://github.com/cytomining/pycytominer-transform/blob/main/pyproject.toml>

## Process

### Bug reporting

We love hearing about use-cases when our software does not work.
This provides us an opportunity to improve.
However, in order for us to fix a bug, you need to tell us exactly what went wrong.

When you report a bug, please tell us as much pertinent information as possible.
This information includes:

- The pycytominer-transform version you’re using
- The format of input data
- Copy and paste two pieces of information: 1) your command and 2) the specific error message
- What you’ve tried to overcome the bug

Please provide this information as an issue in the repository: <https://github.com/cytomining/pycytominer-transform/issues>

Please also search the issues (and documentation) for an existing solution.
It’s possible we solved the bug already!
If you find an issue already describing the bug, please add a comment to the issue instead of opening a new one.

### Suggesting enhancements

We’re deeply committed to a simple, intuitive user experience, and to support core profiling pipeline data processing.
This commitment requires a good relationship, and open communication, with our users.

We encourage you to propose enhancements to improve the pycytominer-transform package as an issue in the repository.

First, figure out if your proposal is already implemented by reading the documentation!
Next, check the issues (<https://github.com/cytomining/pycytominer-transform/issues>) to see if someone else has already proposed the enhancement you have in mind.
If you do find the suggestion, please comment on the existing issue noting that you are also interested in this functionality.
If you do not find the suggestion, please open a new issue and clearly document the specific enhancement and why it would be helpful for your particular use case.

### Your first code contribution

Contributing code for the first time can be a daunting task.
However, in our community, we strive to be as welcoming as possible to newcomers, while ensuring rigorous software development practices.

The first thing to figure out is exactly what you’re going to contribute!
We describe all future work as individual [github issues](https://github.com/cytomining/pycytominer-transform/issues).
For first time contributors we have specifically tagged [beginner issues](https://github.com/cytomining/pycytominer-transform/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22).

If you want to contribute code that we haven’t already outlined, please start a discussion in a new issue before writing any code.
A discussion will clarify the new code and reduce merge time.
Plus, it’s possible your contribution belongs in a different code base, and we do not want to waste your time (or ours)!

### Pull requests

After you’ve decided to contribute code and have written it up, please file a pull request.
We specifically follow a [forked pull request model](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork).
Please create a fork of the pycytominer-transform repository, clone the fork, and then create a new, feature-specific branch.
Once you make the necessary changes on this branch, you should file a pull request to incorporate your changes into the main pycytominer-transform repository.

The content and description of your pull request are directly related to the speed at which we are able to review, approve, and merge your contribution into pycytominer-transform.
To ensure an efficient review process please perform the following steps:

1. Follow all instructions in the [pull request template](https://github.com/cytomining/pycytominer-transform/blob/main/.github/PULL_REQUEST_TEMPLATE.md)
1. Triple check that your pull request is adding _one_ specific feature. Small, bite-sized pull requests move so much faster than large pull requests.
1. After submitting your pull request, ensure that your contribution passes all status checks (e.g. passes all tests)

Pull request review and approval is required by at least one project maintainer to merge.
We will do our best to review the code addition in a timely fashion.
Ensuring that you follow all steps above will increase our speed and ability to review.
We will check for accuracy, style, code coverage, and scope.

### Git commit messages

For all commit messages, please use a short phrase that describes the specific change.
For example, “Add feature to check normalization method string” is much preferred to “change code”.
When appropriate, reference issues (via `#` plus number) .

## Development

### Overview

```{mermaid}
flowchart LR
    subgraph Dagger
        containers["Container(s)"]
    end
    subgraph Poetry
        python[Python]
    end
    python --> |running within| containers
```

Pycytominer-transform is primarily written in Python with related environments managed by Python [Poetry](https://python-poetry.org/).
We use [Dagger](https://docs.dagger.io/) for consistent local testing and for automated tests via containers.

### Getting started

To enable local development, perform the following steps.

1. [Install Python](https://www.python.org/downloads/)
1. [Install Poetry](https://python-poetry.org/docs/#installation)
1. [Install Poetry Environment](https://python-poetry.org/docs/basic-usage/#installing-dependencies): `poetry install`
1. [Install Dagger](https://docs.dagger.io/install/)
1. Install [Buildkit](https://docs.dagger.io/1223/custom-buildkit/) runtime environment for Dagger (for ex. [Docker Desktop](https://www.docker.com/products/docker-desktop/))
1. Initialize Dagger Project: `dagger project update`
1. Use the IDE of your choice to add or edit related content.

### Code style

For general Python code style, we use [black](https://github.com/psf/black).
For Python import order, we use [isort](https://github.com/PyCQA/isort).
Please use black and isort before committing any code.
We will not accept code contributions that do not use black and isort.
We use the [Google Style Python Docstrings](https://www.sphinx-doc.org/en/master/usage/extensions/example_google.html).

### Linting

Work added to this repo is automatically checked using [pre-commit](https://pre-commit.com/) (managed by this repo's poetry environment) via [Github Actions](https://docs.github.com/en/actions).
Pre-commit can work alongside your local [git with hooks](https://pre-commit.com/index.html#3-install-the-git-hook-scripts)
The following command also can perform the same checks:

```sh
% poetry run pre-commit run --all-files
```

### Testing

Automated or manual testing for this repo may be performed using Dagger actions.
The Dagger test action performs [pytest](https://pytest.org/en/latest/contents.html) testing and also pre-commit checks mentioned above.
Dagger actions provide testing through Github actions.

Example source data is sourced from [CellProfiler Examples](https://cellprofiler.org/examples) by processing it with [CellProfiler](https://github.com/CellProfiler/CellProfiler) with Dagger.
This example data is used during testing to ensure expected functionality.
See below for an example of how to create this testing data (Dagger action `gather_data`).

Dagger-based manual testing is performed using the following:

```sh
# update the dagger project
% dagger project update

# gather data for testing
% dagger do gather_data

# perform the testing
% dagger do test
```

Testing with Dagger will automatically test multiple Python versions.
You may also provide specific Python versions for isolated testing using additional arguments with Dagger.

For example:

```sh
# perform all tests under Python 3.8
% dagger do test "3.8"

# perform only sphinx tests under Python 3.9
% dagger do test "3.9" sphinx
```

It's also possible to test locally using Poetry.
Testing without Dagger does not guarantee a successful automated test as environmental differences could exist.
See below for an example of testing without Dagger:

```sh
% poetry run pytest
```

#### Test Coverage

Test coverage is provided via [coverage](https://github.com/nedbat/coveragepy) and [pytest-cov](https://github.com/pytest-dev/pytest-cov).
Use the following command to generate HTML coverage reports (reports made available at `./htmlcov/index.html`):

```sh
% poetry run pytest --cov=pycytominer_transform tests/
```

## Documentation

Documentation is generally provided through [MyST (or Markedly Structured Text)](https://myst-parser.readthedocs.io/en/latest/index.html) markdown documents.
Markdown content is transformed into docsite content using [Sphinx](https://www.sphinx-doc.org/).
Documentation content assumes a "one sentence per line" style.
Diagrams may be added using the [Sphinx extension for Mermaid](https://github.com/mgaitan/sphinxcontrib-mermaid#markdown-support).

### Documentation Linting

Content is tested via Dagger actions using the `sphinx-build ... -W` command to avoid missing autodoc members, etc.
To check your documentation updates before pushing, use the following to trigger a related `sphinx-build` (content made available at `./docs/build/index.html`):

```sh
% dagger do docs
```

### Documentation Builds

Documentation builds presume HTML as the primary export, e.g. `sphinx-build -b html ...`.
Documentation is automatically published to a docsite via [Github Actions](https://docs.github.com/en/actions).

## Attribution

Portions of this contribution guide were sourced from [pyctyominer](https://github.com/cytomining/pycytominer/blob/master/CONTRIBUTING.md).
Many thanks go to the developers and contributors of that repository.
