We welcome contributions to Scaler.


## Helpful Resources


* [README.md](./README.md)

* Documentation: [./docs](./docs)

* [Repository](https://github.com/citi/scaler)

* [Issue tracking](https://github.com/citi/scaler/issues)


## Contributing Guide

Each contribution must meet the following:

* Pass all tests, lint and formatting checks
* Include additional tests to verify the correctness of new features or bug fixes


### Submit a contribution

#### Running tests

```bash
python -m unittest discover
```

#### Lint checks

**We enforce the [PEP 8](https://peps.python.org/pep-0008/) coding style, with a relaxed constraint on the maximum line
length (120 columns)**.

`isort`, `black` and `flake8` can be installed via PIP.

```bash
isort --profile black --line-length 120 .
black -l 120 -C .
flake8 --max-line-length 120 --extend-ignore=E203 .
```

The `isort`, `black` and `flake8` packages can be installed through Python's PIP.


#### Bump version number

You must update the version defined in [about.py](scaler/about.py) for every contribution. Please follow
[semantic versioning](https://semver.org) in the format `MAJOR.MINOR.PATCH`.


#### Create Pull Request

On the Github repository page, open a [new pull request](https://github.com/citi/scaler/pulls).


## Code of Conduct

We are committed to making open source an enjoyable and respectful experience for our community. See
[`CODE_OF_CONDUCT`](https://github.com/citi/.github/blob/main/CODE_OF_CONDUCT.md) for more information.
