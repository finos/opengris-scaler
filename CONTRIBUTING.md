We welcome contributions to OpenGRIS Scaler.

# OpenGRIS Scaler Contribution and Governance Policies

This document describes the contribution process and governance policies of the FINOS OpenGRIS Scaler project. The project is also governed by the [Linux Foundation Antitrust Policy](https://www.linuxfoundation.org/antitrust-policy/), and the FINOS [IP Policy]( https://community.finos.org/governance-docs/IP-policy.pdf), [Code of Conduct](https://community.finos.org/docs/governance/code-of-conduct), [Collaborative Principles](https://community.finos.org/docs/governance/collaborative-principles/), and [Meeting Procedures](https://community.finos.org/docs/governance/meeting-procedures/).


## Helpful Resources


* [README.md](./README.md)

* Documentation: [./docs](./docs)

* [Repository](https://github.com/finos/opengris-scaler)

* [Issue tracking](https://github.com/finos/opengris-scaler/issues)


## Contributing Guide

Each contribution must meet the following:

* Pass all tests, lint and formatting checks
* Include additional tests to verify the correctness of new features or bug fixes

NOTE: All contributors must have a contributor license agreement (CLA) on file with FINOS before their pull requests can be merged. Please review the FINOS [contribution requirements](https://community.finos.org/docs/governance/Software-Projects/contribution-compliance-requirements) and submit (or have your employer submit) the required CLA before submitting a pull request.

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

On the Github repository page, open a [new pull request](https://github.com/finos/opengris-scaler/pulls).

## Governance

### Roles

The project community consists of Contributors and Maintainers:
* A **Contributor** is anyone who submits a contribution to the project. (Contributions may include code, issues, comments, documentation, media, or any combination of the above.)
* A **Maintainer** is a Contributor who, by virtue of their contribution history, has been given write access to project repositories and may merge approved contributions.
* The **Lead Maintainer** is the project's interface with the FINOS team and Board. They are responsible for approving [quarterly project reports](https://community.finos.org/docs/governance/#project-governing-board-reporting) and communicating on behalf of the project. The Lead Maintainer is elected by a vote of the Maintainers. 

### Contribution Rules

Anyone is welcome to submit a contribution to the project. The rules below apply to all contributions. (The key words "MUST", "SHALL", "SHOULD", "MAY", etc. in this document are to be interpreted as described in [IETF RFC 2119](https://www.ietf.org/rfc/rfc2119.txt).)

* All contributions MUST be submitted as pull requests, including contributions by Maintainers.
* All pull requests SHOULD be reviewed by a Maintainer (other than the Contributor) before being merged.
* Pull requests for non-trivial contributions SHOULD remain open for a review period sufficient to give all Maintainers a sufficient opportunity to review and comment on them.
* After the review period, if no Maintainer has an objection to the pull request, any Maintainer MAY merge it.
* If any Maintainer objects to a pull request, the Maintainers SHOULD try to come to consensus through discussion. If not consensus can be reached, any Maintainer MAY call for a vote on the contribution.

### Maintainer Voting

The Maintainers MAY hold votes only when they are unable to reach consensus on an issue. Any Maintainer MAY call a vote on a contested issue, after which Maintainers SHALL have 36 hours to register their votes. Votes SHALL take the form of "+1" (agree), "-1" (disagree), "+0" (abstain). Issues SHALL be decided by the majority of votes cast. If there is only one Maintainer, they SHALL decide any issue otherwise requiring a Maintainer vote. If a vote is tied, the Lead Maintainer MAY cast an additional tie-breaker vote.

The Maintainers SHALL decide the following matters by consensus or, if necessary, a vote:
* Contested pull requests
* Election and removal of the Lead Maintainer
* Election and removal of Maintainers

All Maintainer votes MUST be carried out transparently, with all discussion and voting occurring in public, either:
* in comments associated with the relevant issue or pull request, if applicable;
* on the project mailing list or other official public communication channel; or
* during a regular, minuted project meeting.

### Maintainer Qualifications

Any Contributor who has made a substantial contribution to the project MAY apply (or be nominated) to become a Maintainer. The existing Maintainers SHALL decide whether to approve the nomination according to the Maintainer Voting process above.

### Changes to this Document

This document MAY be amended by a vote of the Maintainers according to the Maintainer Voting process above.

## Code of Conduct

Please see the FINOS [Community Code of Conduct](https://www.finos.org/code-of-conduct).