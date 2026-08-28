# AGENTS.md: rules for changing OpenGRIS Scaler

Rules for humans and coding agents changing this repository: how the code is laid out, built, verified, changed, and written about.
What Scaler is and how to use it: `README.md` and `docs/`.
Dependencies, supported Python versions, and Python tool configuration live in `pyproject.toml`, C++ formatting in `.clang-format`, and CI in `.github/`.
`.agents-local.md` (gitignored) holds one developer's overrides, such as build parallelism: read it before any build, install, or shell command.

## Layout

Scaler is a distributed task scheduler: a Python client, scheduler, worker, and worker managers, with the network layer (YMQ) and the object storage server in C++, speaking Cap'n Proto.

```
src/scaler/              Python: client/, scheduler/, worker/, worker_manager_adapter/, entry_points/ (CLIs),
                         io/ (YMQ bindings), protocol/, config/, ui/ (web monitor), utility/
src/cpp/scaler/          C++20: ymq/ (network layer over libuv), object_storage/, protocol/, wrapper/, utility/
src/protocol/            Cap'n Proto schemas
tests/                   mirrors src/scaler (unittest); tests/cpp mirrors src/cpp/scaler (GTest)
scripts/                 build.sh, test.sh, library_tool.sh (third-party C++ libraries), *.ps1 for Windows
docs/source/tutorials/   user documentation; commands.rst documents CLI flags and config keys
examples/                runnable examples, executed by CI
```

## Build and verify

Environment setup for the devcontainer and for Windows: `docs/source/tutorials/development/`.
The devcontainer ships the third-party C++ libraries (`REMOTE_CONTAINERS=true` inside it). Elsewhere, build them once:

```bash
for library in capnp libuv openssl; do
    ./scripts/library_tool.sh $library download
    ./scripts/library_tool.sh $library compile
    ./scripts/library_tool.sh $library install
done
```

```bash
uv venv .venv && source .venv/bin/activate
uv pip install -e ".[all]" --group dev    # also builds the C++ extensions (scikit-build-core)
./scripts/build.sh                              # standalone C++ build, required by ./scripts/test.sh
```

The gate, before every commit that touches code:

```bash
python_dirs="src tests examples benchmarks docs/source"
isort $python_dirs && black $python_dirs && pflake8 $python_dirs && mypy .
clang-format --Werror --dry-run <the .cpp and .h files you changed>
python -m unittest discover -v tests -t .
./scripts/test.sh                               # C++
```

- CI (`.github/workflows/build-and-test.yml`) runs this gate on a clean checkout on Linux, macOS, and Windows, plus clang-format 21 and the `examples/` scripts on Linux.
- The gate names the tracked directories because the Python tools also scan stray virtualenvs and build directories.
- Check the gate's own exit status: capture the output to a file, or `set -o pipefail`, so a pipe into `tail` or `grep` cannot mask a red.
- Scoped test runs guide iteration between commits.
- Run a command because the task needs its result, never because a document lists it.
- A build or test sequence used by more than one workflow lives in one composite action under `.github/actions/`.

## Principles

Coding principles inspired by the Zen of Python (`python -c 'import this'`), applied to the C++ as much as to the Python.

- **Simple is better than complex.** The least code that solves the problem, in a shape a beginner can follow.
- **Explicit is better than implicit.** Defaults as visible values, behaviour keyed off state the reader can see.
- **Errors should never pass silently.**
  - Entry points and tests fail loudly with the cause
  - A daemon (scheduler, worker, object storage server) logs a misbehaving peer and keeps serving the others
- **In the face of ambiguity, refuse the temptation to guess.** State assumptions, and discuss a request with several readings, or a simpler approach, before coding it.
- **There should be one obvious way to do it.** One mechanism per job: a second one drifts.
- **Special cases aren't special enough to break the rules.** A fix that needs a special case is the wrong fix.
- **If the implementation is hard to explain, it's a bad idea.** Explain a change in one sentence before writing it.
- **Now is better than never, although never is often better than right now.** Build what the task needs now: no speculative features, single-use abstractions, configurability nobody asked for, or handling for impossible states.
- **Flat is better than nested.** Guard clauses over nested conditionals, flat documents over deep hierarchies.
- **Readability counts.** Explicit names, short functions, and modules a reviewer reads top to bottom in one sitting.
- **Practicality beats purity.** Simplicity over DRY: a little duplication beats a single-use abstraction.
- **Namespaces are one honking great idea.** Directories, files, modules, namespaces, and tests match each other by name.
- **Fix the root cause.** A workaround, blind retry, or guard that hides the defect is not a fix.
- **Right-shaped data.** Fix the data shape first and the code around it gets small.
  - Constant conversion between shapes, or a field that can be half-set, means the shape is wrong
- **Least surprise.** A command, class, or flag does the expected thing, and learning one teaches its siblings.
- **Surfaces tell the truth.** `scaler top`, the web monitor, and the logs show the real state: a failed task never reads as done, a dead worker never looks busy.
- **Evidence over opinion.** A claim about behaviour, timing, or performance is verified by running the code.
- **Minimal additions, liberal removals.** A change leaves what it touched simpler than it found it.

## Working on a task

### Before coding

- Read the whole path the change touches, end to end, before proposing a fix.
- Find the existing mechanism first and extend it.
- A new helper appears only when nothing suitable exists, where the next reader will look.
- When a task forks (a trade-off, growing scope, several designs, a new dependency), ask a one-line question and first do everything that does not depend on the answer.
- When these rules already decide, act.
- Turn the task into a verifiable goal:
  - a bug: a test that is red before the fix and green after
  - a refactor: the suite green before and after
  - a feature: the check that proves it
- The fix matches the problem:
  - a clean hole gets fixed
  - a trade-off gets a config option
  - anything architectural gets a writeup and is the maintainers' call
- These rules bind as written, neither looser nor stricter.

### Changing code

- Every changed line traces to the task.
- Match the surrounding style.
- Remove what your change orphaned, and report unrelated dead code rather than deleting it.
- A refactor ships in its own commit, separate from behaviour changes.
- A change to a CLI flag, config key, or documented behaviour updates `docs/source/tutorials/` in the same change.

### Verifying

- Reproduce a reported bug (a review finding, an issue, a stack dump) before fixing it.
- A test whose setup dodges the real path pins nothing, however green it runs.
- Test options in the combinations users run, such as an allocate policy with a scaling policy.
- Attack a fix from the position it assumes (the same dying peer, the same load) before calling it done.
- A tool reporting success is not evidence that the edit landed: confirm by behaviour.
- A failing test is a finding about the code, the test, or the harness: the failing process's log decides which.
- A recurring problem has a systematic cause: correlate every occurrence before calling it transient, and say so when the cause stays unfound.
- Tear down every process a test starts, also when the test fails.
- After three failed attempts at the same fix, stop and name the assumption that may be wrong.
- Record a confirmed finding the moment it is confirmed: the repro command, the trial count, and why it is not a harness artifact.

### Reporting

Applies to replies, PR descriptions, and handoffs.

- Lead with the next action when there is one.
- Number multi-step instructions, one bounded action per step.
- Say what was measured, not that it works: the number, the failing output, the skipped step.
- State an error as its cause, then the fix.
- Finish one issue before raising the next, and list unrelated findings at the end.
- Surface pre-existing breakage early, as a decision, rather than at the end as out of scope.
- A list longer than five items splits into now and later.
- End when the answer is done, after restating where the task stands: done, in progress, next.

## Code

### Both languages

- Names are explicit and specific.
- Abbreviations only when widely understood (`msg`).
- An index is named for what it indexes: `msg_i`, not `i`, over a list of messages.
- Every number that means something is a named constant.
- Composition over inheritance: shared behaviour lives in standalone functions or injected collaborators.
- Abstract classes and mixins declare only abstract methods, so no concrete method is ever inherited.
- A rename carries every derived name: subclasses, variables, parameters, fields.
- ASCII in code, comments, and log messages, non-ASCII only inside other string literals.

### Python

- Type hints on every parameter and return value.
- Naming:
  - Classes: `PascalCase`, acronyms fully capitalized (`HTTPRequest`, not `HttpRequest`)
  - Functions and methods: `snake_case`
  - Constants: `UPPER_SNAKE_CASE`
  - Private members: `_` prefix

### C++

- Headers: `#pragma once`, then includes in three sorted blocks: the associated header, C/C++ libraries, local includes.
- Naming:
  - Classes: `PascalCase`, acronyms fully capitalized
  - Functions, variables, constants: `camelCase`
  - Fields: `_camelCase`, private, behind getters and setters
  - Files: `snake_case`, `.h` and `.cpp`
- Member order:
  - `public` before `private`
  - Nested types, then fields, constructors and destructor, methods, static methods
- Namespaces:
  - `scaler::`, matching the directory structure
  - Fully qualified names at every use, no `using namespace`
- Modern C++:
  - C++20 features that GCC, Clang, and MSVC all support
  - RAII and smart pointers, which make custom copy and move members unnecessary
  - Type-safe handles for every resource
  - `{}` initialization
  - `std::optional` over null pointers
  - `std::expected` over exceptions
- Cross-platform:
  - STL and libuv over native syscalls
  - Platform-specific code in files selected by CMake rather than `#ifdef`: `my_class.h` (interface), `my_class.cpp` (common), `my_class_windows.cpp`, `my_class_unix.cpp`

### Tests

- Tests mirror the code under test: `tests/` for `src/scaler` (unittest), `tests/cpp` for `src/cpp/scaler` (GTest), with matching file names.
- Tests import what they need directly: the environment has the `all` extra and the `dev` group installed.
- `skipIf` and `skipUnless` are for platform and Python-version limits and for dependencies no package index provides (`soamapi`).

## Writing

Applies to everything written here: comments, docstrings, docs, commit messages, log messages, CLI output, reviews, replies, and this file.

### Every sentence

- The shortest version that carries the point, stopping at unambiguous rather than at shortest.
- Lead with the point, and add rationale only when a reader could not reconstruct it.
- One idea per sentence, one topic per paragraph, active voice, at most about 25 words.
- Plain punctuation (periods, commas, colons, parentheses): a semicolon or em dash marks a sentence to split.
- Plain, literal verbs: start (not spin up), analyze (not perform an analysis).
- Concrete over abstract: name the command, the field, the number ("retries twice, then fails the task", not "handles failures robustly").
- An intensifier or marketing adjective gives way to the measurement.
- One name per concept, the project's own: scheduler, worker, agent, processor, worker manager, object storage, task, graph.
- Statements, not questions.
- Prose that names code is a claim to verify: every symbol, default, and flag it states matches the source.
- Present state only: "now", "previously", "no longer", and "used to" belong to the commit message.
- Keep every hedge that carries real uncertainty: "may have failed" stays "may have failed".
- A rewrite keeps every fact and adds none.
- Three or more steps or conditions become a list, and a sequence is numbered.

### Comments

- A comment says what the code and a grep cannot: the why, an invariant, a gotcha, a measured number, a link to a decision.
- One line: a paragraph means the code needs a better shape, or the explanation belongs in the commit message.
- For the next reader of the code, not the reviewer of this change: no "as requested", no PR or issue numbers.
- A test docstring names the behaviour the test pins.

### Documentation and this file

- Flat: a heading, then short bullets or paragraphs.
- One bullet per line, unwrapped, one rule per bullet.
- A section answers one question and is named for its subject.
- A page name says what the page holds.
- One owner per fact, and everything else links to it.
- Moving content deletes the old home and repoints every reference in the same change.
- Bold marks lead-in labels only.
- This file states rules and the present state of the project, and links to facts owned by `pyproject.toml`, `.clang-format`, `.github/`, or `docs/` rather than restating them.

## Commits

- Commit as the git author already configured (`git config user.name`, `git config user.email`), and ask when none is set.
- Every name on a commit is a human with a CLA on file: the configured author, and no agent `Co-authored-by` or `Assisted-by` trailer.
- One concern per commit, each passing the gate on its own.
- A fix to unpushed work folds into the commit it fixes.
- Stage named files: scratch notes, generated output, and session artifacts stay out.
- Subject: [Conventional Commits](https://www.conventionalcommits.org/), a type (`fix`, `feat`, `docs`, `test`, `refactor`, `build`, `ci`) and an optional scope (`fix(ymq):`), then the change in the imperative.
- Body only for what the diff cannot say: what was wrong, why it matters, what was verified, in point form without hedging.
- A message reads identically to someone with no access to the conversation: no session structure, no "as discussed", no local paths, hostnames, or emails.
