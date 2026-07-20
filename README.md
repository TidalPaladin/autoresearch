# Python Template

Template repository for Python projects. Uses [uv](https://github.com/astral-sh/uv) as the package manager.

## Usage

1. Rename [project](./project) to the desired project name
2. Update the `$PROJECT` variable in [Makefile](./Makefile) to match step 1
3. Update `pyproject.toml` as needed
4. Add source code to the renamed `project` folder
5. Run `make init` to install the project to a virtual environment
6. You can execute commands from the virtual environment with `uv run`

## Recipes
* `make style` - Runs code style formatting
* `make quality` - Tests that code complies with quality standards
* `make types` - Run static type checking with [pyright](https://github.com/microsoft/pyright)
* `make test` - Run unit tests
* `make test-pdb-*` - Run unit tests matching pattern `*` with fallback to [`pdb`](https://docs.python.org/3/library/pdb.html)
  for debugging.
* `make deploy` - Install dependencies from `uv` lockfile

## Optional steps
* Setup CI - a template CircleCI config is provided in `.circeci/config.yml`

## Misc

* Run `make help` to get a partial list of available make recipes
* A pytest mark, `ci_skip`, is provided to mark tests that should be skipped 
  during CI pipelines
