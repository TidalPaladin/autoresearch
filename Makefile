.PHONY: audit check clean clean-env format format-check init lint package-check test test-% types update

SOURCES = project scripts tests
UV = uv

format: ## rewrite Python files with Ruff formatting
	$(UV) run --frozen ruff format $(SOURCES)

format-check: ## verify formatting without rewriting files
	$(UV) run --frozen ruff format --check $(SOURCES)

lint: ## run Ruff lint checks
	$(UV) run --frozen ruff check $(SOURCES)

types: ## run Basedpyright type checking
	$(UV) run --frozen basedpyright

test: ## run tests with branch coverage and the 90 percent threshold
	$(UV) run --frozen pytest \
		--cov=project \
		--cov-report=term-missing \
		--cov-report=xml \
		tests

test-%: ## run tests matching a pattern
	$(UV) run --frozen pytest -k $* tests

audit: ## scan all locked dependency groups for known advisories
	audit_requirements="$$(mktemp)"; \
		trap 'rm -f "$$audit_requirements"' EXIT; \
		$(UV) export --quiet --frozen --all-groups --no-hashes --no-emit-project \
			--format requirements-txt --output-file "$$audit_requirements"; \
		$(UV) run --frozen pip-audit --disable-pip --no-deps \
			--progress-spinner off -r "$$audit_requirements"

check: format-check lint types test audit ## run all non-rewriting quality gates

package-check: ## build a wheel and import it in an isolated environment
	$(UV) build --no-sources
	wheel="$$(find dist -name '*.whl' -print -quit)"; \
		test -n "$$wheel"; \
		$(UV) run --isolated --no-project --with "$$wheel" python -c "import project"

init: ## install all locked dependency groups
	$(UV) sync --frozen --all-groups

update: ## refresh the lockfile and local environment
	$(UV) lock --upgrade
	$(UV) sync --all-groups

clean: ## remove local Python and quality-tool caches
	find $(SOURCES) -type d -name '__pycache__' -prune -exec rm -r {} +
	rm -rf .pytest_cache .ruff_cache htmlcov
	rm -f .coverage coverage.xml

clean-env: ## remove the local virtual environment
	rm -rf .venv

help: ## list available recipes
	@awk 'BEGIN {FS = ":.*?## "}; /^[a-zA-Z_-]+:.*?## / {printf "\033[36m  %-25s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort
