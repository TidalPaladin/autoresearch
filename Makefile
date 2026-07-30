.PHONY: audit check clean clean-env format format-check init lint package-check test test-% test-compat test-notify-loop types update

SOURCES = project scripts tests
UV = uv
NOTIFY_WAKE_RUNTIME = notify-wake-runtime @ git+https://github.com/TidalPaladin/skills.git@61a4a819c58243d54e3c99c684ec23ad88e6dfef\#subdirectory=notify-wake

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

test-compat: ## run the full suite without collecting coverage
	$(UV) run --frozen pytest tests

test-notify-loop: ## run the subscription-free notification-loop integration tests
	$(UV) run --frozen pytest tests/test_notify_loop.py

test-%: ## run tests matching a pattern
	$(UV) run --frozen pytest -k $* tests

audit: ## scan all locked dependency groups for known advisories
	audit_requirements="$$(mktemp)"; \
		trap 'rm -f "$$audit_requirements"' EXIT; \
		$(UV) export --quiet --frozen --all-groups --no-emit-project \
			--no-emit-package notify-wake-runtime \
			--format requirements-txt --output-file "$$audit_requirements"; \
		$(UV) run --frozen pip-audit --disable-pip --strict --require-hashes \
			--progress-spinner off -r "$$audit_requirements"

check: format-check lint types test audit ## run all non-rewriting quality gates

package-check: ## build a wheel and import it in an isolated environment
	$(UV) build --no-sources --clear
	wheel_count="$$(find dist -maxdepth 1 -type f -name '*.whl' | wc -l)"; \
		sdist_count="$$(find dist -maxdepth 1 -type f -name '*.tar.gz' | wc -l)"; \
		test "$$wheel_count" -eq 1; \
		test "$$sdist_count" -eq 1; \
		wheel="$$(find dist -maxdepth 1 -type f -name '*.whl' -print -quit)"; \
		$(UV) run --isolated --no-project \
			--with "$(NOTIFY_WAKE_RUNTIME)" \
			--with "$$wheel" \
			python -c "import notify_wake; import project"

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
