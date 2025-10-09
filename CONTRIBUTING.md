# Contributing to pyalexs3

Thanks for considering a contribution! 🎉  
This project loads OpenAlex dumps from S3 into DuckDB with parallel downloads and rich progress bars.

## Code of Conduct
By participating, you agree to the [Code of Conduct](./CODE_OF_CONDUCT.md).

## Quick Start (TL;DR)
- Clone the repo
- Create a branch: `git checkout -b feat/my-change`
- Setup dev environment:
  ```bash
  uv sync --extra dev
  ```
- Run checks:
    **If you are using Linux just run the `./checks.sh`**
    ```bash
    uv run pytest -q
    uv run ruff check . --fix --select I
    uv run ruff check . --fix
    uv run ruff check . --fix --unsafe-fixes
    uv run black . 
    uv run mypy src
    uv run black --check .
    ```
- Change version in `pyproject.toml` `[project][version]`.

> Use uv run black . locally to auto-format before committing.

# Tests

We use `pytest` and `moto` to mock S3.
Please include tests for:
- New features (unit + an end-to-end if applicable)
- Bug fixes (a failing test that passes with the fix)
- Edge cases (empty ranges, missing parts, bad dates)

Guidelines:
- Keep tests fast and deterministic
- Use minimal schemas in tests (patch WORKS_SCHEMA to tiny dict)
- Use monkeypatch to inject the moto S3 client:
    ```python
    monkeypatch.setattr(core_mod.boto3, "client", lambda *a, **k: s3, raising=True)
    ```

# Style & Types

- Formatting: Black
- Linting: ruff (plus ruff format)
- Type hints are required in new/changed code
- Docstrings: Google or NumPy style
- Prefer “why” comments over “what” comments

# Commit Messages

Conventional Commits recommended:

- feat: new user-facing feature
- fix: bug fix
- docs:, test:, perf:, refactor:, chore:

# PR Guidelines

- Link related issues
- Include tests and docs when relevant
- Keep PRs focused and reasonably small

# Adding schemas

Update `src/pyalexs3/schemas.py` and wire it in `core.py` via `__get_schema`.
Provide a short example in README and at least one test.

# Release process (maintainers)

- Bump `project.version` in `pyproject.toml`
- Tag `vX.Y.Z` and publish a GitHub Release
- CI publishes to PyPI (Trusted Publishing or token)
- Update `RELEASE_NOTES.md`

# Questions?
Open a discussion on [Discord Link](https://discord.gg/K7kDkCWQ)
