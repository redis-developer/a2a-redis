# Contributing to a2a-redis

## Development Setup

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) for dependency management
- Redis 8 (for running tests)

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/redis-developer/a2a-redis.git
   cd a2a-redis
   ```

2. Create a virtual environment and install dependencies:

   ```bash
   uv venv && source .venv/bin/activate && uv sync --dev
   ```

### Running Tests

Tests require a running Redis instance. Start one with Docker:

```bash
docker run -d --name redis-test -p 6379:6379 redis:8
```

Run the test suite:

```bash
uv run pytest
```

### Code Quality

Before committing, run linting and type checking:

```bash
uv run ruff check src/ tests/
uv run ruff format src/ tests/
uv run pyright src/
```

## Releasing

Releases are published to PyPI via GitHub Releases.

### Steps to Release

1. **Update the version** in `src/a2a_redis/__init__.py`:

   ```python
   __version__ = "0.3.0"  # New version
   ```

2. **Commit and push** the version bump:

   ```bash
   git add src/a2a_redis/__init__.py
   git commit -m "Bump version to 0.3.0"
   git push origin main
   ```

3. **Create a GitHub Release**:
   - Go to [Releases](https://github.com/redis-developer/a2a-redis/releases)
   - Click "Draft a new release"
   - Create a new tag matching the version: `v0.3.0`
   - Add release notes
   - Click "Publish release"

4. **Approve the workflow**:
   - The release workflow will start automatically
   - Go to [Actions](https://github.com/redis-developer/a2a-redis/actions)
   - The publish job requires approval (configured via the `pypi` environment)
   - Approve to publish to PyPI

### Version Format

- Use semantic versioning: `MAJOR.MINOR.PATCH`
- The tag must match the version in `__init__.py` (prefixed with `v`)
- Example: version `0.3.0` → tag `v0.3.0`

### Environment Setup (Maintainers)

The `pypi` environment must be configured in GitHub:

1. Go to **Settings → Environments → New environment**
2. Name it `pypi`
3. Add required reviewers for approval
4. Add the `PYPI_API_TOKEN` secret
