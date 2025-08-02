#!/usr/bin/env python3
"""
Release script for a2a-redis package.

This script automates the release process for publishing to PyPI:
1. Runs quality checks (linting, formatting, type checking)
2. Runs the full test suite
3. Builds the package
4. Optionally publishes to PyPI (with confirmation)

Usage:
    python scripts/release.py --check-only    # Run checks without publishing
    python scripts/release.py --test-pypi     # Publish to Test PyPI
    python scripts/release.py --pypi          # Publish to PyPI
"""

import argparse
import subprocess
import sys
from pathlib import Path
from typing import List, Optional


class ReleaseManager:
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.venv_path = project_root / ".venv"

    def run_command(self, cmd: List[str], cwd: Optional[Path] = None) -> bool:
        """Run a command and return True if successful."""
        if cwd is None:
            cwd = self.project_root

        print(f"→ Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)

        if result.returncode != 0:
            print(f"✗ Command failed with exit code {result.returncode}")
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")
            return False
        else:
            print("✓ Command succeeded")
            if result.stdout.strip():
                print(f"Output: {result.stdout.strip()}")
            return True

    def activate_venv(self) -> List[str]:
        """Return command prefix to activate virtual environment."""
        if not self.venv_path.exists():
            print("✗ Virtual environment not found at .venv")
            print("Run: uv venv && source .venv/bin/activate && uv sync --dev")
            sys.exit(1)
        return ["uv", "run"]

    def run_quality_checks(self) -> bool:
        """Run code quality checks."""
        print("\n🔍 Running quality checks...")

        cmd_prefix = self.activate_venv()

        checks = [
            (cmd_prefix + ["ruff", "check", "src/", "tests/"], "Linting"),
            (
                cmd_prefix + ["ruff", "format", "--check", "src/", "tests/"],
                "Format checking",
            ),
            (cmd_prefix + ["pyright", "src/"], "Type checking"),
        ]

        all_passed = True
        for cmd, description in checks:
            print(f"\n📋 {description}...")
            if not self.run_command(cmd):
                all_passed = False

        return all_passed

    def run_tests(self) -> bool:
        """Run the full test suite."""
        print("\n🧪 Running tests...")
        cmd_prefix = self.activate_venv()
        return self.run_command(
            cmd_prefix + ["pytest", "--cov=a2a_redis", "--cov-report=term-missing"]
        )

    def build_package(self) -> bool:
        """Build the package."""
        print("\n📦 Building package...")

        # Clean previous builds
        build_dirs = ["build", "dist", "src/a2a_redis.egg-info"]
        for dir_name in build_dirs:
            dir_path = self.project_root / dir_name
            if dir_path.exists():
                print(f"Cleaning {dir_path}")
                subprocess.run(["rm", "-rf", str(dir_path)])

        cmd_prefix = self.activate_venv()
        return self.run_command(cmd_prefix + ["python", "-m", "build"])

    def check_version(self) -> Optional[str]:
        """Get the current version from setuptools_scm."""
        print("\n📋 Checking version...")
        cmd_prefix = self.activate_venv()
        result = subprocess.run(
            cmd_prefix
            + [
                "python",
                "-c",
                "from setuptools_scm import get_version; print(get_version())",
            ],
            cwd=self.project_root,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            print("✗ Failed to get version")
            return None

        version = result.stdout.strip()
        print(f"Current version: {version}")

        # Check if this is a local version (contains +)
        if "+" in version and not version.endswith("+dirty"):
            print("⚠️  Warning: This is a development version with local identifier")
            print("PyPI doesn't accept local versions. To create a proper release:")
            print("1. Create a git tag: git tag v0.1.0")
            print("2. Re-run this script")
            return None

        return version

    def publish_to_pypi(self, test_pypi: bool = False) -> bool:
        """Publish to PyPI or Test PyPI."""
        repo_url = (
            "https://test.pypi.org/legacy/"
            if test_pypi
            else "https://upload.pypi.org/legacy/"
        )

        print(f"\n🚀 Publishing to {'Test ' if test_pypi else ''}PyPI...")

        cmd_prefix = self.activate_venv()
        cmd = cmd_prefix + ["twine", "upload"]

        if test_pypi:
            cmd.extend(["--repository-url", repo_url])

        cmd.append("dist/*")
        return self.run_command(cmd)

    def confirm_publish(
        self, version: str, test_pypi: bool = False, skip_confirm: bool = False
    ) -> bool:
        """Ask for confirmation before publishing."""
        repo_name = "Test PyPI" if test_pypi else "PyPI"

        print(f"\n⚠️  About to publish version {version} to {repo_name}")
        print("This action cannot be undone!")

        if skip_confirm:
            print("Auto-confirming due to --yes flag")
            return True

        response = input("Continue? (yes/no): ").lower().strip()
        return response in ["yes", "y"]


def main():
    parser = argparse.ArgumentParser(description="Release script for a2a-redis")
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Run checks and build without publishing",
    )
    parser.add_argument("--test-pypi", action="store_true", help="Publish to Test PyPI")
    parser.add_argument("--pypi", action="store_true", help="Publish to PyPI")
    parser.add_argument(
        "--skip-checks", action="store_true", help="Skip quality checks and tests"
    )
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompts")

    args = parser.parse_args()

    if not any([args.check_only, args.test_pypi, args.pypi]):
        parser.error("Must specify one of: --check-only, --test-pypi, --pypi")

    project_root = Path(__file__).parent.parent
    manager = ReleaseManager(project_root)

    print("🚀 Starting release process...")

    # Check version first
    version = manager.check_version()
    if not version:
        sys.exit(1)

    # Run quality checks and tests unless skipped
    if not args.skip_checks:
        if not manager.run_quality_checks():
            print("\n✗ Quality checks failed!")
            sys.exit(1)

        if not manager.run_tests():
            print("\n✗ Tests failed!")
            sys.exit(1)

    # Build package
    if not manager.build_package():
        print("\n✗ Package build failed!")
        sys.exit(1)

    print("\n✓ All checks passed! Package built successfully.")

    # Handle publishing
    if args.check_only:
        print(f"✓ Check complete - ready to release version {version}")
    elif args.test_pypi:
        if manager.confirm_publish(version, test_pypi=True, skip_confirm=args.yes):
            if manager.publish_to_pypi(test_pypi=True):
                print(f"\n🎉 Successfully published {version} to Test PyPI!")
            else:
                print("\n✗ Failed to publish to Test PyPI")
                sys.exit(1)
        else:
            print("Cancelled.")
    elif args.pypi:
        if manager.confirm_publish(version, test_pypi=False, skip_confirm=args.yes):
            if manager.publish_to_pypi(test_pypi=False):
                print(f"\n🎉 Successfully published {version} to PyPI!")
                print(f"Install with: pip install a2a-redis=={version}")
            else:
                print("\n✗ Failed to publish to PyPI")
                sys.exit(1)
        else:
            print("Cancelled.")


if __name__ == "__main__":
    main()
