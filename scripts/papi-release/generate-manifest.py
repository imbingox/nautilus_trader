#!/usr/bin/env python3
# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""
Generate the immutable release manifest for a verified PAPI wheel.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
import tomllib
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any


UPSTREAM_SHA = "46a5658a2f66cf0a798d414dc1b63d98cf10fcc1"
FEATURE_SHA = "dda2c3d8f6e6d6bfc3e5a7e72917e69c69129e9a"
INITIAL_MAIN_SHA = "06c658b3606e052fd6f1cac43b53205d746fb35a"


def _run(*args: str) -> str:
    """
    Run a release metadata command and return trimmed stdout.
    """
    return subprocess.run(  # noqa: S603
        args,
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()


def _sha256(path: Path) -> str:
    """
    Return the SHA-256 digest for ``path``.
    """
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _load_toml(path: Path) -> dict[str, Any]:
    """
    Load a TOML document from ``path``.
    """
    with path.open("rb") as stream:
        return tomllib.load(stream)


def _package_version(lock: dict[str, Any], name: str) -> str:
    """
    Return the unique package version named ``name`` from a Cargo lock document.
    """
    versions = {package["version"] for package in lock["package"] if package["name"] == name}
    if len(versions) != 1:
        raise ValueError(f"Expected one {name} version in Cargo.lock, found {sorted(versions)}")
    return versions.pop()


def _source_changes(repo: Path, wheel_dir: Path) -> list[str]:
    """
    Return tracked changes and untracked files outside the wheel artifact directory.
    """
    tracked = _run("git", "-C", str(repo), "diff", "--name-only", "HEAD", "--").splitlines()
    untracked = _run(
        "git",
        "-C",
        str(repo),
        "ls-files",
        "--others",
        "--exclude-standard",
    ).splitlines()
    unexpected_untracked = [
        path for path in untracked if not (repo / path).resolve().is_relative_to(wheel_dir)
    ]
    return [*[f"tracked: {path}" for path in tracked], *unexpected_untracked]


def _generate_manifest(args: argparse.Namespace, repo: Path) -> None:
    """
    Validate release inputs and write the immutable manifest.
    """
    wheel_dir = args.wheel_dir.resolve()
    changes = _source_changes(repo, wheel_dir)
    if changes:
        raise ValueError(f"Release manifests require clean source files, found {changes}")
    source_sha = _run("git", "-C", str(repo), "rev-parse", f"{args.source_ref}^{{commit}}")
    head_sha = _run("git", "-C", str(repo), "rev-parse", "HEAD")
    tag_sha = _run("git", "-C", str(repo), "rev-parse", f"{args.tag}^{{commit}}")
    if source_sha != head_sha or tag_sha != head_sha:
        raise ValueError("Source ref, release tag, and HEAD must resolve to the same commit")

    wheels = list(wheel_dir.glob("*.whl"))
    if len(wheels) != 1:
        raise ValueError(f"Expected one wheel in {wheel_dir}, found {len(wheels)}")
    wheel = wheels[0]
    verification_path = wheel_dir / "wheel-verification.json"
    verification = json.loads(verification_path.read_text(encoding="utf-8"))
    if not verification.get("complete"):
        raise ValueError("Wheel verification is incomplete")
    if verification["filename"] != wheel.name or verification["sha256"] != _sha256(wheel):
        raise ValueError("Wheel does not match its verification record")

    pyproject = _load_toml(repo / "python/pyproject.toml")
    cargo = _load_toml(repo / "Cargo.toml")
    cargo_lock_path = repo / "Cargo.lock"
    python_lock_path = repo / "python/uv.lock"
    cargo_lock = _load_toml(cargo_lock_path)
    project = pyproject["project"]
    if project["name"] != verification["distribution"]:
        raise ValueError("Wheel and pyproject distribution names differ")
    if project["version"] != verification["version"]:
        raise ValueError("Wheel and pyproject versions differ")

    manifest = {
        "schema_version": 1,
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "distribution": project["name"],
        "version": project["version"],
        "source": {
            "repository": "https://github.com/imbingox/nautilus_trader",
            "commit": source_sha,
            "tag": args.tag,
            "upstream_commit": UPSTREAM_SHA,
            "verified_feature_merge": FEATURE_SHA,
            "initial_main_integration": INITIAL_MAIN_SHA,
        },
        "versions": {
            "python_package": project["version"],
            "rust_workspace": cargo["workspace"]["package"]["version"],
            "binance_sdk": _package_version(cargo_lock, "binance-sdk"),
            "maturin": pyproject["build-system"]["requires"][0].removeprefix("maturin=="),
        },
        "build": {
            "profile": "release",
            "python": "CPython 3.14 with GIL",
            "target": "x86_64-unknown-linux-gnu",
            "abi": "cp314",
            "platform_baseline": "manylinux_2_34_x86_64",
            "cpu_baseline": "x86-64 (rustc default target-cpu)",
            "features": pyproject["tool"]["maturin"]["features"],
            "strip": pyproject["tool"]["maturin"]["strip"],
            "cargo_build_jobs": os.environ.get("CARGO_BUILD_JOBS", "2"),
            "rustflags": os.environ.get("RUSTFLAGS", ""),
            "tls": {
                "http_runtime": "rustls",
                "full_graph_contains_native_tls_openssl": True,
                "native_audit": "auditwheel",
            },
        },
        "locks": {
            "Cargo.lock": _sha256(cargo_lock_path),
            "python/uv.lock": _sha256(python_lock_path),
        },
        "tools": {
            "python": f"{platform.python_implementation()} {platform.python_version()}",
            "build_host": platform.platform(),
            "rustc": _run("rustc", "-Vv"),
            "cargo": _run("cargo", "--version"),
            "uv": _run("uv", "--version"),
        },
        "artifact": {
            "filename": wheel.name,
            "size": wheel.stat().st_size,
            "sha256": _sha256(wheel),
        },
        "verification": verification,
        "publication": {
            "testpypi": None,
            "pypi": None,
            "same_bytes_required": True,
            "sdist_published": False,
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(manifest, stream, indent=2, sort_keys=True)
        stream.write("\n")


def main() -> int:
    """
    Validate final source identity and write the release manifest once.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel-dir", type=Path, required=True)
    parser.add_argument("--source-ref", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    try:
        _generate_manifest(args, Path(__file__).resolve().parents[2])
    except (KeyError, OSError, subprocess.CalledProcessError, ValueError) as e:
        print(f"PAPI manifest generation failed: {e}", file=sys.stderr)  # noqa: T201
        return 1

    print(f"Wrote PAPI release manifest to {args.output}")  # noqa: T201
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
