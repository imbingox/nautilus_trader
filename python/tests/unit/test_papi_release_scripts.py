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
Test the fork-specific PAPI wheel release gates.
"""

import importlib.util
import shutil
import subprocess
import zipfile
from pathlib import Path
from types import ModuleType

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "scripts/papi-release/verify_wheel.py"
MANIFEST_SCRIPT = REPO_ROOT / "scripts/papi-release/generate-manifest.py"
WHEEL_NAME = "nautilus_trader_papi-2.0.0rc6-cp314-cp314-manylinux_2_34_x86_64.whl"


def _load_script(path: Path, name: str) -> ModuleType:
    """
    Load the release verifier without making the scripts directory a package.
    """
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_verifier() -> ModuleType:
    return _load_script(SCRIPT, "papi_release_verify_wheel")


def _wheel(
    tmp_path: Path,
    *,
    requirement: str | None = None,
    facade: bytes = b"",
    filename: str = WHEEL_NAME,
    homepage: str = "https://github.com/imbingox/nautilus_trader",
    os_classifier: str = "Operating System :: POSIX :: Linux",
    requires_python: str = ">=3.14, <3.15",
) -> Path:
    """
    Create the smallest archive that satisfies the PAPI wheel content contract.
    """
    path = tmp_path / filename
    dist_info = "nautilus_trader_papi-2.0.0rc6.dist-info"
    metadata = "\n".join(
        [
            "Metadata-Version: 2.4",
            "Name: nautilus-trader-papi",
            "Version: 2.0.0rc6",
            "Summary: Unofficial NautilusTrader PAPI distribution",
            f"Home-Page: {homepage}",
            f"Requires-Python: {requires_python}",
            f"Classifier: {os_classifier}",
            "Provides-Extra: visualization",
            *([] if requirement is None else [f"Requires-Dist: {requirement}"]),
            "",
            "# Test package",
        ],
    )
    wheel_metadata = (
        "Wheel-Version: 1.0\n"
        "Generator: test\n"
        "Root-Is-Purelib: false\n"
        "Tag: cp314-cp314-manylinux_2_34_x86_64\n"
    )
    members = {
        "nautilus_trader/__init__.py": b"",
        "nautilus_trader/_libnautilus.pyi": b"",
        "nautilus_trader/_libnautilus.cpython-314-x86_64-linux-gnu.so": b"extension",
        "nautilus_trader/adapters/binance_papi/__init__.py": facade,
        "nautilus_trader/adapters/binance_papi/__init__.pyi": b"",
        f"{dist_info}/METADATA": metadata.encode(),
        f"{dist_info}/WHEEL": wheel_metadata.encode(),
        f"{dist_info}/RECORD": b"",
        f"{dist_info}/licenses/LICENSE": b"LGPL-3.0-only",
    }
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, value in members.items():
            archive.writestr(name, value)
    return path


def test_verify_wheel_accepts_frozen_release_contract(tmp_path: Path) -> None:
    """
    Accept a complete CPython 3.14 manylinux x86_64 wheel.
    """
    report = _load_verifier().validate_wheel(_wheel(tmp_path))

    assert report["distribution"] == "nautilus-trader-papi"
    assert report["version"] == "2.0.0rc6"
    assert report["platform_tag"] == "manylinux_2_34_x86_64"
    assert report["archive_checks"] == "passed"
    assert report["complete"] is False


@pytest.mark.parametrize(
    "requirement",
    [
        "nautilus-trader==2.0.0rc6",
        "nautilus_trader[visualization]>=2; python_version >= '3.14'",
        "Nautilus.Trader @ https://example.invalid/nautilus-trader.whl",
    ],
)
def test_verify_wheel_rejects_official_distribution_dependency(
    tmp_path: Path,
    requirement: str,
) -> None:
    """
    Reject metadata that could install both conflicting distributions.
    """
    path = _wheel(tmp_path, requirement=requirement)

    with pytest.raises(ValueError, match="conflicting official distribution"):
        _load_verifier().validate_wheel(path)


def test_verify_wheel_rejects_sensitive_test_content(tmp_path: Path) -> None:
    """
    Reject a packaged credential marker from offline adapter tests.
    """
    path = _wheel(tmp_path, facade=b'API_SECRET = "OfflinePapiSecret"')

    with pytest.raises(ValueError, match="forbidden text"):
        _load_verifier().validate_wheel(path)


def test_verify_wheel_rejects_unfrozen_platform(tmp_path: Path) -> None:
    """
    Reject a platform outside the first release matrix.
    """
    filename = WHEEL_NAME.replace("manylinux_2_34_x86_64", "macosx_15_0_arm64")

    with pytest.raises(ValueError, match="Unexpected release wheel filename"):
        _load_verifier().validate_wheel(_wheel(tmp_path, filename=filename))


def test_verify_wheel_rejects_platform_independent_metadata(tmp_path: Path) -> None:
    """
    Reject metadata that claims support beyond the frozen Linux wheel.
    """
    path = _wheel(tmp_path, os_classifier="Operating System :: OS Independent")

    with pytest.raises(ValueError, match="operating system classifier"):
        _load_verifier().validate_wheel(path)


def test_verify_wheel_rejects_upstream_homepage(tmp_path: Path) -> None:
    """
    Reject legacy metadata that presents the fork as the upstream distribution.
    """
    path = _wheel(tmp_path, homepage="https://nautilustrader.io")

    with pytest.raises(ValueError, match="Home-Page does not identify the fork"):
        _load_verifier().validate_wheel(path)


def test_manifest_cleanliness_allows_only_untracked_wheel_artifacts(tmp_path: Path) -> None:
    """
    Allow generated wheel artifacts.

    Reject source or unrelated untracked changes.

    """
    git = shutil.which("git")
    assert git is not None
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run([git, "init", "--quiet"], cwd=repo, check=True)
    source = repo / "source.txt"
    source.write_text("clean\n", encoding="utf-8")
    subprocess.run([git, "add", "source.txt"], cwd=repo, check=True)
    subprocess.run(
        [
            git,
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "--quiet",
            "-m",
            "Initial test commit",
        ],
        cwd=repo,
        check=True,
    )
    wheel_dir = repo / "dist/papi"
    wheel_dir.mkdir(parents=True)
    (wheel_dir / WHEEL_NAME).write_bytes(b"wheel")
    manifest = _load_script(MANIFEST_SCRIPT, "papi_release_generate_manifest")

    assert manifest._source_changes(repo, wheel_dir) == []

    source.write_text("changed\n", encoding="utf-8")
    (repo / "unexpected.txt").write_text("unexpected\n", encoding="utf-8")
    assert manifest._source_changes(repo, wheel_dir) == [
        "tracked: source.txt",
        "unexpected.txt",
    ]
