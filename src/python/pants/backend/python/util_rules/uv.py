# Copyright 2026 Pants project contributors (see CONTRIBUTORS.md).
# Licensed under the Apache License, Version 2.0 (see LICENSE).

from __future__ import annotations

import hashlib
import json
import logging
import os
import shlex
from collections.abc import Iterable
from dataclasses import dataclass
from textwrap import dedent  # noqa: PNT20
from typing import ClassVar, cast

from pants.backend.python.subsystems import uv as uv_subsystem
from pants.backend.python.subsystems.python_native_code import PythonNativeCodeSubsystem
from pants.backend.python.subsystems.uv import (
    DownloadedUv,
    Uv,
)
from pants.backend.python.util_rules.interpreter_constraints import InterpreterConstraints
from pants.backend.python.util_rules.lockfile_metadata import (
    LockfileFormat,
    PythonLockfileMetadataV8,
)
from pants.backend.python.util_rules.pex_environment import PythonExecutable
from pants.backend.python.util_rules.pex_requirements import (
    LoadedLockfile,
)
from pants.base.build_root import BuildRoot
from pants.core.util_rules import system_binaries
from pants.core.util_rules.env_vars import environment_vars_subset
from pants.core.util_rules.subprocess_environment import SubprocessEnvironmentVars
from pants.core.util_rules.system_binaries import RealpathBinary
from pants.engine.composite_process import Subprocess
from pants.engine.env_vars import EnvironmentVarsRequest
from pants.engine.fs import (
    CreateDigest,
    FileContent,
    MergeDigests,
)
from pants.engine.intrinsics import (
    create_digest,
    get_digest_contents,
    merge_digests,
)
from pants.engine.rules import collect_rules, concurrently, implicitly, rule
from pants.util.docutil import bin_name
from pants.util.frozendict import FrozenDict
from pants.util.strutil import softwrap

logger = logging.getLogger(__name__)


_UV_PLATFORM_MAP = {
    ("Linux", "x86_64"): "x86_64-unknown-linux-gnu",
    ("Linux", "aarch64"): "aarch64-unknown-linux-gnu",
    ("Darwin", "x86_64"): "x86_64-apple-darwin",
    ("Darwin", "aarch64"): "aarch64-apple-darwin",
    ("Windows", "x86_64"): "x86_64-pc-windows-msvc",
    ("Windows", "aarch64"): "aarch64-pc-windows-msvc",
}


def uv_platform_from_complete_platform(json_str: str) -> tuple[str, str]:
    """Derive a uv --python-platform string and Python version from a complete_platform JSON.

    Returns (uv_platform, python_version), e.g. ("x86_64-unknown-linux-gnu", "3.12").

    Supports two complete_platform formats:
    - The newer format from `pex3 interpreter inspect --markers --tags`, which includes a
      `marker_environment` field with explicit platform_system, platform_machine, python_version.
    - The simpler format (e.g. used in Pants' built-in AWS Lambda/GCF platform files) which
      includes only a `compatible_tags` list like ["cp312-cp312-manylinux_2_17_x86_64", ...].
    """
    data = json.loads(json_str)

    if "marker_environment" in data:
        env = data["marker_environment"]
        system = env["platform_system"]  # "Linux", "Darwin", "Windows"
        machine = env["platform_machine"]  # "x86_64", "aarch64", "arm64"
        python_version = env["python_version"]  # e.g. "3.12"
        # macOS reports "arm64"; uv uses "aarch64".
        if machine == "arm64":
            machine = "aarch64"
    elif "compatible_tags" in data:
        system, machine, python_version = _parse_platform_from_compatible_tags(
            data["compatible_tags"]
        )
    else:
        raise ValueError(
            softwrap(
                """
                Cannot derive a uv platform string from complete_platform: the JSON must contain
                either a 'marker_environment' or 'compatible_tags' field.
                """
            )
        )

    key = (system, machine)
    if key not in _UV_PLATFORM_MAP:
        raise ValueError(
            softwrap(
                f"""
                Cannot derive a uv platform string from complete_platform with
                platform_system={system!r}, platform_machine={machine!r}.
                Supported combinations: {sorted(_UV_PLATFORM_MAP)}.
                """
            )
        )
    return _UV_PLATFORM_MAP[key], python_version


def _parse_platform_from_compatible_tags(tags: list[str]) -> tuple[str, str, str]:
    """Parse (system, machine, python_version) from a pex compatible_tags list.

    Tags are in the format "interpreter-abi-platform", e.g. "cp312-cp312-manylinux_2_17_x86_64".
    """
    for tag in tags:
        parts = tag.split("-")
        if len(parts) != 3:
            continue
        interp, _abi, plat = parts
        if plat == "any":
            continue

        # Extract Python version from interpreter like "cp312" -> "3.12".
        if interp.startswith("cp") and len(interp) > 2 and interp[2:].isdigit():
            digits = interp[2:]
            python_version = f"{digits[0]}.{digits[1:]}"
        else:
            continue

        # Parse OS and architecture from platform string.
        # Architecture may itself contain underscores (e.g. x86_64), so we cannot simply
        # rsplit("_", 1). Instead we strip the OS prefix and rejoin the remaining parts.
        plat_parts = plat.split("_")
        if plat.startswith("manylinux"):
            # e.g. manylinux_2_17_x86_64 -> parts[3:] = ["x86", "64"] -> "x86_64"
            #      manylinux2014_x86_64   -> parts[1:] = ["x86", "64"] -> "x86_64"
            #      manylinux_2_17_aarch64 -> parts[3:] = ["aarch64"]  -> "aarch64"
            if plat.startswith("manylinux_"):
                # New-style (PEP 600): manylinux_MAJOR_MINOR_ARCH
                machine = "_".join(plat_parts[3:])
            else:
                # Old-style: manylinux<GLIBC>_ARCH (e.g. manylinux2014_x86_64)
                machine = "_".join(plat_parts[1:])
            system = "Linux"
        elif plat.startswith("linux"):
            # e.g. linux_x86_64 -> parts[1:] = ["x86", "64"] -> "x86_64"
            machine = "_".join(plat_parts[1:])
            system = "Linux"
        elif plat.startswith("macosx"):
            # e.g. macosx_14_0_arm64 -> parts[-1] = "arm64"
            #      macosx_11_0_x86_64 -> parts[-2:] = ["x86", "64"] -> "x86_64"
            # arm64 and aarch64 are single tokens; x86_64 is two parts.
            raw_arch = "_".join(plat_parts[3:])
            machine = "aarch64" if raw_arch == "arm64" else raw_arch
            system = "Darwin"
        elif plat.startswith("win"):
            # e.g. win_amd64, win_arm64
            system = "Windows"
            if "amd64" in plat:
                machine = "x86_64"
            elif "arm64" in plat:
                machine = "aarch64"
            else:
                continue
        else:
            continue

        return system, machine, python_version

    raise ValueError(
        softwrap(
            f"""
            Cannot derive platform from compatible_tags: no recognizable platform-specific tag
            found in the first entries: {tags[:5]}.
            """
        )
    )


@dataclass(frozen=True)
class VenvFromUvLockfileRequest:
    """Request to install all packages from a uv lockfile into a virtualenv."""

    lockfile: LoadedLockfile
    # Used for local-platform builds (no complete_platform_json).
    python: PythonExecutable | None = None
    # When set, install wheels for the platform described by this complete_platform JSON
    # (as produced by `pex3 interpreter inspect --markers --tags`) instead of the local one.
    # The Python version and target OS/arch are both derived from the JSON.
    complete_platform_json: str | None = None

    def __post_init__(self) -> None:
        if self.python is None and self.complete_platform_json is None:
            raise ValueError("Either python or complete_platform_json must be set.")


@dataclass(frozen=True)
class VenvRepository:
    """A virtualenv directory that Pex can use as a --venv-repository."""

    cache_name: ClassVar[str] = "venv_cache"
    cache_dir: ClassVar[str] = f".cache/{cache_name}/"

    venv_path_suffix: str
    creation_subprocess: Subprocess

    def relpath(self) -> str:
        # The path to the venv in any sandbox that has the venv_cache append-only cache.
        return os.path.join(self.cache_dir, self.venv_path_suffix)

    @classmethod
    def append_only_caches(cls) -> FrozenDict[str, str]:
        return FrozenDict({cls.cache_name: cls.cache_dir})


@dataclass(frozen=True)
class UvEnvironment:
    env: FrozenDict[str, str]


@rule
async def get_uv_environment(
    subprocess_env_vars: SubprocessEnvironmentVars,
    uv_env_aware: Uv.EnvironmentAware,
    python_native_code: PythonNativeCodeSubsystem.EnvironmentAware,
) -> UvEnvironment:
    path = os.pathsep.join(uv_env_aware.path)
    subprocess_env_dict = dict(subprocess_env_vars.vars)

    extra_env = await environment_vars_subset(
        EnvironmentVarsRequest(uv_env_aware.extra_env_vars), **implicitly()
    )

    if "PATH" in subprocess_env_dict:
        path = os.pathsep.join([path, subprocess_env_dict.pop("PATH")])
    return UvEnvironment(
        env=FrozenDict(
            {
                **extra_env,
                "PATH": path,
                **subprocess_env_dict,
                **python_native_code.subprocess_env_vars,
            }
        )
    )


# A utility function to generate a transient, minimal pyproject.toml for uv to interact with.
# The synthetic project name (pants-lockfile-for-*) must not collide with any real requirement.
# uv will include this project as a virtual package in the lockfile, and we set package = false,
# so it won't try to install it.
def generate_pyproject_toml(resolve: str, ics: InterpreterConstraints, reqs: Iterable[str]) -> str:
    def escape_double_quotes(s: str) -> str:
        return s.replace('"', '\\"')

    requires_python = ",".join(str(constraint.specifier) for constraint in ics)
    deps_lines = "\n".join(f'    "{escape_double_quotes(r)}",' for r in sorted(reqs))

    return dedent(
        """
        [project]
        name = "pants-lockfile-for-{resolve}"
        version = "0.0.0"
        requires-python = "{requires_python}"
        dependencies = [
        {deps_lines}
        ]

        [tool.uv]
        package = false
        """
    ).format(resolve=resolve, requires_python=requires_python, deps_lines=deps_lines)


@rule
async def create_venv_repository_from_uv_lockfile(
    request: VenvFromUvLockfileRequest,
    downloaded_uv: DownloadedUv,
    uv_env: UvEnvironment,
    realpath_binary: RealpathBinary,
    buildroot: BuildRoot,
) -> VenvRepository:
    """Install all packages from a uv lockfile into a virtualenv."""
    if request.lockfile.lockfile_format != LockfileFormat.UV:
        raise ValueError(f"Expected a uv lockfile, got {request.lockfile.lockfile_format}")
    if request.lockfile.metadata is None:
        raise ValueError(
            softwrap(
                f"""
                Cannot install from uv lockfile {request.lockfile.lockfile_path}: metadata is
                missing. uv lockfiles must have a separate metadata file. Please regenerate
                the lockfile by running `{bin_name()} generate-lockfiles`.
                """
            )
        )
    metadata: PythonLockfileMetadataV8 = cast(PythonLockfileMetadataV8, request.lockfile.metadata)

    # We maintain one cached venv per buildroot+resolve+interpreter-or-platform. uv will
    # efficiently incrementally update the venv as the lockfile changes, and will handle
    # concurrency of `uv sync` with appropriate locking.
    buildroot_entropy = hashlib.sha256(buildroot.path.encode()).hexdigest()

    pyproject_content = generate_pyproject_toml(
        metadata.resolve,
        metadata.valid_for_interpreter_constraints,
        tuple(str(req) for req in metadata.requirements),
    )

    uv_config_digest, uv_lock_contents = await concurrently(
        create_digest(
            CreateDigest(
                (
                    FileContent("pyproject.toml", pyproject_content.encode()),
                    # Nothing to put in config right now, but we need it to be present.
                    FileContent("uv.toml", b""),
                )
            )
        ),
        get_digest_contents(request.lockfile.lockfile_digest),
    )
    uv_lock_digest = await create_digest(
        CreateDigest([FileContent("uv.lock", uv_lock_contents[0].content)])
    )

    input_digest = await merge_digests(
        MergeDigests(
            (
                downloaded_uv.digest,
                uv_config_digest,
                uv_lock_digest,
            )
        )
    )

    if request.complete_platform_json is not None:
        uv_platform, python_version = uv_platform_from_complete_platform(
            request.complete_platform_json
        )
        # Key the venv by a hash of the complete_platform JSON so each distinct target
        # platform gets its own cache entry.
        platform_key = hashlib.sha256(request.complete_platform_json.encode()).hexdigest()[:16]
        venv_path_suffix = os.path.join(buildroot_entropy, metadata.resolve, platform_key)
        python_args: tuple[str, ...] = (
            "--python",
            python_version,
            "--python-platform",
            uv_platform,
        )
    else:
        assert request.python is not None
        venv_path_suffix = os.path.join(
            buildroot_entropy, metadata.resolve, request.python.fingerprint
        )
        python_args = ("--python", request.python.path)

    uv_cmd = shlex.join(
        (
            *downloaded_uv.args(),
            "sync",
            "--frozen",
            "--no-install-project",
            # TODO: extras can conflict, so we might need to be more selective.
            "--all-extras",
            "--no-progress",
            *python_args,
        )
    )
    # We use `realpath` to resolve the named cache symlink to an absolute path in whatever
    # environment this process runs in. This gives uv a stable absolute path for the venv
    # so that any entry point scripts it creates exec a valid path that doesn't reference
    # the sandbox.
    command = dedent(
        f"""\
        cache_root="$({realpath_binary.path} {shlex.quote(VenvRepository.cache_dir)})"
        UV_PROJECT_ENVIRONMENT="${{cache_root}}/{venv_path_suffix}" {uv_cmd}
        """
    )

    return VenvRepository(
        venv_path_suffix=venv_path_suffix,
        creation_subprocess=Subprocess(
            command=command,
            input_digest=input_digest,
            env=uv_env.env,
            append_only_caches={
                **downloaded_uv.append_only_caches(),
                **VenvRepository.append_only_caches(),
            },
        ),
    )


def rules():
    return [
        *collect_rules(),
        *uv_subsystem.rules(),
        *system_binaries.rules(),
    ]
