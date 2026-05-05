# Copyright 2021 Pants project contributors (see CONTRIBUTORS.md).
# Licensed under the Apache License, Version 2.0 (see LICENSE).

from __future__ import annotations

from textwrap import dedent

import pytest

from pants.backend.java.dependency_inference.rules import rules as java_dep_inf_rules
from pants.backend.java.target_types import rules as target_types_rules
from pants.core.util_rules import archive, system_binaries
from pants.core.util_rules.archive import ExtractedArchive, MaybeExtractArchiveRequest
from pants.core.util_rules.system_binaries import BashBinary
from pants.engine.fs import CreateDigest, Digest, FileContent, Snapshot
from pants.engine.process import Process, ProcessResult
from pants.jvm import compile as jvm_compile
from pants.jvm import jdk_rules, non_jvm_dependencies
from pants.jvm.classpath import rules as classpath_rules
from pants.jvm.jar_tool import jar_tool
from pants.jvm.jar_tool.jar_tool import JarToolRequest
from pants.jvm.jdk_rules import InternalJdk
from pants.jvm.resolve import coursier_fetch, coursier_setup, jvm_tool
from pants.jvm.testutil import maybe_skip_jdk_test
from pants.jvm.util_rules import rules as util_rules
from pants.testutil.rule_runner import PYTHON_BOOTSTRAP_ENV, QueryRule, RuleRunner


@pytest.fixture
def rule_runner() -> RuleRunner:
    rule_runner = RuleRunner(
        rules=[
            *system_binaries.rules(),
            *archive.rules(),
            *coursier_setup.rules(),
            *coursier_fetch.rules(),
            *classpath_rules(),
            *jvm_tool.rules(),
            *jar_tool.rules(),
            *jvm_compile.rules(),
            *non_jvm_dependencies.rules(),
            *jdk_rules.rules(),
            *java_dep_inf_rules(),
            *util_rules(),
            *target_types_rules(),
            QueryRule(Digest, (JarToolRequest,)),
            QueryRule(ExtractedArchive, (MaybeExtractArchiveRequest,)),
            QueryRule(BashBinary, ()),
            QueryRule(InternalJdk, ()),
            QueryRule(ProcessResult, (Process,)),
        ],
    )
    rule_runner.set_options(args=[], env_inherit=PYTHON_BOOTSTRAP_ENV)
    return rule_runner


JAR_FIXTURE_SRC = dedent(
    """\
    import java.io.OutputStream;
    import java.nio.file.Files;
    import java.nio.file.Path;
    import java.util.jar.JarEntry;
    import java.util.jar.JarOutputStream;

    public class MakeJarWithDataDescriptor {
      public static void main(String[] args) throws Exception {
        Path output = Path.of(args[0]);
        try (OutputStream os = Files.newOutputStream(output);
            JarOutputStream jos = new JarOutputStream(os)) {
          write(jos, "META-INF/", new byte[0]);
          write(jos, "META-INF/MANIFEST.MF", "Manifest-Version: 1.0\\n".getBytes());
          write(jos, "org/", new byte[0]);
          write(jos, "org/apache/", new byte[0]);
          write(jos, "org/apache/iceberg/", new byte[0]);
          write(jos, "org/apache/iceberg/spark/", new byte[0]);
          write(jos, "org/apache/iceberg/spark/SparkCatalog.class", "bytecode".repeat(2000).getBytes());
          write(jos, "LICENSE", "license-one".getBytes());
        }
      }

      private static void write(JarOutputStream jos, String name, byte[] content) throws Exception {
        JarEntry entry = new JarEntry(name);
        entry.setMethod(JarEntry.DEFLATED);
        jos.putNextEntry(entry);
        jos.write(content);
        jos.closeEntry();
      }
    }
    """
)


def _create_data_descriptor_jar(rule_runner: RuleRunner) -> Digest:
    jdk = rule_runner.request(InternalJdk, [])
    bash = rule_runner.request(BashBinary, [])
    digest = rule_runner.request(
        Digest,
        [
            CreateDigest(
                [
                    FileContent("MakeJarWithDataDescriptor.java", JAR_FIXTURE_SRC.encode()),
                    FileContent("out/input.jar", b""),
                ]
            )
        ],
    )
    compile_and_build = Process(
        argv=[
            bash.path,
            jdk.jdk_preparation_script,
            bash.path,
            "-c",
            " && ".join(
                [
                    "__java_home/bin/javac MakeJarWithDataDescriptor.java",
                    "__java_home/bin/java MakeJarWithDataDescriptor out/input.jar",
                ]
            ),
        ],
        input_digest=digest,
        immutable_input_digests=jdk.immutable_input_digests,
        append_only_caches=jdk.append_only_caches,
        env={"PANTS_INTERNAL_ABSOLUTE_PREFIX": "", **jdk.env},
        output_files=("MakeJarWithDataDescriptor.class", "out/input.jar"),
        description="Build local test jar",
    )
    result = rule_runner.request(ProcessResult, [compile_and_build])
    return result.output_digest


@maybe_skip_jdk_test
def test_repack_jar_with_data_descriptor_entries(rule_runner: RuleRunner) -> None:
    input_digest = _create_data_descriptor_jar(rule_runner)

    jar_digest = rule_runner.request(
        Digest,
        [
            JarToolRequest(
                jar_name="output.jar",
                digest=input_digest,
                jars=["out/input.jar"],
                compress=True,
            )
        ],
    )

    jar_extracted = rule_runner.request(
        ExtractedArchive, [MaybeExtractArchiveRequest(digest=jar_digest, use_suffix=".zip")]
    )
    jar_snapshot = rule_runner.request(Snapshot, [jar_extracted.digest])
    assert "org/apache/iceberg/spark/SparkCatalog.class" in jar_snapshot.files
    assert "LICENSE" in jar_snapshot.files
