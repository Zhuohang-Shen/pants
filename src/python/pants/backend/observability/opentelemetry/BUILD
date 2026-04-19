# Copyright 2026 Pants project contributors (see CONTRIBUTORS.md).
# Licensed under the Apache License, Version 2.0 (see LICENSE).

python_sources(
    sources=["*.py", "!*_test.py", "!*_integration_test.py"],
)

python_tests(
    name="tests",
    sources=["*_test.py", "!*_integration_test.py"],
)

python_tests(
    name="integration_tests",
    sources=["*_integration_test.py"],
    # Integration tests take forever given how they are run. :(
    timeout=600,
)
