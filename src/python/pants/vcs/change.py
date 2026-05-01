# Copyright 2026 Pants project contributors (see CONTRIBUTORS.md).
# Licensed under the Apache License, Version 2.0 (see LICENSE).

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ChangeType(Enum):
    Added = "A"
    Deleted = "D"
    Modified = "M"


@dataclass(frozen=True)
class ChangedFile:
    path: str
    change_type: ChangeType
