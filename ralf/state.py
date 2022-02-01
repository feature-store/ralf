from dataclasses import dataclass
import time
from typing import Any, Dict


@dataclass
class Record:
    """Container class for user record type."""

    entries: Dict[str, Any]
    primary_key: str

    def __getitem__(self, key):
        return self.entries[key]

    def __contains__(self, key):
        return key in self.entries
