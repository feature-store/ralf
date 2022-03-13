import random
from typing import List

from ralf.record import Record


class LoadSheddingPolicy:
    def process(self, candidate: Record, current: Record) -> bool:
        return True


KeyType = str


class PrioritizationPolicy:
    def choose(self, keys: List[KeyType]) -> KeyType:
        return random.choice(keys)
