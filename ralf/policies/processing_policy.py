from typing import Dict

from ralf.record import Record


def fifo(first: Record, second: Record):
    return first.processing_time < second.processing_time


def lifo(first: Record, second: Record):
    return first.processing_time > second.processing_time


def last_completed(first: Record, second: Record):
    return first.complete_time > second.complete_time


def make_sorter_with_key_weights(key_to_prority_map: Dict[str, int]):
    """Lower priority value == more prioritized"""

    def rank_using_state_context(first: Record, second: Record):
        return key_to_prority_map[first.key] < key_to_prority_map[second.key]

    return rank_using_state_context
