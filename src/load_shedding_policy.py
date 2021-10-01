import random

from ralf.state import Record


def always_process(candidate: Record, current: Record):
    return True


def newer_processing_time(candidate: Record, current: Record):
    return candidate.processing_time > current.processing_time


def make_cosine_policy(thresh: float = 1.0e-9):
    def changing_cosine(candidate: Record, current: Record):

        t = int(candidate.processing_time * 100 - current.processing_time * 100)
        seasonality = 24 * 7
        offset = t % seasonality + 1
        offset_curr = current.window[offset:] + current.window[:offset]
        # print("cand", [item.value for item in candidate.window])
        # print("offset", [item.value for item in offset_curr])
        # print([item.value for item in current.window])

        a = sum(
            [
                abs(candidate.window[i].value * offset_curr[i].value)
                for i in range(len(candidate.window))
            ]
        )
        b = sum([item.value * item.value for item in candidate.window])
        c = sum([item.value * item.value for item in current.window])

        dist = a / (b * c)
        # print("dist", dist)
        return dist

    return changing_cosine


def make_mean_policy(thresh: float = 0.1):
    def changing_mean(candidate: Record, current: Record):
        candidate_mean = sum([item.value for item in candidate.window])
        current_mean = sum([item.value for item in current.window])

        diff = abs(candidate_mean - current_mean) / abs(current_mean)
        # print("diff", diff)
        return diff > thresh

    return changing_mean


def make_sampling_policy(rate: float = 1):
    def sample_rate(candidate: Record, current: Record):
        return random.random() < rate

    return sample_rate


def later_complete_time(candidate: Record, current: Record):
    return candidate.complete_time > current.complete_time
