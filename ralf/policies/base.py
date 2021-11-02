from ralf.state import Record


class LoadSheddingPolicy:
    def process(self, candidate: Record, current: Record) -> bool:
        return True
