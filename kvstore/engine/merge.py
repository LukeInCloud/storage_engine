import heapq

from .codec import Entry


def _tag(source, pri: int):
    for e in source:
        yield (e.key, pri, e.kind, e.value)


def merge_entries(sources):
    """Merge sources ordered newest-first into one ascending stream, yielding
    only the newest version of every key (tombstones included).

    Ties on a key are resolved by source position: lower index = newer wins.
    """
    tagged = [_tag(s, i) for i, s in enumerate(sources)]
    last = None
    for key, _pri, kind, value in heapq.merge(*tagged):
        if key == last:
            continue
        last = key
        yield Entry(key, value, kind)
