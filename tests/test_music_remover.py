from __future__ import annotations

from webapp.music_remover import subtract_intervals_from_keep


def test_subtract_intervals_from_keep_removes_overlap() -> None:
    keep = [(0.0, 10.0), (20.0, 30.0)]
    remove = [(3.0, 5.0), (22.0, 24.0)]
    got = subtract_intervals_from_keep(
        keep,
        remove,
        total_duration=100.0,
        min_keep_sec=0.05,
    )
    assert got == [(0.0, 3.0), (5.0, 10.0), (20.0, 22.0), (24.0, 30.0)]


def test_subtract_intervals_from_keep_empty_remove() -> None:
    keep = [(1.0, 2.0)]
    assert subtract_intervals_from_keep(
        keep,
        [],
        total_duration=10.0,
        min_keep_sec=0.01,
    ) == [(1.0, 2.0)]
