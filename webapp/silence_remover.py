"""
Local silence and low-noise remover for videos.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
import subprocess
from pathlib import Path


@dataclass(frozen=True)
class SilenceProfile:
    name: str
    noise_threshold_db: float
    min_silence_sec: float
    padding_sec: float
    min_keep_sec: float


@dataclass(frozen=True)
class RenderedProfile:
    profile: str
    output_path: str
    removed_silences: int


PROFILES: tuple[SilenceProfile, ...] = (
    SilenceProfile("conservative", -38.0, 0.85, 0.15, 0.35),
    SilenceProfile("balanced", -34.0, 0.60, 0.10, 0.25),
    SilenceProfile("aggressive", -30.0, 0.40, 0.06, 0.20),
)


_SILENCE_START_RE = re.compile(r"silence_start:\s*([0-9]*\.?[0-9]+)")
_SILENCE_END_RE = re.compile(r"silence_end:\s*([0-9]*\.?[0-9]+)")
_DURATION_RE = re.compile(r"Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)")


def _run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, capture_output=True, check=True)


def format_duration_for_filename(seconds: float) -> str:
    """
    Duration token in whole seconds for filenames, e.g. 120s.
    """
    s = max(0.0, float(seconds))
    return f"{int(round(s))}s"


def output_duration_from_keep_segments(keep_segments: list[tuple[float, float]]) -> float:
    """Total duration of concatenated keep ranges (matches rendered output length)."""
    return sum(max(0.0, e - s) for s, e in keep_segments)


def duration_before_after_tag(before_sec: float, after_sec: float) -> str:
    """Build a token like 120s_to_45s for output filenames."""
    return f"{format_duration_for_filename(before_sec)}_to_{format_duration_for_filename(after_sec)}"


def probe_duration_seconds(video_path: str) -> float:
    proc = subprocess.run(
        ["ffmpeg", "-hide_banner", "-i", video_path],
        text=True,
        capture_output=True,
        check=False,
    )
    payload = (proc.stderr or "") + "\n" + (proc.stdout or "")
    m = _DURATION_RE.search(payload)
    if not m:
        raise RuntimeError("Could not detect video duration via ffmpeg.")
    h = int(m.group(1))
    mi = int(m.group(2))
    sec = float(m.group(3))
    return max(0.0, h * 3600 + mi * 60 + sec)


def detect_silences(video_path: str, *, noise_threshold_db: float, min_silence_sec: float) -> list[tuple[float, float]]:
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-i",
        video_path,
        "-af",
        f"silencedetect=noise={noise_threshold_db}dB:d={min_silence_sec}",
        "-f",
        "null",
        "-",
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True, check=False)
    payload = (proc.stderr or "") + "\n" + (proc.stdout or "")
    silences: list[tuple[float, float]] = []
    current_start: float | None = None
    for line in payload.splitlines():
        sm = _SILENCE_START_RE.search(line)
        if sm:
            current_start = float(sm.group(1))
            continue
        em = _SILENCE_END_RE.search(line)
        if em and current_start is not None:
            end = float(em.group(1))
            if end > current_start:
                silences.append((current_start, end))
            current_start = None
    return silences


def build_keep_segments(
    total_duration: float,
    silences: list[tuple[float, float]],
    *,
    padding_sec: float,
    min_keep_sec: float,
) -> list[tuple[float, float]]:
    if total_duration <= 0:
        return []
    cut_ranges: list[tuple[float, float]] = []
    for start, end in silences:
        s = max(0.0, start + padding_sec)
        e = min(total_duration, end - padding_sec)
        if e > s:
            cut_ranges.append((s, e))
    cut_ranges.sort(key=lambda x: x[0])
    merged: list[tuple[float, float]] = []
    for s, e in cut_ranges:
        if not merged or s > merged[-1][1]:
            merged.append((s, e))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
    keep: list[tuple[float, float]] = []
    cursor = 0.0
    for s, e in merged:
        if s - cursor >= min_keep_sec:
            keep.append((cursor, s))
        cursor = max(cursor, e)
    if total_duration - cursor >= min_keep_sec:
        keep.append((cursor, total_duration))
    return keep


def merge_keep_segments_by_gap(
    keep_segments: list[tuple[float, float]], merge_gap_sec: float
) -> list[tuple[float, float]]:
    if not keep_segments:
        return []
    gap = max(0.0, float(merge_gap_sec))
    merged: list[tuple[float, float]] = [keep_segments[0]]
    for s, e in keep_segments[1:]:
        ls, le = merged[-1]
        if s - le <= gap:
            merged[-1] = (ls, max(le, e))
        else:
            merged.append((s, e))
    return merged


def _render_segments(input_video: str, output_video: str, keep_segments: list[tuple[float, float]]) -> None:
    if not keep_segments:
        raise RuntimeError("No non-silent segments found to render.")
    filters: list[str] = []
    vouts: list[str] = []
    aouts: list[str] = []
    for idx, (start, end) in enumerate(keep_segments):
        filters.append(f"[0:v]trim=start={start}:end={end},setpts=PTS-STARTPTS[v{idx}]")
        filters.append(f"[0:a]atrim=start={start}:end={end},asetpts=PTS-STARTPTS[a{idx}]")
        vouts.append(f"[v{idx}]")
        aouts.append(f"[a{idx}]")
    concat = "".join(x + y for x, y in zip(vouts, aouts))
    filters.append(f"{concat}concat=n={len(keep_segments)}:v=1:a=1[outv][outa]")
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        input_video,
        "-filter_complex",
        ";".join(filters),
        "-map",
        "[outv]",
        "-map",
        "[outa]",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "20",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        output_video,
    ]
    _run(cmd)


def render_keep_segments_video(
    input_video: str,
    output_video: str,
    keep_segments: list[tuple[float, float]],
) -> None:
    """Export video+audio by keeping only the given time ranges (sync-safe concat)."""
    _render_segments(input_video, output_video, keep_segments)


def remove_silence_profiles(input_video: str, output_dir: str, output_prefix: str) -> list[RenderedProfile]:
    base = Path(output_dir)
    base.mkdir(parents=True, exist_ok=True)
    total = probe_duration_seconds(input_video)
    if total <= 0:
        raise RuntimeError("Input video duration is invalid (<= 0).")
    rendered: list[RenderedProfile] = []
    for profile in PROFILES:
        silences = detect_silences(
            input_video,
            noise_threshold_db=profile.noise_threshold_db,
            min_silence_sec=profile.min_silence_sec,
        )
        keep = build_keep_segments(
            total,
            silences,
            padding_sec=profile.padding_sec,
            min_keep_sec=profile.min_keep_sec,
        )
        after = output_duration_from_keep_segments(keep)
        dur_tag = duration_before_after_tag(total, after)
        output_path = base / f"{output_prefix}_{dur_tag}_nosilence_{profile.name}.mp4"
        _render_segments(input_video, str(output_path), keep)
        rendered.append(
            RenderedProfile(
                profile=profile.name,
                output_path=str(output_path),
                removed_silences=len(silences),
            )
        )
    return rendered


def remove_silence_selected_profiles(
    input_video: str,
    output_dir: str,
    output_prefix: str,
    profile_names: list[str] | None = None,
    override_min_keep_sec: float | None = None,
    override_merge_gap_sec: float | None = None,
) -> list[RenderedProfile]:
    selected = set(profile_names or [])
    profiles = [p for p in PROFILES if not selected or p.name in selected]
    if not profiles:
        raise RuntimeError("No valid silence profile selected.")
    base = Path(output_dir)
    base.mkdir(parents=True, exist_ok=True)
    total = probe_duration_seconds(input_video)
    if total <= 0:
        raise RuntimeError("Input video duration is invalid (<= 0).")
    rendered: list[RenderedProfile] = []
    for profile in profiles:
        silences = detect_silences(
            input_video,
            noise_threshold_db=profile.noise_threshold_db,
            min_silence_sec=profile.min_silence_sec,
        )
        keep = build_keep_segments(
            total,
            silences,
            padding_sec=profile.padding_sec,
            min_keep_sec=max(0.01, float(override_min_keep_sec))
            if override_min_keep_sec is not None
            else profile.min_keep_sec,
        )
        if override_merge_gap_sec is not None:
            keep = merge_keep_segments_by_gap(keep, max(0.0, float(override_merge_gap_sec)))
        after = output_duration_from_keep_segments(keep)
        dur_tag = duration_before_after_tag(total, after)
        output_path = base / f"{output_prefix}_{dur_tag}_nosilence_{profile.name}.mp4"
        _render_segments(input_video, str(output_path), keep)
        rendered.append(
            RenderedProfile(
                profile=profile.name,
                output_path=str(output_path),
                removed_silences=len(silences),
            )
        )
    return rendered
