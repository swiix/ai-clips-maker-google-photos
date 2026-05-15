"""
Burn Instagram-style karaoke word captions into exported videos via ASS + ffmpeg.
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Sequence

from webapp.openai_speech_trim import _extract_audio_mp3, transcribe_verbose_json

logger = logging.getLogger(__name__)

_WORD_RE = re.compile(r"^[\w']+$", re.UNICODE)


def extract_words_from_verbose(verbose: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalize OpenAI verbose_json into [{word, start, end}, ...]."""
    words: list[dict[str, Any]] = []

    top = verbose.get("words")
    if isinstance(top, list) and top:
        words.extend(_normalize_word_rows(top))

    if not words:
        segments = verbose.get("segments")
        if isinstance(segments, list):
            for seg in segments:
                seg_words = seg.get("words") if isinstance(seg, dict) else None
                if isinstance(seg_words, list):
                    words.extend(_normalize_word_rows(seg_words))

    return words


def _normalize_word_rows(rows: Sequence[Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        text = str(row.get("word") or row.get("text") or "").strip()
        if not text:
            continue
        try:
            start = float(row.get("start", 0.0))
            end = float(row.get("end", start))
        except (TypeError, ValueError):
            continue
        if end <= start:
            end = start + 0.08
        out.append({"word": text, "start": start, "end": end})
    return out


def words_from_transcription_interval(
    transcription_json_path: str | Path,
    t0: float,
    t1: float,
) -> list[dict[str, Any]]:
    """Build word timings for a clip interval from WhisperX transcription.json."""
    from ai_clips_maker.transcribe.transcription import Transcription

    tr = Transcription(json.loads(Path(transcription_json_path).read_text(encoding="utf-8")))
    rows: list[dict[str, Any]] = []
    clip_start = max(0.0, float(t0))
    clip_end = max(clip_start, float(t1))
    for w in tr.words:
        ws = float(w.start_time)
        we = float(w.end_time)
        if we <= clip_start or ws >= clip_end:
            continue
        text = str(w.text or "").strip()
        if not text:
            continue
        rel_start = max(0.0, ws - clip_start)
        rel_end = min(clip_end - clip_start, we - clip_start)
        if rel_end <= rel_start:
            rel_end = rel_start + 0.08
        rows.append({"word": text, "start": rel_start, "end": rel_end})
    return rows


def transcribe_words_openai(
    video_path: str | Path,
    api_key: str,
    *,
    model: str = "whisper-1",
) -> list[dict[str, Any]]:
    """Transcribe final video audio and return word-level timestamps."""
    video_path = Path(video_path)
    with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as tmp:
        mp3_path = tmp.name
    try:
        _extract_audio_mp3(str(video_path), mp3_path)
        verbose = transcribe_verbose_json(
            api_key,
            mp3_path,
            model=model,
            include_word_timestamps=True,
        )
    finally:
        try:
            Path(mp3_path).unlink(missing_ok=True)
        except OSError:
            pass
    words = extract_words_from_verbose(verbose)
    if not words:
        raise RuntimeError("OpenAI transcription returned no word timestamps for captions.")
    return words


def probe_video_size(video_path: str | Path) -> tuple[int, int]:
    proc = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height",
            "-of",
            "csv=p=0:s=x",
            str(video_path),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"ffprobe failed for video size: {(proc.stderr or proc.stdout or '').strip()}"
        )
    line = (proc.stdout or "").strip().splitlines()[0] if proc.stdout else ""
    parts = line.split("x")
    if len(parts) != 2:
        raise RuntimeError(f"Could not parse video dimensions from ffprobe: {line!r}")
    return int(parts[0]), int(parts[1])


def _ass_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("{", "\\{").replace("}", "\\}")


def _sec_to_ass(ts: float) -> str:
    ts = max(0.0, float(ts))
    h = int(ts // 3600)
    m = int((ts % 3600) // 60)
    s = ts % 60
    return f"{h}:{m:02d}:{s:05.2f}"


def _instagram_font_size(height: int) -> int:
    return int(max(36, min(96, round(height * 0.055))))


def build_karaoke_ass(words: Sequence[dict[str, Any]], width: int, height: int) -> str:
    """One centered Dialogue event per word (karaoke pop-in style)."""
    w = max(1, int(width))
    h = max(1, int(height))
    fontsize = _instagram_font_size(h)
    lines = [
        "[Script Info]",
        "ScriptType: v4.00+",
        f"PlayResX: {w}",
        f"PlayResY: {h}",
        "WrapStyle: 0",
        "",
        "[V4+ Styles]",
        "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding",
        (
            f"Style: Instagram,Arial Black,{fontsize},&H00FFFFFF,&H000000FF,&H00000000,&H96000000,"
            f"-1,0,0,0,100,100,0,0,1,4,2,5,40,40,80,1"
        ),
        "",
        "[Events]",
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text",
    ]
    for row in words:
        text = str(row.get("word") or "").strip()
        if not text:
            continue
        start = float(row.get("start", 0.0))
        end = float(row.get("end", start + 0.1))
        if end <= start:
            end = start + 0.1
        display = _ass_escape(text.upper() if _WORD_RE.match(text) else text)
        lines.append(
            f"Dialogue: 0,{_sec_to_ass(start)},{_sec_to_ass(end)},Instagram,,0,0,0,,{display}"
        )
    if len(lines) <= 10:
        raise RuntimeError("No caption events generated for ASS file.")
    return "\n".join(lines) + "\n"


def _escape_ffmpeg_path(path: Path) -> str:
    return str(path).replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'")


def burn_captions_into_video(
    video_path: str | Path,
    words: Sequence[dict[str, Any]],
    *,
    work_dir: str | Path | None = None,
) -> None:
    """Re-encode video with burned-in ASS captions (in-place)."""
    src = Path(video_path)
    if not src.is_file():
        raise RuntimeError(f"Video not found for caption burn: {src}")
    width, height = probe_video_size(src)
    ass_body = build_karaoke_ass(words, width, height)
    base_dir = Path(work_dir) if work_dir else src.parent
    base_dir.mkdir(parents=True, exist_ok=True)
    ass_path = base_dir / f"{src.stem}_captions.ass"
    ass_path.write_text(ass_body, encoding="utf-8")
    tmp_out = base_dir / f"{src.stem}_captioned_tmp.mp4"
    vf = f"ass='{_escape_ffmpeg_path(ass_path)}'"
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(src),
        "-vf",
        vf,
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "18",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        str(tmp_out),
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True, check=False)
    if proc.returncode != 0 or not tmp_out.is_file():
        detail = (proc.stderr or proc.stdout or "").strip() or str(proc.returncode)
        raise RuntimeError(f"ffmpeg caption burn failed: {detail}")
    src.unlink(missing_ok=True)
    tmp_out.rename(src)
    logger.info("Burned %s word captions into %s", len(words), src.name)
