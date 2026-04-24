from pathlib import Path

from webapp import transcribe_jobs as tj


def test_probe_duration_falls_back_to_ffmpeg_when_ffprobe_missing(monkeypatch, tmp_path: Path):
    target = tmp_path / "sample.mp3"
    target.write_bytes(b"fake")

    class Proc:
        def __init__(self, returncode: int, stdout: str = "", stderr: str = "") -> None:
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    calls: list[list[str]] = []

    def fake_run(cmd, *args, **kwargs):
        calls.append(list(cmd))
        if cmd[0] == "ffprobe":
            raise FileNotFoundError("ffprobe not found")
        # ffmpeg -i probe output style
        return Proc(returncode=1, stderr="Duration: 00:01:23.50, start: 0.000000, bitrate: 128 kb/s")

    monkeypatch.setattr("webapp.transcribe_jobs.subprocess.run", fake_run)

    got = tj._probe_duration_seconds(target)
    assert got == 83.5
    assert calls[0][0] == "ffprobe"
    assert calls[1][0] == "ffmpeg"

