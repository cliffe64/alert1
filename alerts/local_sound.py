"""Local audio playback helper with graceful degradation."""

from __future__ import annotations

import logging
import os
import platform
import subprocess
from pathlib import Path
from typing import Optional

LOGGER = logging.getLogger(__name__)


def _beep() -> None:
    try:
        print("\a", end="", flush=True)
    except Exception:  # pragma: no cover - fallback for unusual consoles
        LOGGER.debug("Terminal bell unavailable")


def play(sound_file: Optional[str], volume: float = 1.0) -> None:
    """Play ``sound_file`` if possible, otherwise emit a console beep."""

    if not sound_file:
        _beep()
        return
    path = Path(sound_file)
    if not path.exists():
        LOGGER.warning("Sound file %s does not exist", sound_file)
        _beep()
        return
    try:  # pragma: no cover - depends on optional packages
        import simpleaudio  # type: ignore

        wave = simpleaudio.WaveObject.from_wave_file(str(path))
        play_obj = wave.play()
        play_obj.wait_done()
        return
    except Exception:
        LOGGER.debug("simpleaudio unavailable, falling back to system player")

    system = platform.system().lower()
    command: Optional[list[str]] = None
    if system == "darwin":
        command = ["afplay", str(path)]
    elif system == "linux":
        command = ["aplay", str(path)]
    elif system == "windows":
        try:
            import winsound  # type: ignore

            winsound.PlaySound(str(path), winsound.SND_FILENAME)
            return
        except Exception:
            command = None

    if command:
        try:
            subprocess.run(command, check=True)  # pragma: no cover
            return
        except Exception:
            LOGGER.exception("Failed to play sound via %s", command)

    _beep()


def test_play(sound_file: Optional[str] = None) -> None:
    play(sound_file)


__all__ = ["play", "test_play"]
