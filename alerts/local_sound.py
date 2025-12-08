"""Local audio playback helper with a notifier wrapper."""

from __future__ import annotations

import logging
import platform
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from alerts.notifiers.base import Notifier, NotifierTestResult, NotificationMessage

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


@dataclass(slots=True)
class LocalSoundNotifier(Notifier):
    """本地声音通道，实现统一通知接口。"""

    enabled_flag: bool
    sound_file: Optional[str] = None
    volume: float = 1.0
    name: str = "local_sound"

    def enabled(self) -> bool:
        return self.enabled_flag

    async def send(self, message: NotificationMessage) -> bool:
        if not self.enabled():
            return False
        play(self.sound_file, self.volume)
        LOGGER.info("Local sound triggered for message: %s", message.title)
        return True

    async def self_test(self) -> NotifierTestResult:
        try:
            play(self.sound_file, self.volume)
            return NotifierTestResult(ok=True, detail="Local sound played")
        except Exception as exc:  # pragma: no cover - environment dependent
            return NotifierTestResult(ok=False, detail=str(exc))


__all__ = ["play", "test_play", "LocalSoundNotifier"]
