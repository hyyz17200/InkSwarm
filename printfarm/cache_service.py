from __future__ import annotations

from pathlib import Path
import shutil


class CacheService:
    def __init__(self, cache_dir: Path, preview_dir: Path) -> None:
        self.cache_dir = cache_dir
        self.preview_dir = preview_dir

    def clear(self) -> None:
        if self.cache_dir.exists():
            shutil.rmtree(self.cache_dir, ignore_errors=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.preview_dir.mkdir(parents=True, exist_ok=True)
