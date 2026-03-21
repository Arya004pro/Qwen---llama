"""Patch installed motia/iii packages so flow metadata reaches iii-console Flow UI.

This script is idempotent and safe to run repeatedly.
"""

from __future__ import annotations

import site
from pathlib import Path


def _patch_file(path: Path, replacements: list[tuple[str, str]]) -> tuple[bool, list[str]]:
    text = path.read_text(encoding="utf-8")
    updated = text
    applied: list[str] = []

    for old, new in replacements:
        if new in updated:
            continue
        if old in updated:
            updated = updated.replace(old, new)
            applied.append(old.splitlines()[0].strip()[:80])

    if updated != text:
        path.write_text(updated, encoding="utf-8")
        return True, applied
    return False, applied


def _site_packages() -> Path:
    for p in site.getsitepackages():
        if p.endswith("site-packages"):
            return Path(p)
    raise RuntimeError("Could not locate site-packages path")


def main() -> None:
    sp = _site_packages()

    iii_file = sp / "iii" / "iii.py"
    motia_runtime = sp / "motia" / "runtime.py"

    if not iii_file.exists() or not motia_runtime.exists():
        raise FileNotFoundError(f"Expected package files not found under {sp}")

    iii_changed, _ = _patch_file(
        iii_file,
        [
            (
                "def register_function(self, path: str, handler: RemoteFunctionHandler, description: str | None = None) -> None:",
                "def register_function(\n"
                "        self,\n"
                "        path: str,\n"
                "        handler: RemoteFunctionHandler,\n"
                "        description: str | None = None,\n"
                "        metadata: dict[str, Any] | None = None,\n"
                "    ) -> None:",
            ),
            (
                "msg = RegisterFunctionMessage(id=path, description=description)",
                "msg = RegisterFunctionMessage(id=path, description=description, metadata=metadata)",
            ),
        ],
    )

    runtime_changed, _ = _patch_file(
        motia_runtime,
        [
            (
                "get_instance().register_function(function_id, api_handler)",
                "get_instance().register_function(function_id, api_handler, config.description, metadata)",
            ),
            (
                "get_instance().register_function(function_id, queue_handler)",
                "get_instance().register_function(function_id, queue_handler, config.description, metadata)",
            ),
            (
                "get_instance().register_function(function_id, cron_handler)",
                "get_instance().register_function(function_id, cron_handler, config.description, metadata)",
            ),
            (
                "get_instance().register_function(function_id, state_handler)",
                "get_instance().register_function(function_id, state_handler, config.description, metadata)",
            ),
            (
                "get_instance().register_function(function_id, stream_handler)",
                "get_instance().register_function(function_id, stream_handler, config.description, metadata)",
            ),
        ],
    )

    status = []
    status.append("patched iii" if iii_changed else "iii already patched")
    status.append("patched motia runtime" if runtime_changed else "motia runtime already patched")
    print(" | ".join(status))


if __name__ == "__main__":
    main()
