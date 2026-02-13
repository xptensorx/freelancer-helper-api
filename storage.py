import json
import os
import tempfile
from typing import Any, Dict, Optional


def load_json(path: str, *, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if default is None:
        default = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return dict(default)
    except json.JSONDecodeError:
        # Corrupt state file: don't crash the whole run
        return dict(default)


def save_json_atomic(path: str, data: Dict[str, Any]) -> None:
    folder = os.path.dirname(path) or "."
    os.makedirs(folder, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(prefix=".tmp_", suffix=".json", dir=folder)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.write("\n")
        os.replace(tmp_path, path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass


def append_jsonl(path: str, obj: Dict[str, Any]) -> None:
    folder = os.path.dirname(path) or "."
    os.makedirs(folder, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


class JsonFileCache:
    """
    Very small persistent cache for user objects keyed by ID.
    Stored as a single JSON dict { "<id>": {...}, ... }.
    """

    def __init__(self, path: str):
        self.path = path
        self.data: Dict[str, Any] = load_json(path, default={})

    def get(self, user_id: int) -> Optional[Dict[str, Any]]:
        return self.data.get(str(user_id))

    def set(self, user_id: int, user_obj: Dict[str, Any]) -> None:
        self.data[str(user_id)] = user_obj

    def save(self) -> None:
        save_json_atomic(self.path, self.data)

