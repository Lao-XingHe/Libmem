import os, json, traceback as tb, threading, sys
from datetime import datetime

_log_dir: str | None = None
_lock = threading.Lock()

def _get_log_dir():
    env_dir = os.environ.get('SHUFANG_DATA_DIR')
    if env_dir:
        return os.path.normpath(os.path.join(env_dir, "logs"))
    base = os.path.dirname(os.path.abspath(__file__))
    return os.path.normpath(os.path.join(base, "data", "logs"))

def _init():
    global _log_dir
    _log_dir = _get_log_dir()
    os.makedirs(_log_dir, exist_ok=True)

def _ensure_init():
    if _log_dir is None:
        _init()

def log_error(module: str, message: str, traceback_str: str = ""):
    try:
        _ensure_init()
        entry = {
            "ts": datetime.now().isoformat(),
            "module": module,
            "level": "ERROR",
            "message": str(message)[:500],
            "traceback": str(traceback_str)[:3000],
        }
        date_str = datetime.now().strftime("%Y-%m-%d")
        fp = os.path.join(_log_dir, f"errors_{date_str}.jsonl")
        with _lock:
            with open(fp, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except:
        pass

def log_exception(module: str, e: Exception):
    log_error(module, str(e), tb.format_exc())

def get_recent_errors(limit: int = 50) -> list:
    try:
        _ensure_init()
        files = sorted([f for f in os.listdir(_log_dir) if f.startswith("errors_") and f.endswith(".jsonl")], reverse=True)
        entries = []
        for fname in files:
            fp = os.path.join(_log_dir, fname)
            try:
                with open(fp, "r", encoding="utf-8") as fh:
                    for line in fh:
                        try:
                            entries.append(json.loads(line.strip()))
                        except: pass
            except: pass
            if len(entries) >= limit:
                break
        return entries[-limit:]
    except: return []

def export_errors() -> str:
    try:
        _ensure_init()
        files = sorted([f for f in os.listdir(_log_dir) if f.startswith("errors_") and f.endswith(".jsonl")], reverse=True)
        all_entries = []
        for fname in files:
            fp = os.path.join(_log_dir, fname)
            try:
                with open(fp, "r", encoding="utf-8") as fh:
                    for line in fh:
                        try:
                            all_entries.append(json.loads(line.strip()))
                        except: pass
            except: pass
        return json.dumps({"exported_at": datetime.now().isoformat(), "count": len(all_entries), "errors": all_entries}, ensure_ascii=False, indent=2)
    except: return "{}"

def clear_errors() -> int:
    try:
        _ensure_init()
        count = 0
        files = [f for f in os.listdir(_log_dir) if f.startswith("errors_") and f.endswith(".jsonl")]
        for fname in files:
            fp = os.path.join(_log_dir, fname)
            try: os.remove(fp); count += 1
            except: pass
        return count
    except: return 0
