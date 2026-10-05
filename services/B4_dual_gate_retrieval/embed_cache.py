"""B4 向量落盘缓存（sidecar SQLite）

背景
----
`retrieval.retrieve()` 原实现在每次检索时，对 Phase1 放行的**全部**候选
（默认上限 1000，本库 771 条）逐条调用 Ollama 生成向量。实测单条 ~2.17s，
全库一次检索约 28 分钟，远超工具调用超时。

本模块把「文本 → 向量」的结果持久化，键是 `sha256(模型名 + 文本)`：
- 文本没变 → 永久命中，检索只剩「query 向量 + 若干余弦」，实测约 3s
- 文本变了 → 哈希变化，自动失效并重算，不会读到脏向量
- 新增记忆 → 只补算新增那几条，增量成本可忽略

存储位置：`config.paths.index_dir/embed_cache.db`（默认 `data/index/`），
与 `header.db` 并列，不修改既有 schema，可随时删除重建。
"""
import array
import hashlib
import os
import sqlite3
import sys
import threading
import time
from typing import Iterable, Optional

_DB_NAME = "embed_cache.db"
_LOCK = threading.RLock()
_CONN: Optional[sqlite3.Connection] = None


# ---------- 配置 ----------

def _retrieval_cfg() -> dict:
    try:
        from services.shared.config.loader import get_config
        return (get_config() or {}).get("retrieval", {}) or {}
    except Exception:
        return {}


def enabled() -> bool:
    """缓存开关（config.retrieval.embed_cache，默认开）"""
    return bool(_retrieval_cfg().get("embed_cache", True))


def _index_dir() -> str:
    try:
        from services.shared.config.loader import get_data_path
        d = get_data_path("index_dir", "./data/index")
    except Exception:
        d = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__)))), "data", "index")
    os.makedirs(d, exist_ok=True)
    return d


def cache_path() -> str:
    return os.path.join(_index_dir(), _DB_NAME)


# ---------- 键与序列化 ----------

def make_key(text: str, model: str = "") -> str:
    """缓存键 = sha256(model \\0 text)。文本或模型任一变则失效。"""
    from .retrieval import _EMBED_MODEL  # 延迟导入，避免包初始化环
    m = model or _EMBED_MODEL
    h = hashlib.sha256()
    h.update(m.encode("utf-8"))
    h.update(b"\x00")
    h.update((text or "").encode("utf-8"))
    return h.hexdigest()


def _pack(vec: Iterable[float]) -> bytes:
    a = array.array("f", vec)
    if sys.byteorder != "little":
        a.byteswap()
    return a.tobytes()


def _unpack(buf: bytes):
    """返回 array('f') 而不是 list：省掉 77 万次 Python float 分配。

    771 × 1024 维转成 list 要 ~2.3s，保持 C 缓冲区只要 ~0.3s。
    array 支持 len/索引/迭代/map，与原有用法完全兼容。
    """
    a = array.array("f")
    a.frombytes(buf)
    if sys.byteorder != "little":
        a.byteswap()
    return a


# ---------- SQLite ----------

def _conn() -> sqlite3.Connection:
    global _CONN
    with _LOCK:
        if _CONN is None:
            c = sqlite3.connect(cache_path(), check_same_thread=False, timeout=30.0)
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA synchronous=NORMAL")
            c.execute("""
                CREATE TABLE IF NOT EXISTS embed_cache (
                    key        TEXT PRIMARY KEY,
                    model      TEXT NOT NULL,
                    dim        INTEGER NOT NULL,
                    vec        BLOB NOT NULL,
                    text_len   INTEGER,
                    updated_at TEXT NOT NULL
                )
            """)
            c.execute("CREATE INDEX IF NOT EXISTS idx_embed_cache_model ON embed_cache(model)")
            c.commit()
            _CONN = c
        return _CONN


def get_vectors(keys: list) -> dict:
    """批量取向量，返回 {key: [float]}（未命中的键不出现）"""
    out = {}
    if not keys:
        return out
    c = _conn()
    with _LOCK:
        # SQLite 变量上限 999，分批查
        for i in range(0, len(keys), 500):
            chunk = keys[i:i + 500]
            q = "SELECT key, vec FROM embed_cache WHERE key IN (%s)" % ",".join("?" * len(chunk))
            for k, blob in c.execute(q, chunk):
                out[k] = _unpack(blob)
    return out


def put_vectors(pairs: list) -> int:
    """批量写入 [(key, vec), ...]，返回写入条数"""
    if not pairs:
        return 0
    from .retrieval import _EMBED_MODEL
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    rows = [(k, _EMBED_MODEL, len(v), _pack(v), None, now) for k, v in pairs if v]
    c = _conn()
    with _LOCK:
        c.executemany(
            "INSERT OR REPLACE INTO embed_cache (key, model, dim, vec, text_len, updated_at)"
            " VALUES (?,?,?,?,?,?)", rows)
        c.commit()
    return len(rows)


def stats() -> dict:
    """缓存概况：条数 / 维度 / 占用字节 / 文件路径"""
    try:
        c = _conn()
        with _LOCK:
            n = c.execute("SELECT COUNT(*) FROM embed_cache").fetchone()[0]
            dims = [r[0] for r in c.execute("SELECT DISTINCT dim FROM embed_cache")]
            models = [r[0] for r in c.execute("SELECT DISTINCT model FROM embed_cache")]
        size = os.path.getsize(cache_path()) if os.path.exists(cache_path()) else 0
        return {"entries": n, "dims": dims, "models": models,
                "size_bytes": size, "size_mb": round(size / 1048576, 2),
                "path": cache_path()}
    except Exception as e:
        return {"entries": 0, "error": repr(e), "path": cache_path()}


def coverage(texts: list) -> dict:
    """按真实候选文本统计覆盖率（比 entries/total 准：相同摘要共用一个条目）

    Returns: {candidates, cached, missing, ratio}
    """
    keys = [make_key(t) for t in texts]
    if not keys:
        return {"candidates": 0, "cached": 0, "missing": 0, "ratio": 1.0}
    have = get_vectors(keys)
    cached = sum(1 for k in keys if k in have)
    return {
        "candidates": len(keys),
        "cached": cached,
        "missing": len(keys) - cached,
        "ratio": round(cached / len(keys), 4),
    }


def clear() -> int:
    """清空缓存（换嵌入模型后建议调用）"""
    c = _conn()
    with _LOCK:
        n = c.execute("SELECT COUNT(*) FROM embed_cache").fetchone()[0]
        c.execute("DELETE FROM embed_cache")
        c.commit()
        c.execute("VACUUM")
    return n


def prune(keep_keys: list) -> int:
    """删除不在 keep_keys 中的条目（清理已删记忆的残留向量）"""
    c = _conn()
    removed = 0
    with _LOCK:
        if not keep_keys:
            return clear()
        for i in range(0, len(keep_keys), 500):
            chunk = keep_keys[i:i + 500]
            q = "DELETE FROM embed_cache WHERE key NOT IN (%s)" % ",".join("?" * len(chunk))
            removed += c.execute(q, chunk).rowcount
        c.commit()
    return removed
