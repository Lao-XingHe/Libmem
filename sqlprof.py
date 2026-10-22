# -*- coding: utf-8 -*-
r"""LibMem-AML · SQL profiling（找出 42 万行库里最慢的那条查询）

## 为什么这么写

不想改业务代码去插桩（那会把线上逻辑和观测混在一起）。改用 **`sqlite3` 的
`set_trace_callback`**：它在**每条 SQL 执行前**回调一次，于是
「相邻两次回调的时间差 ≈ 上一条语句的耗时」——
纯外部观测，业务代码一行不动。

`Connection` 是 C 类型、不能给它挂属性，但它有 `set_trace_callback` **方法**，
所以只 patch `sqlite_store.get_conn`（所有 SQL 都从它拿连接），给返回的连接装上探针即可。

用法::
    python sqlprof.py                 # 用库里最大的用户跑一次
    python sqlprof.py --query "..."   # 指定 query
"""
from __future__ import annotations

import argparse
import collections
import os
import sys
import time

ROOT = r"D:\Deepseek Harness\LibMem-AML"
os.environ["SHUFANG_DATA_DIR"] = ROOT
sys.path.insert(0, ROOT)
os.chdir(ROOT)

from services.B3_header_engine import sqlite_store as SS          # noqa: E402

SQL = collections.defaultdict(lambda: [0.0, 0])   # sql → [总秒数, 次数]
_state = {"sql": None, "t": None}


def _tracer(statement: str) -> None:
    now = time.perf_counter()
    prev, t0 = _state["sql"], _state["t"]
    if prev is not None and t0 is not None:
        key = " ".join(prev.split())[:110]
        SQL[key][0] += now - t0
        SQL[key][1] += 1
    _state["sql"], _state["t"] = statement, now


_orig_get_conn = SS.get_conn


def _patched_get_conn(db_path=None):
    conn = _orig_get_conn(db_path)
    try:
        conn.set_trace_callback(_tracer)
    except Exception:                                   # noqa: BLE001
        pass
    return conn


SS.get_conn = _patched_get_conn

from services.B4_dual_gate_retrieval import retrieve                # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--query", default="what did the user say about work and family")
ap.add_argument("--k", type=int, default=100)
a = ap.parse_args()

conn = _orig_get_conn()
uid = conn.execute(
    "SELECT user_id FROM memories GROUP BY user_id ORDER BY COUNT(*) DESC LIMIT 1"
).fetchone()[0]
total_rows = conn.execute("SELECT COUNT(*) FROM memories").fetchone()[0]
mine = conn.execute("SELECT COUNT(*) FROM memories WHERE user_id=?", (uid,)).fetchone()[0]
conn.close()

print(f"库总行数 {total_rows:,} · 目标用户 {uid[:14]}… 记忆 {mine:,} 条\n")

for label in ("第 1 次（冷）", "第 2 次（热）"):
    SQL.clear()
    _state["sql"], _state["t"] = None, None
    t0 = time.perf_counter()
    out = retrieve(query_text=a.query, user_ids=[uid], max_items=a.k, final_topk=a.k,
                   max_tokens=10 ** 9, apply_heat=False, rerank_enabled=True,
                   route_fuse=True, rerank_doc="raw")
    wall = time.perf_counter() - t0
    sql_total = sum(v[0] for v in SQL.values())
    n_sql = sum(v[1] for v in SQL.values())
    print(f"=== {label}: 总 {wall:.2f}s · SQL {sql_total:.2f}s / {n_sql} 条 · "
          f"返回 {len(out.get('items') or [])} 条")

print("\n=== 最慢的 SQL（按总耗时）===")
for stmt, (sec, cnt) in sorted(SQL.items(), key=lambda kv: -kv[1][0])[:12]:
    print(f"  {sec:7.3f}s  ×{cnt:<5}  avg {sec / max(cnt, 1) * 1000:7.1f}ms  {stmt[:110]}")
