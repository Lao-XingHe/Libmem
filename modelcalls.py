# -*- coding: utf-8 -*-
r"""数清楚"一次搜索调了几次模型" —— 为什么 16 并发从 0.21s 变 4.4s。

## 为什么要数

2026-10-22：单次搜索已优化到 **0.21s**（SQL 从 19,322 条砍到 867 条），
但 16 并发 p95 仍 **4.9s**，且**加到 4 个进程也毫无改善** ⇒
瓶颈必然在**所有进程共用的资源**上。剩下的候选只有两个模型服务。

如果是"每个候选都调一次嵌入"（phase1_limit=800 ⇒ 最多 800 次！），
那 16 并发必炸；改成批量一次调用就能立刻解决。
如果只有 1~2 次调用，那就是模型服务的 --parallel 不够，调大即可。

⚠️ 脚本必须放在**仓库目录**里跑：`$env:TEMP` 下有一个会遮蔽标准库
   `struct` 的文件，在那边跑会 `AttributeError: module 'struct' has no attribute 'calcsize'`。
"""
from __future__ import annotations

import collections
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
os.environ["SHUFANG_DATA_DIR"] = HERE
os.chdir(HERE)

import requests                                                    # noqa: E402

CALLS = collections.defaultdict(lambda: [0, 0.0])
_orig_post = requests.post
_orig_sess_post = requests.Session.post


def _record(url: str, dt: float) -> None:
    key = url.split("//")[-1].split("/")[0]
    CALLS[key][0] += 1
    CALLS[key][1] += dt


def post(url, *args, **kwargs):
    t0 = time.perf_counter()
    try:
        return _orig_post(url, *args, **kwargs)
    finally:
        _record(url, time.perf_counter() - t0)


def sess_post(self, url, *args, **kwargs):
    t0 = time.perf_counter()
    try:
        return _orig_sess_post(self, url, *args, **kwargs)
    finally:
        _record(url, time.perf_counter() - t0)


requests.post = post
requests.Session.post = sess_post

from services.B3_header_engine import sqlite_store as SS            # noqa: E402
from services.B4_dual_gate_retrieval import retrieve                # noqa: E402


def run(label: str, query: str, uid: str) -> None:
    CALLS.clear()
    t0 = time.perf_counter()
    out = retrieve(query_text=query, user_ids=[uid], max_items=100, final_topk=100,
                   max_tokens=10 ** 9, apply_heat=False, rerank_enabled=True,
                   route_fuse=True, rerank_doc="raw")
    wall = (time.perf_counter() - t0) * 1000
    items = len(out.get("items") or [])
    print(f"\n{label}: 总 {wall:.0f}ms · 返回 {items} 条")
    if not CALLS:
        print("  （没有任何模型调用！）")
        return
    total = 0.0
    for key, (count, sec) in sorted(CALLS.items(), key=lambda kv: -kv[1][1]):
        total += sec
        print(f"  {key:<22} 调用 {count:>4} 次 · 合计 {sec * 1000:7.0f}ms · "
              f"平均 {sec / max(count, 1) * 1000:6.1f}ms")
    print(f"  ⇒ 模型合计 {total * 1000:.0f}ms，占 {total * 1000 / wall * 100:.0f}%")


def main() -> int:
    conn = SS.get_conn()
    uid = conn.execute(
        "SELECT user_id FROM memories GROUP BY user_id ORDER BY COUNT(*) DESC LIMIT 1"
    ).fetchone()[0]
    rows = conn.execute("SELECT COUNT(*) FROM memories WHERE user_id=?", (uid,)).fetchone()[0]
    conn.close()
    print(f"目标用户 {uid[:14]}… 记忆 {rows:,} 条")

    q = "what did the user say about work and family"
    run("第 1 次（预热）", q, uid)
    run("第 2 次（测量）", q, uid)
    return 0


if __name__ == "__main__":
    sys.exit(main())
