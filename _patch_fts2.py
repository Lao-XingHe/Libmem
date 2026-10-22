# -*- coding: utf-8 -*-
"""精确补丁：fts_search / raw_search 改成"按真实稀有度选词"。

上一版用正则，把 fts_search 的定义吃掉了（ImportError）。这一版：
  * 不碰正则，只用**精确字符串替换**；
  * 以 `def raw_search(` 为界把文件切成两半，各半独立处理 —— 同名字符串不会串台；
  * 每次替换都断言"恰好命中 1 次"，否则直接失败退出（不写坏文件）；
  * 写回前 compile() 自检。
"""
from __future__ import annotations

import io

PATH = r"D:\Deepseek Harness\LibMem-AML\services\B3_header_engine\sqlite_store.py"
src = io.open(PATH, encoding="utf-8").read()

HELPER = '''# ============================================================
# FTS 表达式阶梯（2026-10-22：按**真实稀有度**选词）
# ============================================================
# 为什么：`bm25()` 会为**每个命中**查一次 docsize。只要 MATCH 命中该用户全部
# 记忆（`"a" OR "b" OR ...` 很容易如此），一次搜索就是 1.8 万条 SQL、1.3 秒；
# 16 并发直接超时。
#
# 关键洞察：**词长 ≠ 稀有度**（实测最长的词恰好是最常见的）。必须真的数
# 每个词命中多少篇，取最少的几个 AND 起来。探测 ~0.5ms/词，进程内带缓存。
_RARE_CACHE: dict = {}
_RARE_CACHE_MAX = 8192


def _count_token(table: str, token: str) -> int:
    """某词在该 FTS 表里命中多少篇（越小越稀有）。失败当成极大（= 不优先）。"""
    key = (table, token)
    cached = _RARE_CACHE.get(key)
    if cached is not None:
        return cached
    expr = '"' + token.replace('"', '""') + '"'
    conn = None
    value = 10 ** 9
    try:
        conn = get_conn()
        value = int(conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {table} MATCH ?", (expr,)
        ).fetchone()[0])
    except Exception:            # noqa: BLE001 - 探测失败不致命
        value = 10 ** 9
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:    # noqa: BLE001
                pass
    if len(_RARE_CACHE) > _RARE_CACHE_MAX:
        _RARE_CACHE.clear()
    _RARE_CACHE[key] = value
    return value


def _expr_ladder(tokens: list, table: str, max_probe: int = 4) -> list:
    """按选择性从高到低给出 MATCH 表达式阶梯（去重保序）。

    ① AND(全部词)      —— 最严，通常 0 命中，但一次极快的查询
    ② AND(最稀有 2 词) —— **主路径**：选择性高且保留相关性
    ③ OR(最稀有 2 词)
    ④ AND(最稀有 3 词)
    ⑤ OR(全部词)       —— 兜底保召回（最慢，尽量走不到）
    """
    quoted = ['"' + t.replace('"', '""') + '"' for t in tokens]
    probe = [t for t in tokens[:max_probe] if t]
    ranked = sorted(probe, key=lambda t: _count_token(table, t)) if probe else []
    rare_q = ['"' + t.replace('"', '""') + '"' for t in ranked]

    exprs, seen = [], set()

    def add(item: str) -> None:
        if item and item not in seen:
            seen.add(item)
            exprs.append(item)

    if len(quoted) > 1:
        add(" AND ".join(quoted))
    if len(rare_q) >= 2:
        add(" AND ".join(rare_q[:2]))
        add(" OR ".join(rare_q[:2]))
    if len(rare_q) >= 3:
        add(" AND ".join(rare_q[:3]))
    if len(rare_q) == 1:
        add(rare_q[0])
    add(" OR ".join(quoted))
    return exprs


'''

SPLIT = "def raw_search("
assert src.count(SPLIT) == 1, "拆分锚点必须唯一"
head, tail = src.split(SPLIT)

EXPR_OLD = "    expr = \" OR \".join('\"' + t.replace('\"', '\"\"') + '\"' for t in tokens)\n"
EXEC_OLD = ("        rows = conn.execute(sql, [expr] + params + [fetch_k]).fetchall()\n"
            "        out = []\n")


def patched(half: str, table_name: str, tag: str) -> str:
    assert half.count(EXPR_OLD) == 1, f"{tag}: expr 行必须恰好 1 处（实际 {half.count(EXPR_OLD)}）"
    half = half.replace(EXPR_OLD, f"    exprs = _expr_ladder(tokens, {table_name})\n", 1)
    assert half.count(EXEC_OLD) == 1, f"{tag}: 执行块必须恰好 1 处（实际 {half.count(EXEC_OLD)}）"
    half = half.replace(
        EXEC_OLD,
        "        rows = []\n"
        "        for expr in exprs:              # 逐级放宽：够 k 条就停\n"
        "            rows = conn.execute(sql, [expr] + params + [fetch_k]).fetchall()\n"
        "            if len(rows) >= int(k):\n"
        "                break\n"
        "        out = []\n",
        1)
    return half


head = patched(head, "_FTS_TABLE", "fts_search")
tail = patched(SPLIT + tail, "_RAW_FTS_TABLE", "raw_search")
src = head + tail

assert "def fts_search(" in src and "def raw_search(" in src, "两个函数必须都还在"
assert "_expr_ladder(tokens, _FTS_TABLE)" in src and "_expr_ladder(tokens, _RAW_FTS_TABLE)" in src

# 插到 fts_search 之前
if "_expr_ladder" in src.split("def fts_search(")[0]:
    print("  helper 已在位，跳过插入")
else:
    idx = src.index("def fts_search(")
    src = src[:idx] + HELPER + src[idx:]
    print("  已插入 _count_token / _expr_ladder")

compile(src, PATH, "exec")
io.open(PATH, "w", encoding="utf-8", newline="\n").write(src)
print("  ✅ 已写回并通过编译检查")
