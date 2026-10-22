# -*- coding: utf-8 -*-
r"""给 sqlite_store 打补丁：fts_search / raw_search 改成"按真实稀有度选词"。

## 为什么是"稀有度"而不是"词长"

2026-10-22 实测：一次搜索执行 **19,000+ 条 SQL**，其中 18,600 条是
`SELECT sz FROM memories_fts[_raw]_docsize WHERE id=?` —— 那是 FTS5 的 `bm25()`
在为**每一个命中文档**查 docsize。而我们的表达式 `"a" OR "b" OR ...` 命中了
该用户全部约 9,800 条（summary 与 raw 各一遍）。

第一次尝试用"最长的词"做 AND —— **无效**：最长的词恰好是 `about`/`family`
这种在该用户数据里遍地都是的词。**词长不等于稀有度。**

正确做法：**真的数一下**每个词命中多少篇，取最少的两个 AND 起来。
探测本身很便宜（稀有词 ~0.5ms），而且进程内带缓存（同一批评测里词高度重复）。
"""
from __future__ import annotations

import io
import os
import re

PATH = r"D:\Deepseek Harness\LibMem-AML\services\B3_header_engine\sqlite_store.py"
src = io.open(PATH, encoding="utf-8").read()

HELPER = '''
# ============================================================
# FTS 表达式阶梯（2026-10-22：按**真实稀有度**选词）
# ============================================================
# 为什么需要它：`bm25()` 会为**每个命中**查一次 docsize。只要 MATCH 表达式命中
# 该用户全部记忆（`"a" OR "b" OR ...` 很容易如此），一次搜索就是 1.8 万条 SQL、
# 1.3 秒；16 并发直接超时。
#
# 关键洞察：**词长 ≠ 稀有度**。必须真的数每个词命中多少篇，取最少的几个 AND 起来。
# 探测 ~0.5ms/词，且带进程内缓存（评测里词高度重复，命中率很高）。
_RARE_CACHE: dict = {}
_RARE_CACHE_MAX = 8192


def _count_token(table: str, token: str) -> int:
    """某个词在指定 FTS 表里命中多少篇（越小越稀有）。出错当成极大。"""
    key = (table, token)
    cached = _RARE_CACHE.get(key)
    if cached is not None:
        return cached
    expr = '"' + token.replace('"', '""') + '"'
    value = 10 ** 9
    conn = None
    try:
        conn = get_conn()
        value = int(conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {table} MATCH ?", (expr,)
        ).fetchone()[0])
    except Exception:            # noqa: BLE001 - 探测失败不致命，当成"很常见"
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

    ① AND(全部词)       —— 最严，通常 0 命中，但只有一次极快的查询
    ② AND(最稀有 2 词)  —— 主路径：既选择性高又保留相关性
    ③ AND(最稀有 3 词)
    ④ OR(最稀有 2 词)
    ⑤ OR(全部词)        —— 兜底保召回（最慢，尽量不走到）
    """
    quoted = ['"' + t.replace('"', '""') + '"' for t in tokens]
    probe = [t for t in tokens[:max_probe] if t]
    ranked = sorted(probe, key=lambda t: _count_token(table, t)) if probe else []
    rare_q = ['"' + t.replace('"', '""') + '"' for t in ranked]

    exprs, seen = [], set()

    def add(expr: str) -> None:
        if expr and expr not in seen:
            seen.add(expr)
            exprs.append(expr)

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

# 1) 插入 helper（放在 fts_search 之前）
anchor = "def fts_search("
if "_expr_ladder" not in src:
    idx = src.index(anchor)
    src = src[:idx] + HELPER.lstrip("\n") + "\n" + src[idx:]
    print("  已插入 _count_token / _expr_ladder")
else:
    print("  helper 已存在，跳过")

# 2) fts_search：把旧的阶梯块换成 _expr_ladder
old_fts = re.search(
    r"\n    quoted = \[.*?\n    seen_expr = set\(\)\n    exprs = \[e for e in exprs if not \(e in seen_expr or seen_expr\.add\(e\)\)\]\n",
    src, re.S)
if old_fts:
    src = src[:old_fts.start()] + "\n    exprs = _expr_ladder(tokens, _FTS_TABLE)\n" + src[old_fts.end():]
    print("  fts_search 阶梯已替换为 _expr_ladder")
elif "exprs = _expr_ladder(tokens, _FTS_TABLE)" in src:
    print("  fts_search 已是新阶梯")
else:
    print("  ⚠️ fts_search 未匹配")

# 3) fts_search 查询循环（若还没改成逐个试探）
if "for expr in exprs:                      # 逐级放宽，够用就停" not in src and "for expr in exprs:" not in src:
    old_one = ('        rows = conn.execute(sql, [expr] + params + [fetch_k]).fetchall()\n'
               '        out = []')
    new_one = ('        rows = []\n'
               '        for expr in exprs:                      # 逐级放宽，够用就停\n'
               '            rows = conn.execute(sql, [expr] + params + [fetch_k]).fetchall()\n'
               '            if len(rows) >= int(k):\n'
               '                break\n'
               '        out = []')
    if old_one in src:
        src = src.replace(old_one, new_one, 1)
        print("  fts_search 已改为逐级试探")
    else:
        print("  ⚠️ fts_search 查询循环未匹配")

# 4) raw_search：同样的改造
old_raw_expr = ('    expr = " OR ".join(\'"\' + t.replace(\'"\', \'""\') + \'"\' for t in tokens)\n'
                '    where, params = _filter_where(task_types, time_buckets, layer, prefix="m.",\n'
                '                                  session_ids=session_ids, user_ids=user_ids)\n'
                '    fetch_k = int(k) * 3 if core_entity_any else int(k)\n'
                '    sql = (f"SELECT m.memory_id AS mid, m.core_entities AS ents "\n'
                '           f"FROM {_RAW_FTS_TABLE} f "')
new_raw_expr = ('    exprs = _expr_ladder(tokens, _RAW_FTS_TABLE)\n'
                '    where, params = _filter_where(task_types, time_buckets, layer, prefix="m.",\n'
                '                                  session_ids=session_ids, user_ids=user_ids)\n'
                '    fetch_k = int(k) * 3 if core_entity_any else int(k)\n'
                '    sql = (f"SELECT m.memory_id AS mid, m.core_entities AS ents "\n'
                '           f"FROM {_RAW_FTS_TABLE} f "')
if old_raw_expr in src:
    src = src.replace(old_raw_expr, new_raw_expr, 1)
    print("  raw_search 表达式已换成阶梯")
elif "exprs = _expr_ladder(tokens, _RAW_FTS_TABLE)" in src:
    print("  raw_search 已是新阶梯")
else:
    print("  ⚠️ raw_search 表达式未匹配")

old_raw_exec = ('        rows = conn.execute(sql, [expr] + params + [fetch_k]).fetchall()\n'
                '        out = []\n'
                '        for r in rows:\n'
                '            if not _entity_ok(r["ents"], core_entity_any):\n'
                '                continue\n'
                '            out.append(r["mid"])\n'
                '            if len(out) >= int(k):\n'
                '                break\n'
                '        return out\n'
                '    except Exception:      # noqa: BLE001 - 退化，不抛')
new_raw_exec = ('        rows = []\n'
                '        for expr in exprs:                      # 逐级放宽，够用就停\n'
                '            rows = conn.execute(sql, [expr] + params + [fetch_k]).fetchall()\n'
                '            if len(rows) >= int(k):\n'
                '                break\n'
                '        out = []\n'
                '        for r in rows:\n'
                '            if not _entity_ok(r["ents"], core_entity_any):\n'
                '                continue\n'
                '            out.append(r["mid"])\n'
                '            if len(out) >= int(k):\n'
                '                break\n'
                '        return out\n'
                '    except Exception:      # noqa: BLE001 - 退化，不抛')
if old_raw_exec in src:
    src = src.replace(old_raw_exec, new_raw_exec, 1)
    print("  raw_search 已改为逐级试探")
elif "for expr in exprs:" in src[src.index("def raw_search("):]:
    print("  raw_search 已是逐级试探")
else:
    print("  ⚠️ raw_search 查询循环未匹配")

# 5) 语法自检后写回
compile(src, PATH, "exec")
io.open(PATH, "w", encoding="utf-8", newline="\n").write(src)
print("  ✅ 已写回并通过编译检查")
