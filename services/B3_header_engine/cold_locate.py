# -*- coding: utf-8 -*-
"""冷库定位 —— `memory_id ↔ 整天 JSONL 里那一行` 的**唯一**实现。

为什么单独抽一个模块
--------------------
这条规则（`memory_id = md5("{日期}#{原始 id}")[:12]`）在本仓**已经被抄了三份**：

    sqlite_store._memory_id        warm_indexer.memory_id_for        B6 protocol.hashlib_md5_12

路4 的原文索引也要按它去冷库取原文。再抄第四份，就是**第四处**将来可能不一致的地方；
而不一致的表现是"反查不到"（fail-closed），或者更糟 —— **张冠李戴**
（B6 在 2026-09-23 就吃过一次：只有 1 行的 JSONL 被 `json.loads` 当单文档认成
"这条记忆的原文"，实际返回的是**别人的内容**）。

所以规则在这里定义一次，其余各处**委托**过来。改的是纯函数、语义完全相同，
属于行为中性的收敛。

⚠️ 本模块**只依赖标准库**。它是 B3 的最底层，不能被上面任何服务反向 import
（否则 B3 与 B6 会成环）。B6 import 本模块是**正确方向**（B6 本来就 import B3）。
"""
from __future__ import annotations

import hashlib
import json
import os
import re

_DATE_RE = re.compile(r'(\d{4}-\d{2}-\d{2})')


def memory_id_for(date_str: str, entry_id: str) -> str:
    """`md5("{date}#{entry_id}")[:12]` —— 全仓唯一的 id 算法。

    与 V1 温记忆、`warm_indexer.memory_id_for`、`sqlite_store._memory_id`
    逐字一致；那两处现在委托到这里。
    """
    return hashlib.md5(f"{date_str}#{entry_id}".encode()).hexdigest()[:12]


def hashlib_md5_12(raw: str) -> str:
    """裸 md5 前 12 位（B6 的老入口，保留名字以免调用方改动）。"""
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def date_from_path(path: str):
    """从 `.../YYYY-MM-DD.jsonl` 取日期；取不到返回 `None`。"""
    m = _DATE_RE.search(os.path.basename(path or ''))
    return m.group(1) if m else None


def locate_jsonl_entry(path: str, memory_id: str):
    """在「整天冷库 JSONL」里按 memory_id 反查那一行。

    定位原理（2026-09-22 实测确认）：`memories.memory_id` 就是
    `md5(f"{日期}#{原始 id}")[:12]` —— B3 `_memory_id()` 与 warm_indexer
    `memory_id_for()` 是同一套算法，日期从文件名 `YYYY-MM-DD.jsonl` 取。
    于是逐行重算 hash 就能**精确命中**，不用新增字段、不用迁移、不用约定新目录。

    Returns: `(entry|None, 总行数, 失败原因)`
    """
    date_str = date_from_path(path)
    if not date_str:
        return None, 0, f'文件名里没有日期（{os.path.basename(path or "")}），无法反算 memory_id'

    total, bad = 0, 0
    with open(path, 'r', encoding='utf-8', errors='replace') as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                raw = json.loads(line)
            except Exception:      # noqa: BLE001 - 单行坏掉不该毁掉整次深挖
                bad += 1
                continue
            if not isinstance(raw, dict):
                continue
            raw_id = raw.get('id') or raw.get('entry_id') or ''
            if not raw_id:
                continue
            if hashlib_md5_12(f'{date_str}#{raw_id}') == memory_id:
                return raw, total, ''
    return None, total, (f'整天 JSONL 共 {total} 行（{bad} 行不是合法 JSON），'
                         f'但没有一行能反算出 memory_id={memory_id}')


def day_index(path: str):
    """**一次扫盘**建出 `{memory_id: entry}` —— 批量回填专用。

    为什么要有它：`locate_jsonl_entry` 是"一次查一条"，对**单条**增量写入没问题，
    但全量回填时按它逐条查就是 O(记忆数 × 当天行数)。实测 stage2 有 5,871 条记忆、
    218 个日文件 —— 逐条查要扫 ~600 万行，而按文件扫一遍只要 ~6 千行。

    Returns: `(idx, total, bad)`
    """
    date_str = date_from_path(path)
    idx, total, bad = {}, 0, 0
    if not date_str:
        return idx, total, bad
    with open(path, 'r', encoding='utf-8', errors='replace') as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                raw = json.loads(line)
            except Exception:      # noqa: BLE001
                bad += 1
                continue
            if not isinstance(raw, dict):
                continue
            raw_id = raw.get('id') or raw.get('entry_id') or ''
            if raw_id:
                idx[hashlib_md5_12(f'{date_str}#{raw_id}')] = raw
    return idx, total, bad


def source_text(entry: dict):
    """一条冷库记录的**原文**，以及"用的是哪一份文本"。

    ⚠️ **优先原文，不是摘要。** 摘要是有损的，而且**语言可能与原文不同**
    —— 本语料实测就是「英文原文 → 中文摘要」，而路4 的整套价值
    （检索层 63.9% vs 11.9%）正建立在"索引原文"上。

    兜底链（对齐 §五 步骤 4 语言方案的"兜底链 + 记录用了哪个"）：
    `user+assistant` → `text` → `summary`，返回的 `src` 如实标出用了哪个，
    好让"没召回"可解释。

    Returns: `(text, src)`，`src ∈ {'orig', 'text', 'summary', 'none'}`
    """
    entry = entry or {}
    parts = [str(entry.get('user') or ''), str(entry.get('assistant') or '')]
    body = '\n'.join(p for p in parts if p).strip()
    if body:
        return body, 'orig'
    txt = str(entry.get('text') or '').strip()
    if txt:
        return txt, 'text'
    sm = str(entry.get('summary') or '').strip()
    return (sm, 'summary') if sm else ('', 'none')
