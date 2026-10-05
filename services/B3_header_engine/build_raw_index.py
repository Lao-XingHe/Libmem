# -*- coding: utf-8 -*-
"""建/重建 **路4 原文索引**（`memories_fts_raw`），并做可机器判定的自检。

    python services/B3_header_engine/build_raw_index.py              # 重建 + 自检
    python services/B3_header_engine/build_raw_index.py --selfcheck  # 只自检
    python services/B3_header_engine/build_raw_index.py --demo 300   # 加跑跨语言对照

为什么单独一个维护脚本，而不是塞进 `ensure_schema`
--------------------------------------------------
`ensure_raw_fts()` 只在"表为空"时回填，够用于**新建库**；但存量库重建、
换库、以及"索引到底装了多少"的核对，需要一个**能反复跑、能报数**的入口。
本仓在这件事上踩过坑：`memories_fts` 曾经"表建好了却是 0 行"，
而 `fts_search` 按设计静默返回 `[]` —— 整条通道看起来在跑、实际从没生效。

自检项（每一项都是"不通过就说明这一步没做成"）
--------------------------------------------
1. `idrule_converged`  —— id 算法四处委托是否真的一致（唯一实现收敛后防漂移）
2. `day_index_matches` —— 批量扫盘与单条反查是否给出同一条记录
3. `coverage`          —— 索引行数 / 库行数（残索引就是死在这里）
4. `no_tok`            —— 有没有"取了文本却切不出 token"的条目
5. `self_retrieval`    —— 拿一条记忆自己的稀有 token 去搜，能不能搜回它自己
6. `cross_lang`（可选）—— 英文查询在**原文索引** vs **摘要索引**上的命中率对照
"""
from __future__ import annotations

import argparse
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

# ⚠️ Windows 控制台默认 GBK，打不出 `✅`/`⚠️` 会**直接把脚本打崩**
# —— 而且是在收尾打印结论那一行崩，前面跑的全白费（实测踩到）。
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding='utf-8', errors='replace')
    except Exception:                          # noqa: BLE001
        pass

from services.B3_header_engine import (          # noqa: E402
    get_conn, raw_rebuild, raw_stats, raw_search, raw_count, raw_available,
    fts_search, memory_id_for,
)
from services.B3_header_engine import cold_locate  # noqa: E402


def _db_path() -> str:
    conn = get_conn()
    try:
        for row in conn.execute('PRAGMA database_list'):
            return row[2] or '(memory)'
    finally:
        conn.close()
    return '?'


def check_converged(sample: int = 200) -> tuple:
    """id 算法是否一致（收敛之后防漂移）。

    ★★ 参赛版：参与方从四处降到**两处** ——
    原版还要比 `warm_indexer.memory_id_for`（B2 已移除）与
    `B6.hashlib_md5_12`（B6 已移除）。**留下的这两处正是参赛路径真正会用的**：
      * `cold_locate.memory_id_for` —— `raw_upsert_ids` 找不到原文时的兜底
      * `sqlite_store._memory_id`  —— 写入时的主算法
    ⚠️ 这两处不一致会让"写入用的 id"与"原文索引回填用的 id"对不上，
    表现是**原文索引里那一行永远为空**（路4 静默失效）。所以这个自检必须留。
    """
    from services.B3_header_engine import sqlite_store
    rng = random.Random(20260930)
    bad = []
    for _ in range(sample):
        d = f'{rng.randint(2016, 2026)}-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}'
        e = f'conv-{rng.randint(1, 99)}_s{rng.randint(1, 30)}_{rng.randint(1, 40)}'
        want = cold_locate.memory_id_for(d, e)
        got = {
            'cold_locate.memory_id_for': want,
            'cold_locate.hashlib_md5_12': cold_locate.hashlib_md5_12(f'{d}#{e}'),
            'sqlite_store._memory_id': sqlite_store._memory_id(d, e),
        }
        for k, v in got.items():
            if v != want:
                bad.append(f'{k}: {v} != {want}')
    return (not bad), bad


def check_day_index(sample: int = 40) -> tuple:
    """`day_index`（批量）与 `locate_jsonl_entry`（单条）是否指向同一条记录。"""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT memory_id, content_path FROM memories "
            "WHERE content_path IS NOT NULL AND content_path != ''").fetchall()
    finally:
        conn.close()
    if not rows:
        return True, []          # 没有 content_path 的库（纯温记忆）不适用
    rng = random.Random(7)
    paths = list({r['content_path'] for r in rows})
    rng.shuffle(paths)
    bad, tested = [], 0
    for p in paths[:8]:
        if not os.path.exists(p):
            continue
        idx = cold_locate.day_index(p)[0]
        for r in [x for x in rows if x['content_path'] == p][:6]:
            tested += 1
            one = cold_locate.locate_jsonl_entry(p, r['memory_id'])[0]
            if (one is None) != (idx.get(r['memory_id']) is None):
                bad.append(f"{r['memory_id']} 两种查法结论不一致")
            elif one is not None:
                a = one.get('id') or one.get('entry_id')
                b = idx[r['memory_id']].get('id') or idx[r['memory_id']].get('entry_id')
                if a != b:
                    bad.append(f"{r['memory_id']} 命中不同行：{a} vs {b}")
    return (not bad), bad + ([f'实测 {tested} 条'] if tested else ['无可用样本'])


def check_self_retrieval(sample: int = 120) -> tuple:
    """拿记忆自己的**稀有** token 去搜，能不能搜回它自己。

    为什么用稀有 token：常见 token（人名）会把结果冲散，测不出"这一条能不能被搜到"。
    这一项验证的是 **tokenizer ↔ 表 ↔ 查询**这一整条链真的通了。
    """
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT x.memory_id AS mid, f.seg AS seg FROM memories_raw_meta x "
            "JOIN memories_fts_raw f ON f.rowid = x.rowid "
            "WHERE x.src='orig' AND x.n_tok >= 4").fetchall()
    except Exception:      # noqa: BLE001
        return False, ['读不到原文索引（先重建）']
    finally:
        conn.close()
    if not rows:
        return False, ['原文索引里没有 src=orig 的条目']
    df: dict = {}
    for r in rows:
        for t in set((r['seg'] or '').split()):
            df[t] = df.get(t, 0) + 1
    rng = random.Random(11)
    picks = rng.sample(rows, min(sample, len(rows)))
    hit, miss = 0, []
    for r in picks:
        # ⚠️ 排序键必须带 token 本身：只按 df 排的话，**同 df 的并列**会落到
        # `set` 的迭代顺序上，而它依赖哈希种子 → 同一份数据两次跑出不同数字，
        # 自检就成了"时灵时不灵"的门，比没有门更糟。
        toks = sorted(set((r['seg'] or '').split()),
                      key=lambda t: (df.get(t, 0), t))[:2]
        if not toks:
            continue
        got = raw_search(' '.join(toks), k=5)
        if r['mid'] in got:
            hit += 1
        elif len(miss) < 5:
            miss.append(f"{r['mid']} 用 {toks} 搜不到")
    return (hit >= 0.9 * len(picks)), miss + [f'自检索命中 {hit}/{len(picks)}']


def cross_lang_demo(sample: int = 300) -> dict:
    """跨语言对照：**英文查询**在原文索引 vs 摘要索引上的命中率。

    这是本步存在理由的直接复现（评测口径下实测 63.9% vs 11.9%）：
    本语料的 `summary` 是中文、原文是英文，所以英文查询命中原文索引、
    却经常命中不了摘要索引。
    """
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT x.memory_id AS mid, f.seg AS seg FROM memories_raw_meta x "
            "JOIN memories_fts_raw f ON f.rowid = x.rowid "
            "WHERE x.src='orig' AND x.lang='en' AND x.n_tok >= 6").fetchall()
    except Exception:      # noqa: BLE001
        return {'error': '读不到原文索引'}
    finally:
        conn.close()
    if not rows:
        return {'error': '没有 lang=en 且 src=orig 的条目（本库可能不是英文原文）'}
    rng = random.Random(23)
    picks = rng.sample(rows, min(sample, len(rows)))
    hit_raw = hit_sum = 0
    for r in picks:
        # 同样要排序后再切：依赖 `set` 迭代顺序会让这个对照每次跑出不同数字。
        toks = sorted(t for t in set((r['seg'] or '').split())
                      if t.isascii() and len(t) >= 4)[:3]
        if not toks:
            continue
        q = ' '.join(toks)
        if r['mid'] in raw_search(q, k=10):
            hit_raw += 1
        if r['mid'] in fts_search(q, k=10):
            hit_sum += 1
    n = len(picks)
    return {'n': n, 'raw_hit': hit_raw, 'sum_hit': hit_sum,
            'raw_pct': hit_raw / max(1, n) * 100, 'sum_pct': hit_sum / max(1, n) * 100}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--selfcheck', action='store_true', help='只自检，不重建')
    ap.add_argument('--demo', type=int, default=0, help='跑跨语言对照，指定样本数')
    args = ap.parse_args()

    print('=' * 80)
    print('路4 原文索引 · 建库与自检')
    print('=' * 80)
    print(f'  SHUFANG_DATA_DIR = {os.environ.get("SHUFANG_DATA_DIR", "(未设，用默认)")}')
    print(f'  库文件           = {_db_path()}')

    if not args.selfcheck:
        print('\n[1] 全量重建…')
        st = raw_rebuild()
        print(f"    扫描 {st.get('scanned')} 条 · 写入 FTS {st.get('rows')} 行")
        print(f"    兜底链 src = {st.get('src')}")
        print(f"    语言分布   = {st.get('lang')}")
        print(f"    content_path 文件不存在 {st.get('missing_file')} · "
              f"反查不到（id 对不上）{st.get('orphan')} · "
              f"切不出 token {st.get('no_text')}")
    else:
        print('\n[1] 跳过重建（--selfcheck）')

    print('\n[2] 现状 raw_stats()')
    s = raw_stats()
    for k in ('fts_rows', 'meta_rows', 'mem_rows', 'coverage', 'src', 'lang', 'no_tok'):
        print(f'    {k:<10} = {s.get(k)}')

    print('\n[3] 自检')
    results = {}
    ok1, d1 = check_converged()
    results['1. idrule_converged（id 算法四处一致）'] = (ok1, d1)
    ok2, d2 = check_day_index()
    results['2. day_index_matches（批量=单条）'] = (ok2, d2)
    cov = s.get('coverage') or 0
    results['3. coverage ≥ 95%'] = (cov >= 0.95, [f'coverage={cov:.3f}'])
    results['4. no_tok == 0'] = ((s.get('no_tok') or 0) == 0, [f"no_tok={s.get('no_tok')}"])
    ok5, d5 = check_self_retrieval()
    results['5. self_retrieval ≥ 90%'] = (ok5, d5)

    for name, (ok, detail) in results.items():
        print(f"    {'PASS' if ok else 'FAIL'}  {name}")
        for d in (detail or [])[:6]:
            print(f'          {d}')

    if args.demo:
        print(f'\n[4] 跨语言对照（英文查询，样本 {args.demo}）')
        r = cross_lang_demo(args.demo)
        if 'error' in r:
            print(f"    跳过：{r['error']}")
        else:
            print(f"    原文索引 hit@10 = {r['raw_hit']}/{r['n']} = {r['raw_pct']:.1f}%")
            print(f"    摘要索引 hit@10 = {r['sum_hit']}/{r['n']} = {r['sum_pct']:.1f}%")
            print(f"    → 差 {r['raw_pct'] - r['sum_pct']:+.1f} 点"
                  f"（评测口径下实测 63.9% vs 11.9%）")

    allok = all(ok for ok, _ in results.values())
    print('\n' + '=' * 80)
    if allok:
        print('✅ 全绿：原文索引已建成且可验证')
    else:
        print('❌ 有自检未过 —— 先修，别拿这个索引去接检索管道')
    return 0 if allok else 1


if __name__ == '__main__':
    raise SystemExit(main())
