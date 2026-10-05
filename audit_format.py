# -*- coding: utf-8 -*-
r"""格式兼容审计（对 AML 各数据集的**编排器口径**，纯机械）

## 为什么这件必须先做

榜首是**六个数据集聚合分**，而我们只针对 LongMemEval-S 调过。
另外几个数据集的编排器**读不同的键、拼不同的格式** —— 键名不匹配的后果是
**整条记忆被静默丢掉**（"检索到了但读者看不到"），而分数上只表现为"答错"。

三件事（都能在本地机械核，不需要它们的数据集）：

  ① **CL-Bench 口径**：它的 `format_selected_memories` 逐字是 `- [{created_at}] {text}`
     ⇒ 核我们返回的条目**是否有这两个键、渲染形状是否一致**。
     ⚠️ 已知风险：我们的 `text` **自带** `[YYYY-MM-DD] ` 前缀 ⇒ 会渲染成
     `- [2023-04-20T14:13:20+00:00] [2023-04-20] user: …`（**双时间戳**）。
  ② **LongMemEval / LoCoMo 口径**：按 role 分进 `speaker_1_memories` / `speaker_2_memories`
     两个槽 ⇒ 核**每条都能落进某个槽**（role 缺失的条目会被静默丢掉）。
  ③ **排序题的序列完整性**（BEAM `event_ordering` / ScriptMem·CL-Bench 的 `ordering`）：
     扩窗的全部价值就在这里。量"交付集里**同一会话的连续段长度**"分布 ——
     连续段越长，读者越能排出正确顺序。

用法::  python audit_format.py --tag aml50
"""
from __future__ import annotations

import argparse
import io
import json
import os
import statistics as st
import sys
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
HERE = os.path.dirname(os.path.abspath(__file__))
LME = os.path.join(os.path.dirname(HERE), 'longmemeval')
os.environ.setdefault('SHUFANG_DATA_DIR', os.path.join(LME, 'store_turns'))
sys.path.insert(0, HERE)
os.chdir(HERE)
from services.B4_dual_gate_retrieval import retrieve                    # noqa: E402
from services.C2_aml_service.server import AmlService                   # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument('--tag', default='aml50')
ap.add_argument('--k', type=int, default=10)
ap.add_argument('--expand', type=int, default=2)
ap.add_argument('--limit', type=int, default=0)
a = ap.parse_args()

qs = {q['question_id']: q for q in json.load(
    io.open(os.path.join(LME, 'data', 'longmemeval_s_cleaned.json'), encoding='utf-8'))}
rows = json.load(io.open(os.path.join(HERE, f'hyp_{a.tag}.json'), encoding='utf-8'))
if a.limit:
    rows = rows[:a.limit]

n_items = n_missing_key = n_no_slot = n_double_date = 0
seg_lens, per_type_seg = [], defaultdict(list)
examples = {'clbench': None, 'double': None}

for r in rows:
    q = qs[r['question_id']]
    got = retrieve(query_text=q['question'],
                   session_ids=q.get('haystack_session_ids') or [r['question_id']],
                   max_items=a.k, final_topk=a.k, max_tokens=10 ** 9,
                   apply_heat=False, rerank_enabled=True, route_fuse=True,
                   rerank_doc='raw', expand_window=a.expand, expand_budget_chars=24000)
    payload = [AmlService._to_payload(it) for it in (got.get('items') or [])]
    n_items += len(payload)
    for p in payload:
        # ① CL-Bench 口径：必须同时有 created_at 与 text
        if not p.get('created_at') or not p.get('text'):
            n_missing_key += 1
        line = f'- [{p.get("created_at")}] {p.get("text")}'
        if examples['clbench'] is None:
            examples['clbench'] = line[:120]
        # 已知风险：text 自带 [日期] 前缀 ⇒ 双时间戳
        if (p.get('text') or '').startswith('[') and (p.get('source_date') or ''):
            n_double_date += 1
            if examples['double'] is None:
                examples['double'] = line[:140]
        # ② 两槽口径：role 缺失 ⇒ 落不进任何槽（会被静默丢掉）
        if not p.get('role'):
            n_no_slot += 1
    # ③ 序列完整性：同一会话内**按交付顺序**的最长连续段（只对有日期可排的题）
    by_sess = defaultdict(list)
    for p in payload:
        by_sess[p.get('session_id')].append(p)
    for sess, items in by_sess.items():
        # 同一会话里能按时间排出顺序的最大条数（这里用"该会话被交付的条数"近似）
        seg = len(items)
        seg_lens.append(seg)
        per_type_seg[r['question_type']].append(seg)

print(f'臂 {a.tag} · 题 {len(rows)} · 交付条目共 {n_items}（中位 {st.median([len(AmlService._to_payload(i)) for i in [{}]]) if False else ""}）')
print()
print('① CL-Bench 口径（`- [{created_at}] {text}`）')
print(f'   缺键（created_at/text 任一为空）: {n_missing_key}')
print(f'   双时间戳（text 自带 [日期] 前缀）: {n_double_date} / {n_items}')
print(f'   渲染样例: {examples["clbench"]}')
print(f'   ⚠️ 双时间戳样例: {examples["double"]}')
print()
print('② 两槽口径（speaker_1_memories / speaker_2_memories）')
print(f'   role 缺失（会落不进任何槽、被静默丢掉）: {n_no_slot} / {n_items}')
print()
print('③ 序列完整性（排序题靠它）：同会话被交付的条数分布')
if seg_lens:
    print(f'   中位 {st.median(seg_lens):.0f} · 最大 {max(seg_lens)} · '
          f'≥3 条的会话占比 {sum(1 for s in seg_lens if s >= 3) / len(seg_lens) * 100:.0f}%')
print(f'   {"题型":<28}{"会话数":>6}{"条数中位":>9}{"≥3条占比":>10}')
for t in sorted(per_type_seg):
    v = per_type_seg[t]
    print(f'   {t:<28}{len(v):>6}{st.median(v):>9.0f}'
          f'{sum(1 for s in v if s >= 3) / len(v) * 100:>9.0f}%')
