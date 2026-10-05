# -*- coding: utf-8 -*-
r"""交付质量审计（**纯机械，不调任何模型**）

## 为什么需要它

2026-10-03 的 50 题实测确认了两件事：
  ① **检索/交付不是瓶颈** —— 15 道错题里金标会话 **15/15** 全部已交付；
  ② **读者不是我们能控的** —— 同一道日期题在两次运行里答 14 天 / 45 天（参考 24），
     连"用真 question_date 当锚点"的上界都**净 0**。

⇒ 既然瓶颈不在我们这一段，**该盯的就是我们自己那一段**。而它可以用**机械判据**量，
不必再花子代理去判分（判分只能告诉我们"读者答得怎么样"，那会掩盖交付本身的问题）。

## 两条独立判据

1. **金标在场**：交付集里有没有**参考答案所属的会话**（= 召回口径，与 LEADERBOARD 可比）。
2. **关键值在场**：参考答案里的**数字/金额/百分比**有没有出现在交付文本里
   —— 这是"证据本身到没到"的更严一档。

⚠️ 上一版把第 2 条写成"取参考里最长的一段字母数字串"，**那是坏的**：
它会把 "None"、"and the" 这类噪声当关键值，结果一律 0/15，看起来像"证据没送到"。
现在改成**白名单式抽取**（先用正则捞数字/金额/百分比；一个都没有才退回专名）。

用法::
    python audit_delivery.py --tag aml50           # 一个臂
    python audit_delivery.py --tag aml50 --tag aml50_qd   # 多臂并列
"""
from __future__ import annotations

import argparse
import io
import json
import os
import re
import statistics as st
import sys

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
HERE = os.path.dirname(os.path.abspath(__file__))
LME = os.path.join(os.path.dirname(HERE), 'longmemeval')
os.environ.setdefault('SHUFANG_DATA_DIR', os.path.join(LME, 'store_turns'))
sys.path.insert(0, HERE)
os.chdir(HERE)
from services.B4_dual_gate_retrieval import retrieve                    # noqa: E402
from services.C2_aml_service.server import AmlService                   # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument('--tag', action='append', required=True)
ap.add_argument('--k', type=int, default=10)
ap.add_argument('--expand', type=int, default=2)
a = ap.parse_args()

qs = {q['question_id']: q for q in json.load(
    io.open(os.path.join(LME, 'data', 'longmemeval_s_cleaned.json'), encoding='utf-8'))}
spec = json.load(io.open(os.path.join(LME, 'cloud_sample50.json'), encoding='utf-8'))

# ---- 关键值抽取（**类型化 + 可判别性过滤**）----
#
# v1 的两个病灶（2026-10-04 全量 500 题实测）：
#   ① **裸数字**："14"、"17" 在历史里到处都有，判"没送到"没有意义 ⇒ 噪声误报。
#      修法：无单位的裸数字只保留 **≥3 位**（120/125/140/59.6 留下，14/17 丢掉）。
#   ② **缩写/全称**：参考写 "University of California, Los Angeles (UCLA)"，
#      抽取会拿到 'angeles'，而交付文本里写的是 **UCLA** ⇒ 误报"漏了"。
#      修法：**只在"抽到的关键值全部缺失"时才算真漏**（有任一个在场即不 flag）。
#   另加**可判别性**：某值在整段历史里出现 >5 次 ⇒ 它不是"窄事实"，不参与判定。
_UNITS = (r'(?:%|percent|dollars?|days?|weeks?|months?|years?|hours?|minutes?|'
          r'stars?|miles?|mpg|times?|people|engineers?|books?|songs?|bikes?|'
          r'plants?|items?|pieces?|pages?|episodes?|shirts?|festivals?|cuisines?)')
_NUM = re.compile(rf'(\$?\d[\d,]*(?:\.\d+)?)\s*({_UNITS})?\b', re.I)
_PROPER = re.compile(r'\b([A-Z][a-z]{3,})\b')
_STOP = {'none', 'and', 'the', 'that', 'this', 'with', 'your', 'you', 'would',
         'prefer', 'preferred', 'information', 'available'}


def key_values(ref: str) -> list:
    """抽出"应当在交付文本里能核到"的关键值，返回 `[(value, kind)]`。

    kind ∈ money / percent / duration / count / number / proper
    """
    out, seen = [], set()
    for m in _NUM.finditer(ref or ''):
        raw, unit = m.group(1), (m.group(2) or '')
        digits = re.sub(r'[^\d]', '', raw)
        u = unit.lower().rstrip('s') if unit else ''
        if raw.startswith('$'):
            kind = 'money'
        elif unit and unit.lower() in ('%', 'percent'):
            kind = 'percent'
        elif u in ('day', 'week', 'month', 'year', 'hour', 'minute'):
            kind = 'duration'
        elif u:
            kind = 'count'
        else:
            kind = 'number'
        # ★ 值必须**带单位**（'14 days' 而不是 '14'）——
        #   否则 `hist.count('14')` 会命中日期里的 "2023-03-14"，
        #   把"两日期之差"这种**计算型**答案误判成"历史里有、却没送到"。
        v = (raw + ' ' + unit).strip().lower() if unit else raw.lower()
        if kind == 'number' and len(digits) < 3:      # ① 裸数字只留 ≥3 位
            continue
        if v not in seen:
            seen.add(v)
            out.append((v, kind))
    if not out:
        for m in _PROPER.finditer(ref or ''):
            w = m.group(1)
            if w.lower() not in _STOP and w not in seen:
                seen.add(w)
                out.append((w.lower(), 'proper'))
    return out[:5]


def absent_in(text: str, hist: str, kvs: list, max_hist_hits: int = 5) -> tuple:
    """哪些关键值"该在交付里却没有"。**只在全部关键值都缺时才算真漏**（②）。

    返回 `(missing, considered)`；`missing` 为空 = 不算漏。
    """
    considered = [(v, k) for v, k in kvs if hist.count(v) <= max_hist_hits]
    if not considered:
        return [], []
    missing = [v for v, _ in considered if v not in text]
    if len(missing) < len(considered):
        return [], considered
    return missing, considered


def haystack_text(q: dict) -> str:
    """整段 haystack 的原文（用来区分"该字面出现却没送到" vs "本来就要算"）。

    ★ 这是本审计最要紧的一次细分（2026-10-03 加）：
      第一版把"参考答案的关键值不在交付文本里"一律记成"交付没到位"，
      结果 8 题里 7 题是**计算型答案**（`24 days` 是两个日期之差、`$300` 是两笔之和、
      `59.6` 是若干年龄的平均）—— 那种值**在整段历史里都不会字面出现**，
      把它算作"我们的缺口"是误判，会让人去修修不好的东西。

      判据改成：
        * 关键值**在整段 haystack 里也没有** ⇒ **计算型**，与交付无关（单独列出，不入分母）
        * 关键值**在 haystack 里有、却没进交付集** ⇒ **真·交付漏了**（这才是要修的）
    """
    from services.B3_header_engine import phase1_filter
    rows = phase1_filter(session_ids=q.get('haystack_session_ids') or [], limit=100000)
    return ' '.join((r['summary'] or '') for r in rows).lower()


def audit(tag: str) -> dict:
    rows = json.load(io.open(os.path.join(HERE, f'hyp_{tag}.json'), encoding='utf-8'))
    n_gold = n_lit_ok = n_lit = n_calc = n_miss = n_hits = 0
    ctxs, items_n = [], []
    calc, miss = [], []
    for r in rows:
        qid = r['question_id']
        q = qs[qid]
        got = retrieve(query_text=q['question'],
                       session_ids=q.get('haystack_session_ids') or [qid],
                       max_items=a.k, final_topk=a.k, max_tokens=10 ** 9,
                       apply_heat=False, rerank_enabled=True, route_fuse=True,
                       rerank_doc='raw', expand_window=a.expand,
                       expand_budget_chars=24000)
        items = got.get('items') or []
        payload = [AmlService._to_payload(it) for it in items]
        gold = set(q.get('answer_session_ids') or [])
        text = ' '.join((d.get('content') or '') for d in payload).lower()
        nh = sum(1 for it in items if not it.get('_expanded'))
        hist = haystack_text(q)
        kvs = key_values(str(q.get('answer') or ''))
        missing, considered = absent_in(text, hist, kvs)
        has_gold = any((d.get('session_id') or '') in gold for d in payload)
        n_gold += has_gold
        if not missing:
            # 要么关键值都在场，要么本来就抽不出可判别的值
            n_lit += 1
            n_lit_ok += 1
        elif any(v in hist for v in missing):
            n_lit += 1
            n_miss += 1
            miss.append((qid, missing[:3],
                         [(v, hist.count(v)) for v in missing[:3]]))
        else:
            n_calc += 1
            calc.append((qid, missing[:3]))
        n_hits += nh
        ctxs.append(len(text))
        items_n.append(len(payload))
    n = len(rows)
    return {'tag': tag, 'n': n, 'gold': n_gold, 'lit_ok': n_lit_ok, 'lit': n_lit,
            'calc': n_calc, 'miss': miss, 'calc_list': calc,
            'ctx_med': st.median(ctxs), 'items_med': st.median(items_n),
            'hits_med': n_hits / n}


print(f'{"臂":<12}{"金标在场":>10}{"字面关键值":>12}{"计算型":>8}{"真漏":>7}'
      f'{"上下文中位":>12}{"条数中位":>10}')
res = []
for tag in a.tag:
    r = audit(tag)
    res.append(r)
    print(f'{tag:<12}{r["gold"]:>4}/{r["n"]:<5}'
          f'{r["lit_ok"]:>7}/{r["lit"]:<4}{r["calc"]:>8}{len(r["miss"]):>7}'
          f'{r["ctx_med"]:>12,.0f}{r["items_med"]:>10.0f}')
print()
for r in res:
    print(f'{r["tag"]}：')
    print(f'  ★ 真·交付漏了：{len(r["miss"])} 题 —— **可逐条核对**（值, 在历史里出现次数）')
    for qid, vals, counts in r['miss'][:12]:
        print(f'      {qid:<22} 缺 {vals}  历史频次 {counts}')
    if len(r['miss']) > 12:
        print(f'      … 另 {len(r["miss"]) - 12} 题')
    print(f'  计算型（历史里也没有 ⇒ 与交付无关）：{r["calc"]} 题 {r["calc_list"][:6]}')
