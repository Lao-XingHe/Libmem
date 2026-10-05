# -*- coding: utf-8 -*-
r"""比赛专用记忆插件 · 本地 50 题端到端（检索 + 交付 + 9B 作答）

## 为什么用**平台官方的固定提示词**，而不是我们自己的

AML 的回答提示词是**平台写死**的（`data/longmemeval-s/pipeline.py` 里的
`OPEN_ENDED_ANSWER_TEMPLATE`，本文件逐字抄来）。我们唯一能控制的是
**`<memories>` 里塞什么**。所以本地评测必须用同一份模板 ——
否则测的是"我们的提示词写得好不好"，不是"我们的记忆交付好不好"。

## 两个关键口径（与线上一致）

1. **不注入任何我们的话术**：不加重写指令、不加"未找到"标记、不加拒答规则。
   （参赛版已删 B6 `clarify`，因为平台提示词写着 "Do not refuse just because..."）
2. **每条记忆自带 `[YYYY-MM-DD]` 时间戳**：平台提示词要靠它换算相对时间，
   而官方判词又禁止"相对↔绝对"互换 ⇒ 时间戳与**原话**都必须给。

## 数据来源

复用 `longmemeval/store_turns`（轮级库，`session_id` 已填好），
按**本题的 haystack** 限定 —— 这正是 AML 里 `user_id` 起到的作用（隔离边界）。
**不写库、不改库**（`user_ids=None` + `session_ids=[haystack]`，显式传参不走 config）。

用法::
    python eval50.py --ids ../longmemeval/cloud_sample50.json --tag aml50
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
import urllib.request

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
LME = os.path.join(ROOT, 'longmemeval')
STORE = os.path.join(LME, 'store_turns')
BASE_URL = os.environ.get('AML_LLM_BASE', 'http://127.0.0.1:8088/v1')
MODEL = os.environ.get('AML_LLM_MODEL', 'Qwen3.5-9B-Q4_K_M')

# ★ 平台官方模板（逐字抄自 AML 仓库 data/longmemeval-s/pipeline.py）
PLATFORM_TEMPLATE = """You are asked to answer a question based on your memories of a conversation.

<instructions>
1. Use only the provided memories. Prefer the memory that answers the question most directly.
2. Your memories are episodic raw observations. Reason about what they imply. Do not refuse just because the answer is not stated verbatim.
3. The question may contain typos. Match it to the most relevant memory even if the wording differs.
4. When multiple answers are possible, list all supported answers, not just the first.
5. For counts or time intervals, enumerate carefully before answering.
6. Preserve specific names, titles, places, and labels from the memories. Use "Rob" not "a colleague", "Sweden" not "home country".
7. Convert relative times like "yesterday", "last month", and "last year" into dates, months, or years when the memory timestamp makes it clear. Keep week-based expressions relative.
8. If memories conflict, prefer the most recent supported memory.
9. For list questions, include all required items and no extras.
10. Keep the final answer minimal. Do not add explanation, background, or extra dates unless needed for correctness.
</instructions>

<memories>
Memories for user {speaker_1}:

{speaker_1_memories}

Memories for user {speaker_2}:

{speaker_2_memories}
</memories>

Question: {question}
Answer with the shortest correct phrase or sentence. No preamble, no fluff:"""

ap = argparse.ArgumentParser()
ap.add_argument('--ids', default=os.path.join(LME, 'cloud_sample50.json'))
ap.add_argument('--tag', default='aml50')
ap.add_argument('--k', type=int, default=10)
ap.add_argument('--expand', type=int, default=2)
ap.add_argument('--limit', type=int, default=0)
ap.add_argument('--refdate', default='off', choices=('off', 'proxy', 'question'),
                help='证据块首行的参考日期：\n'
                     '  off      = 不注入（基线）\n'
                     '  proxy    = 检索范围内 MAX(source_date)（"你记得的最近一件事"）\n'
                     '             ⚠️ 实测它是**坏代理**：LongMemEval 的 haystack 里有\n'
                     '             比 question_date 晚 ~9 个月的会话（2023-05-30 的题，\n'
                     '             MAX 给到 2024-02-20）⇒ 会故意答错，只作对照\n'
                     '  question = 数据集的 `question_date`（**上界**：比赛拿不到，\n'
                     '             但用它可判断"正确的锚点到底值不值"）')
a = ap.parse_args()

os.environ['SHUFANG_DATA_DIR'] = STORE
sys.path.insert(0, HERE)
os.chdir(HERE)
from services.B4_dual_gate_retrieval import retrieve                     # noqa: E402
from services.C2_aml_service.server import AmlService                     # noqa: E402

qs = {q['question_id']: q for q in json.load(
    io.open(os.path.join(LME, 'data', 'longmemeval_s_cleaned.json'), encoding='utf-8'))}
spec = json.load(io.open(os.path.abspath(a.ids), encoding='utf-8'))
if a.limit:
    spec = spec[:a.limit]
print(f'题数 {len(spec)} · 库 {STORE} · k={a.k} · expand=±{a.expand}', flush=True)


def ask(prompt: str, tries: int = 3) -> str:
    """调 9B。**容忍单次失败** —— 500 不该让整卷作废（2026-10-03 实际踩过：
    router 的 models-max=1 把 9B 换出加载 4B，于是 50 题跑完前面全丢）。"""
    body = json.dumps({'model': MODEL, 'messages': [{'role': 'user', 'content': prompt}],
                       'temperature': 0.1, 'max_tokens': 512,
                       'chat_template_kwargs': {'enable_thinking': False}}).encode('utf-8')
    last = ''
    for attempt in range(tries):
        try:
            req = urllib.request.Request(BASE_URL + '/chat/completions', data=body,
                                         headers={'Content-Type': 'application/json'})
            with urllib.request.urlopen(req, timeout=600) as resp:
                return (json.loads(resp.read().decode('utf-8'))['choices'][0]
                        ['message'].get('content') or '').strip()
        except Exception as error:                    # noqa: BLE001
            last = f'{type(error).__name__}: {error}'
            print(f'      retry {attempt + 1}/{tries}: {last[:90]}', flush=True)
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(last)


# ★ 增量落盘（本仓纪律：跑一半也得保住已完成的）
#   第一版只在**全部跑完**才写文件 ⇒ 第 37 题 500 时前 36 题的答案全丢。
#   现在每题 append 一行 JSONL，并可**续跑**（已完成的 question_id 自动跳过）。
OUT_JSONL = os.path.join(HERE, f'hyp_{a.tag}.jsonl')
done = {}
if os.path.exists(OUT_JSONL):
    for line in io.open(OUT_JSONL, encoding='utf-8'):
        if line.strip():
            rec = json.loads(line)
            done[rec['question_id']] = rec
    print(f'续跑：已有 {len(done)} 条', flush=True)

rows, t0 = [], time.time()
with io.open(OUT_JSONL, 'a', encoding='utf-8') as sink:
    for i, s in enumerate(spec, 1):
        if s['question_id'] in done:
            rows.append(done[s['question_id']])
            continue
        q = qs[s['question_id']]
        got = retrieve(
            query_text=q['question'],
            session_ids=q.get('haystack_session_ids') or [s['question_id']],   # = 隔离边界
            max_items=a.k, final_topk=a.k, max_tokens=10 ** 9,
            apply_heat=False,
            rerank_enabled=True, route_fuse=True, rerank_doc='raw',
            expand_window=a.expand, expand_budget_chars=24000)
        items = got.get('items') or []
        payload = [AmlService._to_payload(it) for it in items]
        # ★ 参考日期：检索范围内**最新的记忆日期**（= "你记得的最近一件事"）
        ref_line = ''
        if a.refdate != 'off':
            try:
                if a.refdate == 'question':
                    # 数据集格式：'2023/05/30 (Tue) 20:24' → 取 YYYY-MM-DD
                    ref = str(q.get('question_date') or '')[:10].replace('/', '-')
                else:
                    from services.B3_header_engine import sqlite_store as SS
                    ref = SS.max_source_date(
                        session_ids=q.get('haystack_session_ids') or [s['question_id']])
                if ref:
                    ref_line = f'[Reference date (today): {ref}]'
            except Exception as error:                # noqa: BLE001
                print(f'      refdate 失败: {type(error).__name__}: {error}', flush=True)
        # 按 role 分到两个 speaker 槽（平台模板的两个槽位）
        s1 = '\n'.join(f'- {d["content"]}' for d in payload
                       if (d.get('role') or 'user') != 'assistant')
        s2 = '\n'.join(f'- {d["content"]}' for d in payload
                       if (d.get('role') or '') == 'assistant')
        if ref_line:
            s1 = ref_line + '\n' + s1
        prompt = (PLATFORM_TEMPLATE.replace('{speaker_1}', 'speaker 1')
                  .replace('{speaker_2}', 'speaker 2')
                  .replace('{speaker_1_memories}', s1 or '(none)')
                  .replace('{speaker_2_memories}', s2 or '(none)')
                  .replace('{question}', q['question']))
        try:
            ans, err = ask(prompt), ''
        except Exception as error:                    # noqa: BLE001
            ans, err = '', f'{type(error).__name__}: {error}'
        rec = {'question_id': s['question_id'], 'question_type': q['question_type'],
               'stratum': s.get('stratum', ''), 'question': q['question'],
               'answer': str(q.get('answer')), 'hypothesis': ans, 'error': err,
               'n_items': len(payload), 'n_expanded': got.get('n_expanded'),
               'ctx_chars': len(s1) + len(s2), 'prompt_chars': len(prompt)}
        rows.append(rec)
        sink.write(json.dumps(rec, ensure_ascii=False) + '\n')
        sink.flush()                                  # ← 立刻落盘，中途挂了也保住
        print(f'  {i}/{len(spec)} {s["question_id"]:<22} items={len(payload)} '
              f'ctx={len(s1) + len(s2):>6} 答={ans[:58]!r}', flush=True)

out = os.path.join(HERE, f'hyp_{a.tag}.json')
json.dump(rows, io.open(out, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
json.dump({'tag': a.tag, 'k': a.k, 'expand': a.expand, 'model': MODEL,
           'prompt': 'AML 官方 longmemeval-s 模板（逐字）', 'store': STORE,
           'routes_enabled': True, 'rerank_doc': 'raw', 'apply_heat': False,
           'speaker_split': 'role=user→speaker1, role=assistant→speaker2'},
          io.open(os.path.join(HERE, f'hyp_{a.tag}.meta.json'), 'w', encoding='utf-8'),
          ensure_ascii=False, indent=1)
import statistics as st
print(f'\n✅ {out}')
print(f'   上下文中位 {st.median([r["ctx_chars"] for r in rows]):,.0f} 字符 · '
      f'交付条数中位 {st.median([r["n_items"] for r in rows]):.0f} · '
      f'拒答 {sum(1 for r in rows if "no information" in r["hypothesis"].lower())}/{len(rows)}')
print(f'   总耗时 {(time.time() - t0) / 60:.1f} 分钟')
