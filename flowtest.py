# -*- coding: utf-8 -*-
r"""LibMem-AML · 端到端流程测试（**真起服务进程**，含重启持久化）

## 为什么自检不够、必须再做这一遍

`selftest.py` 是在**同进程内**把服务对象起来测的 —— 它证明不了两件事：

  ① **跨进程持久化**：`/add` 返回 200 之后，**杀掉进程再起**，记忆还在不在？
     这正是"**记忆入不了库**"的症状：进程内看着一切正常，重启即空。
     （成因会是：没 commit / 写进了内存态 / data-dir 解析到了别处 / WAL 没落盘。）
  ② **真实 HTTP 栈**：`BaseHTTPRequestHandler` 的 body 解析、鉴权、错误码，
     在同进程直调时都被绕过了。

所以本测试**用 subprocess 起真服务**，并在中途**杀进程重启**。

用法::  python flowtest.py
"""
from __future__ import annotations

import io
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
HERE = os.path.dirname(os.path.abspath(__file__))
PORT = int(os.environ.get('FLOWTEST_PORT', '18091'))
BASE = f'http://127.0.0.1:{PORT}'
KEY = 'flowtest-key'
PASS, FAIL = [], []


def chk(name, cond, extra=''):
    (PASS if cond else FAIL).append(name)
    print(f'  [{"OK  " if cond else "FAIL"}] {name}{("  " + extra) if extra else ""}', flush=True)


def call(path, body=None, method='POST', key=KEY, timeout=600):
    data = json.dumps(body, ensure_ascii=False).encode('utf-8') if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={'Content-Type': 'application/json',
                                          **({'X-Api-Key': key} if key else {})})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode('utf-8') or '{}')


def start(data_dir, log):
    env = dict(os.environ, SHUFANG_DATA_DIR=data_dir, AML_API_KEY=KEY,
               AML_PORT=str(PORT), AML_HOST='127.0.0.1', PYTHONIOENCODING='utf-8')
    f = io.open(log, 'a', encoding='utf-8')
    p = subprocess.Popen([sys.executable, '-m', 'services.C2_aml_service.server',
                          '--port', str(PORT), '--data-dir', data_dir],
                         cwd=HERE, env=env, stdout=f, stderr=subprocess.STDOUT)
    for _ in range(60):
        time.sleep(0.5)
        try:
            with urllib.request.urlopen(BASE + '/health', timeout=2) as r:
                if r.status == 200:
                    return p
        except Exception:            # noqa: BLE001
            if p.poll() is not None:
                raise SystemExit(f'服务启动即退出，见 {log}')
    raise SystemExit(f'服务 30 秒未就绪，见 {log}')


data_dir = tempfile.mkdtemp(prefix='libmem_flow_')
log = os.path.join(data_dir, 'server.log')
print(f'data_dir={data_dir}\n')
proc = None
try:
    print('① 启动真服务进程（子进程）')
    proc = start(data_dir, log)
    code, health = call('/health', method='GET', key=None)
    chk('/health 免鉴权 200', code == 200 and health.get('status') == 'ok')
    chk('/health 报版本名', 'LibMem-AML' in str(health.get('version')), str(health.get('version')))
    chk('/health 报模型健康', health.get('embed_ok') is True and health.get('rerank_ok') is True,
        f"embed={health.get('embed_ok')} rerank={health.get('rerank_ok')}")

    print('\n② Add 一条**真实形状**的会话（带 Unix 毫秒时间戳）')
    msgs = [
        {'role': 'user', 'content': 'My sister gave me a stand mixer for my birthday.',
         'timestamp': 1682000000000},
        {'role': 'assistant', 'content': 'That is a lovely gift from your sister.',
         'timestamp': 1682000060000},
        {'role': 'user', 'content': 'Project Thunderbird is my private diary topic.',
         'timestamp': 1682000120000},
        {'role': 'user', 'content': 'We hiked the Cedar Creek trail for 3 hours on Sunday.',
         'timestamp': 1682000180000},
    ]
    code, r = call('/add', {'request_id': 'flow:1', 'user_id': 'alice',
                            'session_id': 'sess-A', 'messages': msgs})
    chk('POST /add → 200 且回声正确',
        code == 200 and r.get('added') == 4 and r.get('request_id') == 'flow:1',
        f"status={code} added={r.get('added')}")

    print('\n③ 大批量 Add（模拟平台一次推整段历史）')
    big = [{'role': 'user' if i % 2 == 0 else 'assistant',
            'content': f'Turn {i}: the workshop covered topic {i} and we took notes.',
            'timestamp': 1682100000000 + i * 60000} for i in range(200)]
    t0 = time.time()
    code, rb = call('/add', {'request_id': 'flow:big', 'user_id': 'bob',
                             'session_id': 'sess-B', 'messages': big})
    chk('200 条一次 Add → 200', code == 200 and rb.get('added') == 200,
        f"status={code} added={rb.get('added')} 用时 {time.time() - t0:.1f}s")

    print('\n④ Search（真实 HTTP）')
    code, s1 = call('/search', {'query': 'What did my sister give me?',
                                'user_id': 'alice', 'top_k': 10})
    d1 = s1.get('data') or []
    chk('POST /search → 200 且非空', code == 200 and len(d1) > 0, f'n={len(d1)}')
    chk('结果含答案证据', any('stand mixer' in (x.get('content') or '') for x in d1))
    chk('★ 隔离：alice 拿不到 bob 的', not any((x.get('user_id') or '') == 'bob' for x in d1))
    code, s2 = call('/search', {'query': 'Cedar Creek trail', 'user_id': 'alice', 'top_k': 5})
    chk('第二条查询也能命中（扩窗带邻居）',
        any('cedar' in (x.get('content') or '').lower() for x in (s2.get('data') or [])))

    print('\n⑤ ★ 关键：杀进程 → 重启 → 记忆还在不在（"入不了库"就是这个症状）')
    proc.terminate()
    proc.wait(timeout=30)
    chk('进程已退出', proc.poll() is not None)
    time.sleep(1)
    proc = start(data_dir, log)
    code, s3 = call('/search', {'query': 'What did my sister give me?',
                                'user_id': 'alice', 'top_k': 10})
    d3 = s3.get('data') or []
    chk('★ 重启后 alice 的记忆仍在', len(d3) > 0, f'n={len(d3)}')
    chk('★ 重启后内容不变', any('stand mixer' in (x.get('content') or '') for x in d3))
    code, s4 = call('/search', {'query': 'workshop topic', 'user_id': 'bob', 'top_k': 5})
    chk('★ 重启后 bob 的 200 条也在', len(s4.get('data') or []) > 0,
        f"n={len(s4.get('data') or [])}")

    print('\n⑥ 直接查库（不经服务）：行数与原文索引')
    db = os.path.join(data_dir, 'data', 'index', 'header.db')
    conn = sqlite3.connect(f'file:{db}?mode=ro', uri=True)
    n_all = conn.execute('SELECT COUNT(*) FROM memories').fetchone()[0]
    per_user = dict(conn.execute('SELECT user_id, COUNT(*) FROM memories GROUP BY user_id'))
    try:
        src = dict(conn.execute('SELECT src, COUNT(*) FROM memories_raw_meta GROUP BY src'))
    except Exception as e:                     # noqa: BLE001
        src = {'查不了': str(e)}
    conn.close()
    chk('库里总行数 = 204', n_all == 204, f'n={n_all} · per_user={per_user}')
    chk('★ 原文索引 src 全是 summary（路4 有数据）',
        src.get('summary', 0) == n_all and src.get('none', 0) in (0, None), f'src={src}')

    print('\n⑦ 错误路径（真实 HTTP 状态码）')
    code, _ = call('/search', {'query': 'x'})
    chk('缺 user_id → 400', code == 400, f'status={code}')
    code, _ = call('/add', {'user_id': 'u', 'messages': 'oops'})
    chk('messages 非数组 → 400', code == 400, f'status={code}')
    code, _ = call('/search', {'query': 'x', 'user_id': 'u'}, key='wrong')
    chk('错 key → 401', code == 401, f'status={code}')
    code, _ = call('/nope', {}, key=KEY)
    chk('未知路径 → 404', code == 404, f'status={code}')
finally:
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=20)
        except Exception:                      # noqa: BLE001
            proc.kill()

print('\n' + '=' * 68)
print(f'通过 {len(PASS)} · 失败 {len(FAIL)}')
for f in FAIL:
    print(f'  ✗ {f}')
print(f'data_dir（排障用）= {data_dir}')
print('=' * 68)
sys.exit(1 if FAIL else 0)
