# -*- coding: utf-8 -*-
r"""LibMem-AML · 容量 / 并发曲线（**填申请表单用**）

## 为什么必须实测

AML 的 api-guide 把 `429` 归因于「**参测接口容量不足**，或平台评测配额尚未恢复」，
而申请时就要填「**容量和运行限制**」。填错的后果是特定的：
**报高了 ⇒ 平台并发打过来，我们扛不住 ⇒ 429/超时 ⇒ 有界重试 ⇒ 仍然失败。**

我们的服务是 `ThreadingHTTPServer`（能并发），但**检索会串行在本地两个模型服务上**
（嵌入 18099 / 重排 18098）⇒ 并发下的延迟增长必须量出来，不能猜。

## 口径

* 测 `/search`（热路径）与 `/add`（写入路径）两类
* 每个并发档跑 `--n` 次请求，报 p50 / p95 / 成功率
* **数据**：先灌 1 个用户 300 条记忆（贴近平台分片后的规模：20 条/片 × 多片）

用法::  python bench_concurrency.py
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import io
import json
import os
import statistics as st
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
HERE = os.path.dirname(os.path.abspath(__file__))
PORT = int(os.environ.get('BENCH_PORT', '18092'))
BASE = f'http://127.0.0.1:{PORT}'
KEY = 'bench-key'

ap = argparse.ArgumentParser()
ap.add_argument('--levels', default='1,2,4,8')
ap.add_argument('--n', type=int, default=12, help='每个并发档的请求数')
a = ap.parse_args()


def call(path, body, timeout=120):
    req = urllib.request.Request(BASE + path, method='POST',
                                 data=json.dumps(body, ensure_ascii=False).encode(),
                                 headers={'Content-Type': 'application/json', 'X-Api-Key': KEY})
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            json.loads(r.read().decode())
        return (time.perf_counter() - t0) * 1000, True
    except Exception:                             # noqa: BLE001
        return (time.perf_counter() - t0) * 1000, False


def run(level, kind, n):
    """并发 level 打 n 次。kind ∈ {'search','add'}"""
    if kind == 'search':
        bodies = [{'query': f'what did I note about topic {i}', 'user_id': 'bench',
                   'top_k': 30} for i in range(n)]
        path = '/search'
    else:
        bodies = [{'request_id': f'bench:{i}', 'user_id': 'bench', 'session_id': f's{i}',
                   'messages': [{'role': 'user', 'content': f'bench fact {i}',
                                 'timestamp': 1682000000000 + i}]} for i in range(n)]
        path = '/add'
    with cf.ThreadPoolExecutor(max_workers=level) as ex:
        res = list(ex.map(lambda b: call(path, b), bodies))
    lat = [r[0] for r in res]
    ok = sum(1 for r in res if r[1])
    return {'level': level, 'kind': kind, 'n': n, 'ok': ok,
            'p50': st.median(lat),
            'p95': sorted(lat)[max(0, int(len(lat) * 0.95) - 1)],
            'max': max(lat)}


data_dir = tempfile.mkdtemp(prefix='libmem_bench_')
log = os.path.join(data_dir, 'srv.log')
env = dict(os.environ, SHUFANG_DATA_DIR=data_dir, AML_API_KEY=KEY,
           AML_PORT=str(PORT), AML_HOST='127.0.0.1', PYTHONIOENCODING='utf-8')
f = io.open(log, 'a', encoding='utf-8')
proc = subprocess.Popen([sys.executable, '-m', 'services.C2_aml_service.server',
                         '--port', str(PORT), '--data-dir', data_dir],
                        cwd=HERE, env=env, stdout=f, stderr=subprocess.STDOUT)
try:
    for _ in range(60):
        time.sleep(0.5)
        try:
            with urllib.request.urlopen(BASE + '/health', timeout=2) as r:
                if r.status == 200:
                    break
        except Exception:                          # noqa: BLE001
            if proc.poll() is not None:
                raise SystemExit(f'服务未起来，见 {log}')
    # 灌 300 条（贴近平台分片规模）
    msgs = [{'role': 'user' if i % 2 == 0 else 'assistant',
             'content': f'Memory {i}: the user mentioned topic {i} and item {i * 7}.',
             'timestamp': 1682000000000 + i * 60000} for i in range(300)]
    call('/add', {'request_id': 'seed', 'user_id': 'bench', 'session_id': 'seed', 'messages': msgs})
    print(f'服务已起 {BASE} · 预热完成（300 条）\n')
    rows = []
    for kind in ('search', 'add'):
        for lv in [int(x) for x in a.levels.split(',')]:
            r = run(lv, kind, a.n)
            rows.append(r)
            print(f'  {kind:<7} 并发 {lv:<2} → p50 {r["p50"]:>7.0f}ms · p95 {r["p95"]:>7.0f}ms · '
                  f'max {r["max"]:>7.0f}ms · 成功 {r["ok"]}/{r["n"]}', flush=True)
    out = os.path.join(HERE, '_bench_concurrency.json')
    json.dump({'data_dir': data_dir, 'rows': rows}, io.open(out, 'w', encoding='utf-8'),
              ensure_ascii=False, indent=1)
    print(f'\n明细 → {out}')
    s1 = [r for r in rows if r['kind'] == 'search' and r['level'] == 1][0]
    print(f'★ 单并发基线：search p50 {s1["p50"]:.0f}ms / p95 {s1["p95"]:.0f}ms')
    worst = max((r for r in rows if r['kind'] == 'search'), key=lambda r: r['p95'])
    print(f'★ 最高并发档：{worst["level"]} → p95 {worst["p95"]:.0f}ms，成功率 '
          f'{worst["ok"]}/{worst["n"]}')
    print('★ 填表建议：把「最高实测并发档 + 同级 p95」写进"容量和运行限制"；'
          '留一档余量（超出即可能被平台判容量不足 → 429）。')
finally:
    proc.terminate()
    try:
        proc.wait(timeout=20)
    except Exception:                              # noqa: BLE001
        proc.kill()
