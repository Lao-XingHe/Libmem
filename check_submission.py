# -*- coding: utf-8 -*-
r"""提交前合规自检（AML 官方「明确不公开」清单 → 可执行判据）

## 官方原文（README_CN §明确不公开的内容）

> 为保护评测完整性与参评者隐私，本仓库不会包含：
> - **数据集原文、保留题目、标准答案、评分量表或私有标注**；
> - 参评系统提交内容、检索记忆、模型输出、日志或运行产物；
> - 服务凭据、模型密钥、参评 API Key 或部署密钥；
>
> **请勿在 Issue 或 Pull Request 中提交上述内容。**

⇒ 这条对「**开源方法**」赛道是硬约束：我们要**公开仓库**，但仓库里**
**不能有评测数据/参考答案/凭据**。

## 为什么用脚本而不是"记得删"

这类违规的特征是"**本地跑得好好的、推上去就违规**"，而且通常在提交那一刻才想起来。
脚本化的好处是它可以进 CI / 进构建期，**不需要人记**。

用法::
    python check_submission.py            # 扫本目录
    python check_submission.py --strict   # 有命中就退出码 1（给 CI/构建期用）
"""
from __future__ import annotations

import argparse
import io
import json
import os
import re
import sys

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
HERE = os.path.dirname(os.path.abspath(__file__))

# 排除：源码本体 / 缓存 / 虚拟环境
SKIP_DIRS = {'__pycache__', '.git', '.venv', 'node_modules', 'data'}
SKIP_SUFFIX = ('.pyc',)

# ① 评测产物（含参考答案 / 判定 / 题目原文）
ARTIFACT_PATTERNS = [
    re.compile(r'^hyp_.*\.jsonl?$'),          # 我们的答案文件（带 answer 字段）
    re.compile(r'^hyp_.*\.meta\.json$'),
    re.compile(r'^judge_batches'),            # 判定批次（含参考答案）
    re.compile(r'^.*\.judged\.json$'),
    re.compile(r'^cloud_sample\d+\.json$'),   # 抽样题目清单
    re.compile(r'^hyp_full500\.json$'),
]
# ② 凭据 / 密钥
SECRET_PATTERNS = [
    re.compile(r'sk-[A-Za-z0-9]{16,}'),
    re.compile(r'AML_API_KEY\s*=\s*["\'][^"\']{8,}'),
    re.compile(r'eyJ[A-Za-z0-9_\-]{20,}\.[A-Za-z0-9_\-]{20,}\.'),   # JWT
]
# ③ 数据本体（大文件）
DATA_SUFFIX = ('.gguf', '.db', '.sqlite', '.db-wal', '.db-shm')
DATA_SIZE_LIMIT = 5 * 1024 * 1024          # 单文件 >5MB 视为数据/模型，不该进仓库


def scan() -> dict:
    hits = {'artifacts': [], 'secrets': [], 'big': []}
    for root, dirs, files in os.walk(HERE):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for fn in files:
            if fn.endswith(SKIP_SUFFIX):
                continue
            rel = os.path.relpath(os.path.join(root, fn), HERE).replace(os.sep, '/')
            if any(p.search(fn) for p in ARTIFACT_PATTERNS):
                hits['artifacts'].append(rel)
            path = os.path.join(root, fn)
            size = os.path.getsize(path)
            if fn.endswith(DATA_SUFFIX) or size > DATA_SIZE_LIMIT:
                hits['big'].append((rel, size))
            # 只扫文本，且跳过本文件（它自带那些正则字面量）
            if fn == os.path.basename(__file__) or size > 2 * 1024 * 1024:
                continue
            try:
                text = io.open(path, encoding='utf-8', errors='ignore').read()
            except OSError:
                continue
            for p in SECRET_PATTERNS:
                for m in p.finditer(text):
                    hits['secrets'].append((rel, m.group(0)[:24] + '…'))
    return hits


ap = argparse.ArgumentParser()
ap.add_argument('--strict', action='store_true')
a = ap.parse_args()
h = scan()

print(f'扫描目录：{HERE}\n')
print(f'① 评测产物（含参考答案 / 判定 / 题目清单）：{len(h["artifacts"])} 个')
for r in h['artifacts'][:12]:
    print(f'     ✗ {r}')
if len(h['artifacts']) > 12:
    print(f'     … 另 {len(h["artifacts"]) - 12} 个')
print(f'② 凭据 / 密钥特征：{len(h["secrets"])} 处')
for rel, s in h['secrets'][:6]:
    print(f'     ✗ {rel}: {s}')
print(f'③ 数据/模型本体（>5MB 或 .gguf/.db）：{len(h["big"])} 个')
for rel, size in h['big'][:6]:
    print(f'     ✗ {rel}  {size / 1048576:.1f} MB')

total = len(h['artifacts']) + len(h['secrets']) + len(h['big'])
print()
if total == 0:
    print('✅ 未发现官方「明确不公开」清单里的内容 —— 可以公开仓库')
else:
    print(f'❌ 命中 {total} 处。**公开前必须移出仓库**（.gitignore 不够，'
          f'公开仓库的历史里也会留痕）。')
    print('   建议：把评测产物移到仓外目录（如 ../aml-eval-artifacts/），'
          '只把「源码 + config + Dockerfile + README + selftest」推上去。')
sys.exit(1 if (a.strict and total) else 0)
