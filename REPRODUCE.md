# LibMem-AML · 复现文档

> **LibMem-AML** = Library + Memory + AML。AML（Agent Memory Leaderboard）参赛用的记忆系统实现。
> 本文件说明"**怎么把我的数字复现出来**"——竞赛要求"公开代码、配置、原始工作署名与复现材料"，
> 这就是那份复现材料。

---

## 〇、三个数字先立住（复现的目标）

| 数字 | 值 | 复现命令 |
|---|---|---|
| **金标会话在场**（交付质量） | **488 / 500 = 97.6%** | `python audit_delivery.py --tag <tag>` |
| **字面关键值在场** | **396 / 420 = 94.3%**（真漏 24 = 4.8%） | 同上 |
| **格式零静默丢条** | `created_at`/`text`/`role` 缺键 **0 / 1444**，双时间戳 **0** | `python audit_format.py --tag <tag>` |

> ⚠️ **口径必须一起引用**：以上都是 **LongMemEval-S（500 题）** 上、用
> `routes_enabled=true` · `rerank_doc=raw` · `expand_window=2` · `apply_heat=false` 测出来的。
> 换任何一个开关，数字都会变（关四路会让召回从 97.6% 掉到 46.0%）。

---

## 一、环境

| 项 | 要求 | 说明 |
|---|---|---|
| Python | **3.12** | 只用标准库 + 3 个第三方包（见 `requirements.txt`） |
| 嵌入服务 | **bge-m3** @ `127.0.0.1:18099`（llama.cpp，OpenAI 兼容 `/v1/embeddings`） | 缺了不会崩，但语义路退化（见 §五） |
| 重排服务 | **bge-reranker-v2-m3** @ `127.0.0.1:18098`（`/v1/rerank`） | 缺了 CE 重排整环跳过（fail-open 保序） |
| 显存 | **≈2.3 GB**（嵌入 1.25 + 重排 1.0） | 实测 |
| 依赖 | `pip install -r requirements.txt` | PyYAML · jieba · requests |

**这两个服务怎么起**（本机示例，llama.cpp）：

```powershell
# 嵌入
llama-server.exe -m bge-m3-F16.gguf --host 127.0.0.1 --port 18099 --embedding -ngl 99 -fa auto
# 重排
llama-server.exe -m bge-reranker-v2-m3-Q8_0.gguf --host 127.0.0.1 --port 18098 --reranking -ngl 99 -fa auto
```

> 地址可用环境变量覆盖：`AML_EMBED_BASE` / `AML_RERANK_BASE`。
> `/health` 会**真调一次**这两个接口再报 `embed_ok` / `rerank_ok`（不是只探端口）。

---

## 二、起服务

```bash
# 本地
python -m services.C2_aml_service.server --host 0.0.0.0 --port 8080 --data-dir ./data
# 容器（Dockerfile 里已把自检放进构建期：自检不过就不产出镜像）
docker build -t libmem-aml . && docker run -p 8080:8080 -e AML_API_KEY=xxx -v libmem-data:/data libmem-aml
```

**三个端点**（AML 官方契约）：

```bash
curl -s localhost:8080/health                       # 2xx，免鉴权
curl -s localhost:8080/add -H 'Content-Type: application/json' -H 'X-Api-Key: xxx' -d '{
  "request_id":"r1","user_id":"u1","session_id":"s1",
  "messages":[{"role":"user","content":"My sister gave me a stand mixer.","timestamp":1682000000000}]}'
curl -s localhost:8080/search -H 'Content-Type: application/json' -H 'X-Api-Key: xxx' \
  -d '{"query":"What did my sister give me?","user_id":"u1","top_k":10}'
```

---

## 三、复现我们的测试（按从快到慢）

| 命令 | 内容 | 耗时 | 依赖模型服务 |
|---|---|---|---|
| `python selftest.py` | **32 条契约断言**（隔离 / 时间戳 / 幂等 / 退化可用） | ~40 s | 不需要（全挂也须全绿） |
| `python flowtest.py` | **19 条端到端**（真起子进程 + **杀进程重启验持久化**） | ~90 s | 需要 |
| `python check_submission.py` | 提交前合规（评测产物/凭据/数据不进仓） | <5 s | 不需要 |
| `python audit_format.py --tag <t>` | 格式兼容（CL-Bench 双时间戳 / 两槽 role / 序列完整性） | ~1 min/50 题 | 需要 |
| `python audit_delivery.py --tag <t>` | 交付质量（金标在场 / 字面关键值 / 计算型 vs 真漏） | ~1 min/50 题 | 需要 |
| `python eval50.py --ids <清单> --tag <t>` | **端到端作答**（平台官方固定提示词 + 本地 9B） | ~5 min/50 题 | 需要 9B |

**`--tag` 说明**：`audit_*` 读 `hyp_<tag>.json`（每题一行，含 `question_id`）。
该文件**含参考答案**，属于官方"明确不公开"清单 ⇒ **评测产物放在仓外**（`../aml-eval-artifacts/`）。

---

## 四、口径（写进任何引用）

```yaml
retrieval:
  routes_enabled: true     # 四路 + RRF。关掉：召回 97.6% → 46.0%
  routes: semantic,entity,time,lexical
  rerank_enabled: true
  rerank_doc: raw          # 读**原文**（平台提示词："episodic raw observations"）
  rerank_pool: 30
  fts_prefilter: true
  phase1_limit: 10000
  expand_window: 2         # 命中轮 ±2；3/5 个数据集评测"顺序"（ordering / event_ordering）
  embed_backend: llama
  embed_llama_base: http://127.0.0.1:18099
```

**实测延迟**：检索 **~518 ms/题**（四路全开；关四路 320 ms）。
**上下文中位**：31,091 字符 / 题（交付条数中位 28）。

---

## 五、已知边界（**必须一起看**）

| # | 项 | 状态 |
|---|---|---|
| 1 | `/search` 的**超时与速率限制** | ❓ 官方 README 未写，`api-guide` 是 JS 单页 ⇒ **只能申请 Key 后跑 Smoke 才知** |
| 2 | 除 LongMemEval-S 外的 5 个数据集 | 只读过官方契约（`data/*/pipeline.py`），**未跑真实数据** |
| 3 | 模型服务缺失时 | **不崩、不返回空**（fail-open + 阈值护栏），但语义路退化成 hash 噪声 ⇒ 只剩词法/实体/时间三路 |
| 4 | 审计判据的残余误报 | `hist` 用**整段 haystack**，所以"在历史某处出现、但不在金标会话里"的值仍会被算成该送 ⇒ 4.8% 里含少量误报 |
| 5 | 本地 9B 在平台口径下判对 **35/50 = 70.0%** | 折成全量 **68.0%**。**但这测的是读者** —— 比赛里读者是平台的模型 ⇒ 此数**不代表比赛成绩** |

---

## 六、目录

```
LibMem-AML/
├── services/
│   ├── B3_header_engine/        记忆库 + FTS 索引 + 原文索引（+ user_id 隔离列）
│   ├── B4_dual_gate_retrieval/  四路 + RRF + CE 重排 + 扩窗
│   ├── C2_aml_service/server.py AML Add/Search HTTP 服务  ← 交付面
│   └── shared/                  配置加载 / 维度 / 任务类型
├── config.yaml                  参赛口径（§四）
├── selftest.py                  32 条契约断言
├── flowtest.py                  19 条端到端（含重启持久化）
├── check_submission.py          提交前合规
├── audit_delivery.py            交付质量（机械，不调模型）
├── audit_format.py              格式兼容（机械）
├── eval50.py                    端到端作答（9B + 平台官方提示词）
├── Dockerfile / requirements.txt
└── REPRODUCE.md（本文件）
```

---

## 七、署名与许可

* **署名**：本实现的原始工作为「书房 OS」（Shufang OS）记忆插件 V1.3.4 的参赛子集。
  作者署名见仓库 `AUTHOR` 字段 / 提交表单。**竞赛要求"原始工作署名"，不要求法定真名**；
  但**奖励与赛事身份**通常需实名，那属于报名信息，与代码署名是两件事。
* **许可**：**Apache License 2.0**（OSI 认证的开源协议，含明确专利授权）。
  选它的理由：竞赛的「开源方法」类别要的是**公开、可复现的开源实现**，而 Apache-2.0 是
  科研与工业界最通行的选择；它要求的 **`NOTICE` 文件**同时承载了竞赛要求的
  「**原始工作署名**」（见仓库根 `NOTICE`）。
* ⚠️ **与上游的差别要说清**：上游 `gitee.com/lao-xinghe/study` 用的是
  **PolyForm Noncommercial**（限制商用）；**本参赛仓库改为 Apache-2.0 ⇒ 允许他人商用**
  —— 这是"符合比赛要求"的必要代价。若仍想对上游保留商用限制，就**双许可**：
  上游保持 PolyForm、参赛库 Apache-2.0（**目前就是这个状态**）。


---

## 八、容量与运行限制（**申请表单要填这一栏**）

> AML api-guide 原文：申请时需填「**容量和运行限制**」；
> 而 `429` 的归因是「**参测接口容量不足**，或平台评测配额尚未恢复」。
> ⇒ 报高了会被并发打垮（429 → 有界重试 → 仍失败），所以这一栏必须是**实测值**。

复现命令：`python bench_concurrency.py --levels 1,2,4,8 --n 12`
（先灌 300 条记忆再打；实测环境：本机单卡 12GB 显存，嵌入/重排与本体服务同机）

| 路径 | 并发 | p50 | p95 | max | 成功率 |
|---|---|---|---|---|---|
| `/search` | 1 | 192 ms | 243 ms | 1348 ms | 12/12 |
| `/search` | 2 | 188 ms | 295 ms | 316 ms | 12/12 |
| `/search` | 4 | 320 ms | 456 ms | 457 ms | 12/12 |
| **`/search`** | **8** | **679 ms** | **763 ms** | 848 ms | **12/12** |
| `/add` | 1 | 34 ms | 49 ms | 50 ms | 12/12 |
| `/add` | 8 | 58 ms | 105 ms | 120 ms | 12/12 |

**建议申报（留一档余量）**：

* **支持并发**：**≥ 8**（实测档 100% 成功；更高档未测，建议按 8 申报）
* **`/search` 延迟**：单并发 **p50 192 ms / p95 243 ms**；8 并发 **p95 763 ms**
* **`/add` 延迟**：8 并发 **p95 105 ms**
* **单请求超时上限**：平台允许 30 分钟，**我们远低于**（最慢实测 1.35 s）
* **资源**：1 张 ≥12 GB 显存卡（嵌入 1.25 GB + 重排 1.0 GB + 余量）；CPU 4 核以上；无需外网

⚠️ **两条限制要说清（否则等于虚报）**：

1. **延迟随「该用户记忆总量」增长** —— 本表是 **300 条/用户** 下测的。
   检索的 Phase2 是 O(候选)，而 `phase1_limit: 10000` 是上限；用户记忆上万条时需重测。
2. **模型服务同一台机器** —— 嵌入/重排是**本地** HTTP 服务，若平台把服务部署到没有 GPU 的环境，
   会退化成"词法+实体+时间"三路（**不崩、不返回空**，但质量下降）。**建议声明必须带 GPU**。

### 新增脚本（本表与 §三 命令表）

| 命令 | 内容 |
|---|---|
| `python bench_concurrency.py` | 并发曲线（生成本节表格） |
| `python audit_delivery.py --tag <t> --k 100` | 交付上限对比（`k=10` vs `k=100`） |

### `top_k` 交付上限的实测取舍（全量 500 题）

**背景**：api-guide 规定「正式外部评测**固定 `top_k: 100`**」，而本实现的调优与全部历史测量
都在 `k=10`（命中 10 + 扩窗 ±2 ⇒ 交付中位 28 条）。**所以必须量一次 k=100。**

| 指标 | `k=10`（历史口径） | `k=100`（正式评测值） | 差 |
|---|---|---|---|
| 金标会话在场 | 488/500 = 97.6% | **492/500 = 98.4%** | +0.8pp |
| 字面关键值在场 | 396/420 = 94.3% | **399/420 = 95.0%** | +0.7pp |
| 真·交付漏了 | 24 | **21** | −3 题 |
| 上下文中位 | 31,091 字符 | **59,720 字符** | **×1.92** |
| 交付条数中位 | 28 | **62** | ×2.2 |

复现：`python audit_delivery.py --tag <t> --k 10` / `--k 100`（**纯机械，不调模型**）

**决定：不加交付上限，照平台给的 `top_k` 返回。** 依据：

1. 收益方向正确且是全量表上的净收益（金标 +4 题、真漏 −3 题）；
2. 成本 60k 字符 ≈ 15–20k token，对平台的回答模型不是瓶颈；
3. 官方明确「返回数量**不得超过** `top_k`」——「不得超过」是上界，少返合法，
   但既然多返更好，就没有理由人为截断。

⚠️ 一条口径提示：**70.0%（本地 9B）那个数字是在 `k=10` 下测的**。
`k=100` 下读者会看到约两倍证据，**本地 9B 那一档未重测**（比赛里读者是平台的模型，
而本表量的是**我们可控的交付质量**，它在 k=100 下更高）。