# LibMem-AML

> **本仓的参赛专用插件，后续开发以这一份为准。**
> 目录名用 ASCII（`LibMem-AML/`）以免在 Docker / git / 脚本里踩非 ASCII 路径的坑；
> **插件名 = `LibMem-AML`**（Library + Memory + AML；见服务 `/health` 的 `version` 串）。

面向 **AML（Agent Memory Leaderboard / Agent 记忆挑战赛）** 的提交版本。
从 `shufang-os/v2`（V1.3.4 生产版）提取裁剪而来，**只保留参赛路径真正用到的模块**。

> **为什么要单独一份、而不是加开关**：参赛版与原版是两种系统 ——
> 原版靠**时间轴上的历史**（冷库→提炼→热度→审计）运转；
> 参赛是**一次性 Add/Search**，回答与评审都归平台。
> 加开关会让两边互相污染，而且生产库里那 629 条真实记忆 + 热度 + 审计
> **绝不能被评测碰到**。

**公开仓库**：
* **GitHub（竞赛主用）**：`https://github.com/Lao-XingHe/Libmem` · SSH `git@github.com:Lao-XingHe/Libmem.git`
* **Gitee（镜像/备份）**：`https://gitee.com/lao-xinghe/lib-mem-aml` · SSH `git@gitee.com:lao-xinghe/lib-mem-aml.git`
* 分支 `main` · 提交时填**固定 commit**（见 `REPRODUCE.md`）

**许可**：**Apache License 2.0**（OSI 开源，含专利授权）· 原始工作署名见 `NOTICE` ·
复现步骤见 `REPRODUCE.md`。

**配套文档**（★ 先读总纲）：
**`docs/roadmap/AML-参赛总纲-20261004.md`（架构 · 要点 · 踩过的坑 · 口径 · 提交清单）** ·
需求依据 `docs/roadmap/AML-需求对照-20261003.md` ·
装配细目 `docs/roadmap/ARCH-记忆模块-装配-20261003.md`。

**三个数字先立住**：金标会话在场 **50/50** · 字面关键值 **40/44** ·
（平台固定提示词下）本地 9B 判对 **70.0%** —— **交付不是瓶颈，读者才是**；
而比赛里读者是平台的强模型。**已证伪、别再做**：日期锚点 · 更激进的压缩 · 让 CE 判可答性 · 给 AML 加构建层/热度/审计。

---

## 一、官方契约（本服务实现的部分）

AML 只要求参评方提供两类操作，其余全部由平台负责：

```
GET  /health   → 2xx，免鉴权
POST /add      → {"request_id","messages":[{"role","content"}],"user_id","session_id"}
                 返回 {"success","request_id","user_id","session_id"}，**落库完成后才 200**
POST /search   → {"query","user_id","top_k"} → {"data":[...]}（按相关性排序，≤ top_k）
鉴权：Authorization: Bearer <key> / Token <key> / X-Api-Key: <key>（设 AML_API_KEY 时启用）
```

**平台侧负责**（`api_config.py` 里由平台注入 `ANSWER_API` / `JUDGE_API`）：
回答生成 · 结果评审 · 分数聚合 · 任务编排。
⇒ **本插件不生成答案**，也不影响判分；模型只用在 Add 与 Search 两侧。

---

## 二、模块用上 / 没用上（**逐条给理由**）

| 层 | 参赛版 | 理由 | 依据 |
|---|---|---|---|
| **B3 表头引擎** | ✅ 核心 | 记忆库 + 原文索引，检索的地基 | 实测 |
| **B4 双门检索** | ✅ 核心 · **四路必须开** | 关：召回@10 **46.0%**；开：**97.6%**（+51.6pp），只贵 198 ms | **实测** |
| **交付层** | ✅ | 每条带时间戳；命中与上下文不混 | 见 §四 |
| **C2 → AML HTTP 服务** | ✅ **新增** | 原版 C2 是 MCP **stdio**，不是 HTTP；AML 要的是公网可达的 Add/Search | 契约 |
| B2 温提炼 | ❌ 删 | ①**唯一"建库必须调 LLM"的环节**，轮级 246,738 轮 ⇒ 约 34~137 小时 ②实测**原文索引就够 97.6% 召回** ③摘要会抹平比赛题正问的细节（`$800`/`24 days`/`38 subjects`） | 实测 + 推理 |
| B1 冷记忆 | ❌ 删 | 参赛不做实时记录（数据由平台 Add 推入）。**只借用它的 jsonl 逐轮格式**作为装载格式 | 推理 |
| B5 热度机 | ❌ 删 | 无使用史 ⇒ heat/count 是"没有定义的输入"；且热度编码**使用频率**、比赛考**内容相关** | 实测（评测库 heat/count 恒为 0） |
| B7 审计 | ❌ 删 | 对分数 **0**；`/search` 是同步接口，多写一次库只加延迟 | 实测 |
| B6 MAP 协议 | ❌ 删 | `clarify`（拒答话术）与**平台固定提示词取向相反**（平台写着 "Do not refuse just because the answer is not stated verbatim"）；`protocol.py` 在原版生产里本来就没有调用者 | 实测 |
| C1 知识库 | ❌ 删 | 赛制无知识库语料。**★ 例外**：若某赛道含文档问答，`kb_indexer.py` 要拿回来 | 契约 |
| C3 人设 | ❌ 删 | 参赛不扮演角色；**但产品系统提示词要留**（平台用自己的，所以这里也不留） | 契约 |
| watcher（Node） | ❌ 删 | 桌面集成，与答题无关 | — |

**删除后没有任何"死代码残留"**：`audit_log`/`frames` 两张表**保留定义但不写入**
（B3 的 `write_epoch`/`_integrity_hash` 仍引用它们，删表会让那几个函数炸），
检索里的 `_audit_search`/`_audit_heat`/`_apply_retrieval_heat` 都改成**显式 no-op 并写明理由**。

---

## 三、目录

```
config.yaml                     参赛口径（见 §四）
requirements.txt                3 个第三方包（PyYAML / jieba / requests）
Dockerfile                      学术代码提交路线用
selftest.py                     契约自检（22 条断言）
services/
  B3_header_engine/             记忆库 + 原文索引（+ user_id 隔离列）
  B4_dual_gate_retrieval/       双门检索 + 四路 + RRF + CE 重排 + 扩窗
  C2_aml_service/server.py      ★ AML HTTP 服务（/health /add /search）
  shared/                       配置加载 / 维度 / 任务类型
```

---

## 四、口径（**引用任何分数前先看这里**）

实测基线：LongMemEval-S 轮级库、500 题、纯检索（`longmemeval/LEADERBOARD-20261001.md` §3.15）。

| 项 | 参赛版 | 原版生产默认 | 依据 |
|---|---|---|---|
| `routes_enabled` | **true** | false | 召回 **97.6% → 46.0%**（关掉就是这么多） |
| `rerank_doc` | **raw**（原文） | auto（摘要） | 平台提示词："Your memories are episodic raw observations" |
| `phase1_limit` | **10000**（显式） | 与 FTS 可用性绑定 | 防"候选被 LIMIT 静默截断" |
| 检索耗时 | **518 ms/题** | 320 ms/题 | 四路的代价是 **+198 ms（1.62×）** |
| `apply_heat` | 恒 false | true | B5/B7 已移除 |
| `expand_window` | **0**（见下） | 0 | 参赛版**可以开**（AML 的 Add 必带 `session_id`） |

**交付文本格式**（`search` 返回的 `content`）：

```
[2023-04-20] user: My sister gave me a stand mixer for my birthday.
```

* **带日期前缀**：平台回答提示词要靠记忆里的时间戳换算相对时间（第 7 条）；
* **保留原话**：官方判词明确 "**Do NOT convert relative ↔ absolute**" ——
  所以既要给日期、也要留 "last week" 这类原始表述，让模型按问题的形式作答；
* JSON 里另给完整 `created_at` / `source_date` / `role` / `session_id`。

---

## 五、`user_id` 隔离（**本版最要紧的一处改动**）

官方契约原文：**"`user_id` is the sole retrieval isolation boundary"**。
原版没有这一列 —— 生产是单用户、评测靠 `session_id` 限定 haystack。

本版的落实**必须覆盖全部四条召回路**（漏一条就是跨用户泄漏、成绩作废）：

| 召回路 | 隔离怎么落实 |
|---|---|
| Phase1（SQL 候选） | `_filter_where(..., user_ids=)` —— **过滤的唯一收口** |
| FTS 预筛 | 同一个 `_filter_where`（`fts_search` / `raw_search` 都走它） |
| **路4 词法** | `fuse_routes(filters={'user_ids': ...})` → 转发给 `fts_search` |
| **路2 实体 / 路3 时间** | ★ `EntityRoute.from_store(user_ids=)` / `TimeRoute.from_store(user_ids=)` —— **索引按用户作用域建**（原来是全表建，`rank()` 会返回别人的记忆） |

另外两处**不显眼但同样致命**的：

1. **`memory_id` 把 `user_id` 拌进哈希**
   （`md5(f"{user_id}#{session_id}#{created_at}#{idx}#{content}")[:16]`）。
   否则两个用户出现同一句话会算出同一个 id，UPSERT 让一方"继承"另一方的行；
2. **`_route_objects` 的缓存键带上用户作用域**，否则 A 的实体倒排会被 B 的查询复用
   —— 最隐蔽的一种泄漏：查询本身没问题，错的是被复用的索引。

`selftest.py` 里有四条断言专门钉这些（含"同一句话必须存成两行"）。

---

## 六、模型服务（**部署时必须处理**）

嵌入与重排是**外部 HTTP 服务**，不在本镜像里：

| 用途 | 默认地址 | 环境变量 |
|---|---|---|
| 嵌入 bge-m3 | `http://127.0.0.1:18099` | `AML_EMBED_BASE` |
| 重排 bge-reranker-v2-m3 | `http://127.0.0.1:18098` | `AML_RERANK_BASE` |

* ⚠️ 容器里 `127.0.0.1` 指容器自己 —— 提交前**务必**把这两个地址指到可达位置，
  否则平台实测的是"退化版"，与我们本地量到的 97.6% 不是一回事。

### 6.1 ★ 实测过的两个坑（2026-10-03 · 两个服务都拒绝连接时）

| 现象 | 根因 | 修法 |
|---|---|---|
| **每个查询都返回 `{"data": []}`** | 嵌入挂 ⇒ 落 **hash 兜底** ⇒ 余弦≈0 ⇒ 被 `min_sim_threshold=0.15` **全部切掉**。而阈值卡在**四路融合之前** ⇒ **连词法路已捞到的结果也一起没了** | `phase2_rank` 加 **fail-open 护栏**：阈值只许降噪、**不许清零**；真被清空时退回未过滤的 top_k 并记 `threshold_fallback` |
| `/health` 报 `embed_reachable=True`，实际不可用 | 只探了 `/health`，没真调嵌入 | `/health` 改为**真调一次** `/v1/embeddings` 与 `/v1/rerank`，并给 `embed_detail` |

> 两条都属于"**不报错但分数归零**"那一类：一个 200 的健康检查掩盖了一场 0 分的灾难，
> 而"检索没找到"与"找到了却被阈值砍了"在日志里长得一模一样。
> 自检的**第 ⑧ 组（退化可用性）**专钉这两条 —— **模型服务全挂时也必须全绿**。

> ⚠️ **口径提示**：那条 fail-open 护栏相对本地实测口径（97.6% 召回那次）**多了一层**。
> 它只在"原本会返回空"的情形下生效（`above_threshold == 0`），
> 对"本来就有结果"的题目**逐字不变**；但它确实可能改变少数题的召回 ——
> 见 `README` §八 待办第 1 条。

---

## 七、运行

```bash
# 本地
pip install -r requirements.txt
python -m services.C2_aml_service.server --host 0.0.0.0 --port 8080 --data-dir ./data

# 容器
docker build -t shufang-aml .
docker run -p 8080:8080 -e AML_API_KEY=xxx \
  -e AML_EMBED_BASE=http://host.docker.internal:18099 \
  -e AML_RERANK_BASE=http://host.docker.internal:18098 \
  -v shufang-aml-data:/data shufang-aml

# 契约自检（22 条断言；不需要模型服务也能跑）
python selftest.py
```

**配置种入机制**：加载器只认**数据目录**里的 `config.yaml`（因为 `SHUFANG_DATA_DIR` 优先）。
所以服务启动时会把仓库模板**种进数据目录**，并在日志里打出**生效值**：

```
[aml] effective: routes_enabled=True rerank_doc=raw rerank=True phase1_limit=10000 expand_window=0
```

> ⚠️ 这条不是装饰：第一次自检就是被它抓出来的 —— 数据目录里没有 config.yaml 时，
> 加载器会**静默生成默认配置**（`routes_enabled=False`），服务照常工作、日志一切正常，
> 只是四路没开（召回 97.6% → 46.0%）。想强制覆盖已有配置：`--force-config`。

---

## 八、已知缺口 / 待定量（**参赛前该处理**）

| # | 项 | 状态 |
|---|---|---|
| 1 | **嵌入/重排的部署可达性** | ❗ 最要紧的一条：不可达就是退化版，见 §六 |
| 2 | `expand_window` 的最优 N | 未定。AML 里 `session_id` 是给的，所以**可以正确开**；先用 0 跑通，再定量（评测端 ±8 是拐点：值在场率 62%→74%） |
| 3 | `rerank_doc: raw` vs `auto` | 未对比实测（Auto 需要 B2 摘要，而 B2 已删 ⇒ 本版只能 raw） |
| 4 | 长历史（LongMemEval-**M**）上四路是否仍占优 | 未验。论文侧：轮级召回在 M 上 **0.582 < 会话 0.706** |
| 5 | 逐路消融（LongMemEval 上） | 未做。现有留一法是 **LoCoMo** 的（去词法 −12.6 / 去语义 −4.9 / 去时间 −0.4 / 去实体 −0.2） |
| 6 | 赛制细节（`/search` 超时、速率限制、Add 单次大小上限） | 官方 api-guide 是 JS 单页，未取到正文，**需人工核对** |
| 7 | 若某赛道含文档问答 | C1 知识库要拿回来 |

---

## 九、与原版的关系

* 上游：`shufang-os/v2`（V1.3.4）。提取脚本：`scratch/_aml_extract.py`；
* 本版**不含**生产数据（`data/` 由部署环境提供），也**不碰**生产库；
* 上游的实测战绩与全部结论：`longmemeval/LEADERBOARD-20261001.md`（§3.11–3.15）。


---

## 附录 · 相对上游的方法改动说明

> 竞赛「开源方法」类要求提交「公开仓库、固定 commit、**原始工作引用**和**方法改动说明**」。
> 原始工作引用见 `NOTICE`；本节是改动说明。

**上游**：书房 OS 记忆插件（Shufang OS）v1.0 – V1.3.4，归档于 `gitee.com/lao-xinghe/study`。
**本条**：其参赛子集，为 AML 的 `Add / Search` 契约而裁剪与改造。

### 一、保留下来的（三条脊椎）

| 层 | 上游实现 | 本条件 |
|---|---|---|
| 表头引擎 | `B3_header_engine`（SQLite + FTS5 + 原文索引） | **保留**，新增 `user_id` 隔离列与两个索引 |
| 双门检索 | `B4_dual_gate_retrieval`（Phase1 过滤 → 四路 → RRF → CE 重排） | **保留**；四路与 RRF 全开 |
| 交付层 | 上游把命中条目交给 MCP 工具 | **改为** AML 的 `POST /search` 响应（`{"data":[…]}`） |

### 二、移除的（及理由，逐条）

| 模块 | 移除理由 |
|---|---|
| `B1_cold_memory` | 参赛不做实时记录：数据由平台 `Add` 推入。**只借用它的逐轮 jsonl 格式**作为装载格式 |
| `B2_warm_extract` | ①唯一"建库必须调 LLM"的环节（轮级 246,738 轮 ⇒ 约 34~137 小时）②实测**原文索引即可达 97.6% 召回**③摘要会抹平竞赛正问的细节 |
| `B5_heat_state` | 无使用史 ⇒ `heat/count` 是"没有定义的输入"；且热度编码**使用频率**、竞赛考**内容相关** |
| `B7_audit` | 对分数 **0**；`/search` 是同步接口，多写一次库只加延迟 |
| `B6_map_protocol` | `clarify`（拒答话术）与**平台固定提示词取向相反**（平台写着 "Do not refuse just because…"）；`protocol.py` 在上游生产里本就没有调用者 |
| `C1_knowledge` | 竞赛文本赛道无知识库语料 |
| `C3_persona` | 参赛不扮演角色；平台用自己的 `system_prompt` |
| `dsh-cold-memory-watcher` | 桌面集成（Node），与答题无关 |

### 三、新增/改造的（本条的实质工作）

1. **`C2_aml_service/server.py`（新）**：AML `Add / Search / health` HTTP 服务。
   上游 `C2` 是 MCP **stdio**，不是 HTTP —— 这是从"插件"到"参赛服务"最大的一块新增。
2. **`user_id` 隔离（全链路）**：官方原文 "`user_id` 是 Search 接口**唯一使用**的检索范围标识"。
   落实点：`_filter_where` 唯一收口 + FTS 预筛 + **路4 走 filters** +
   **路2/路3 的索引按用户作用域建**（上游是**全表**建，会跨用户泄漏）+ `memory_id` 拌 `user_id` +
   `_route_objects` 缓存键含用户作用域。
3. **`phase2_rank` 的 fail-open 护栏**：阈值只许降噪、**不许清零**。
   （实测：嵌入服务挂 ⇒ hash 兜底 ⇒ 余弦≈0 ⇒ 上限阈值把每条都切掉 ⇒ **每个查询返回空**。）
4. **`neighbours_of` + `expand_hits`（命中即扩窗）**：上游有，本条默认开 **±2 轮**
   （5 个数据集中 3 个评测"顺序"：ScriptMem/CL-Bench 的 `ordering`、BEAM 的 `event_ordering`）。
5. **`created_at` 从 `Add` 时间戳派生**：上游 `time_bucket`/`source_date` 取"现在"，
   而竞赛是把历史一次性推入 ⇒ 用摄取时刻会让所有记忆挤进本月、**时间题直接错**。
6. **交付格式**：每条带 `[YYYY-MM-DD]` 前缀（`content`）而 `text` **不带**
   —— 后者是防 CL-Bench 的 `format_selected_memories` 产生**双时间戳**。
7. **口径改动**：`routes_enabled: false → true`（召回 **46.0% → 97.6%**）· `rerank_doc: auto → raw`
   · `phase1_limit` 显式 10000。

### 四、口径与实测

见 `REPRODUCE.md`（口径、复现命令、已知边界）。**所有数字都标注了开关组合** ——
换任何一个开关数字都会变（关四路：召回 97.6% → 46.0%）。