# -*- coding: utf-8 -*-
r"""AML（Agent Memory Leaderboard）· **参赛插件服务**

官方契约只要求两类操作（其余全部由平台负责）：

    GET  /health   → 2xx，免鉴权
    POST /add      → {"request_id","messages":[{"role","content"}],"user_id","session_id"}
                     返回 {"success","request_id","user_id","session_id"}，**落库完成后才 200**
    POST /search   → {"query","user_id","top_k"} → {"data":[...]}（按相关性排序，≤ top_k）
    鉴权：Authorization: Bearer <key> / Token <key> / X-Api-Key: <key>

## 为什么用标准库而不是 FastAPI

参赛者记录里那份实现了同一契约的公开实现也是"dependency-free HTTP wrapper"。
理由不只是省依赖：**Docker 里少一层 ASGI 栈，冷启动与内存都可控**，
而 AML 的评测机是同步调用、超时按秒算。

## 三条硬约束（都在代码里落实，不是注释）

1. **`user_id` 是唯一检索隔离边界** —— 它必须进 `_filter_where`（Phase1 + FTS 预筛）
   **并且**进四路各自的检索（路2/路3 的索引按用户作用域建、路4 走 filters）。
   漏任何一路都是跨用户泄漏，成绩作废。
2. **Add 返回 200 之前必须已持久化** —— B3 的连接是 `isolation_level=None`（autocommit），
   所以 `insert_memory()` 返回即已落盘。这里**不做批量延迟写**。
3. **每条记忆必须带自己的时间戳** —— 平台固定的回答提示词写着
   "Convert relative times … when the memory timestamp makes it clear"。
   所以交付文本带 `[YYYY-MM-DD]` 前缀；同时 JSON 里给完整 `created_at`。

## 写入口径（见 README「口径」节）

* `summary` = `f"{role}: {content}"`（**原文**，不是摘要）→ `rerank_doc: raw` 读的就是它
* `content_path` = `""` → 原文索引回退到 `summary`（`src='summary'`），**不依赖 B1 冷库**
* `core_entities` = 规则抽取（拉丁大写词 + jieba 词性），**不需要模型**
* `memory_id` = md5(`user_id#session_id#created_at#idx#content`)[:16]
  —— ★ **把 user_id 拌进哈希**：否则两个用户出现同一句话会算出同一个 id，
  UPSERT 会让其中一方"继承"另一方的行（隔离边界的静默破坏）。

用法::

    python -m services.C2_aml_service.server --host 0.0.0.0 --port 8080
    # 鉴权（可选）：设置 AML_API_KEY；数据目录：SHUFANG_DATA_DIR 或 --data-dir
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ---------------------------------------------------------------- 常量

SERVER_VERSION = "LibMem-AML/1.0（书房 OS V1.3.4 参赛子集）"
MAX_TOP_K = 100                      # 官方契约允许到 100
DEFAULT_TOP_K = 30
MAX_BODY_BYTES = 64 * 1024 * 1024    # Add 可能一次推很多消息

# 「显式要最近」的查询 → 允许**有界的**时效破平（官方契约原文：
# "Explicit latest/recent queries receive a bounded recency tie-breaker;
#  ordinary queries remain relevance-first"）。
# `RECENCY_WEIGHT` 就是那个"bounded"：0.2 = 最多把名次挪动两成。
RECENCY_WEIGHT = 0.2
_RECENT_PATTERNS = re.compile(
    r"\b(latest|most recent|recently|these days|right now|currently|nowadays"
    r"|last time|as of now)\b"
    r"|最新|最近的|最近|目前|现在|当下|上次|这一次|当前",
    re.IGNORECASE)

# 实体抽取（拉丁）：与评测端一致 —— 句首大写词，去停用词，cap 8
_LATIN_ENT = re.compile(r"\b([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})?)\b")
_LATIN_STOP = {'the', 'and', 'but', 'for', 'with', 'you', 'your', 'this', 'that',
               'there', 'then', 'they', 'them', 'when', 'what', 'which', 'who'}
# 实体抽取（中日韩）：jieba 词性白名单（人名/地名/机构/专名）
_CJK_POS = {'nr', 'ns', 'nt', 'nz'}
_ENTITY_CAP = 8
_ASCII_MARK = {'ok': '[ok]', 'fail': '[fail]'}


# ---------------------------------------------------------------- 小工具

def log(msg: str) -> None:
    print(f"[aml] {msg}", file=sys.stderr, flush=True)


def md5_12(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()[:12]


def memory_id_for(user_id: str, session_id: str, created_at: str,
                  idx: int, content: str) -> str:
    """记忆 id：**把 user_id 拌进哈希**（隔离安全）＋ 内容可复现。

    为什么不用随机 uuid：同一次 Add 被平台重放（smoke 与正式评测之间、
    或断点续传）时应当幂等 —— 重复写入同一条不该产生两条记忆，
    否则「同一句话」会在检索里占两个名次，等于自己给自己刷分。
    """
    return md5_12(f"{user_id}#{session_id}#{created_at}#{idx}#{content}")


def to_iso(value) -> str:
    """把 AML 的时间戳归一成 ISO 8601（UTC）。

    契约记录里明说时间戳是 **Unix 毫秒**；但真实数据里也能见到秒、ISO 串、
    甚至缺省。**一律容错**，且缺省时退回"现在"而不是报错 ——
    Add 失败会让整轮评测作废，而时间戳只是排序线索。
    """
    if value is None or value == "":
        return datetime.now(timezone.utc).isoformat(timespec="seconds")
    if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
        num = float(value)
        while num > 1e11:            # 毫秒 → 秒（也容忍微秒）
            num /= 1000.0
        try:
            return datetime.fromtimestamp(num, tz=timezone.utc).isoformat(timespec="seconds")
        except (OverflowError, OSError, ValueError):
            return datetime.now(timezone.utc).isoformat(timespec="seconds")
    text = str(value).strip()
    # 已经是 ISO 就原样（把结尾的 Z 换成 +00:00 便于解析）
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00"))
        return text
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).replace(
                tzinfo=timezone.utc).isoformat(timespec="seconds")
        except ValueError:
            continue
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def extract_entities(text: str, cap: int = _ENTITY_CAP) -> list:
    """**规则抽取**实体（不需要模型）—— 路2 只吃这一列。

    为什么坚持规则：
      ① 评测端 246,738 条的记忆就是这么建的（8 行正则），召回 **97.6%**；
      ② AML 的 Add 是**流式**的，每条都调模型会让写入变成瓶颈；
      ③ 实体路在留一法里只值 **−0.2**（去掉它几乎无感），不值得为它上模型。

    ⚠️ 拉丁正则靠**首字母大写**，中文数据一条都抽不到 ——
    所以中文另走 jieba 词性。两者都失败时返回空列表（**不会**让写入失败）。
    """
    out: list = []
    seen = set()

    def push(word: str) -> None:
        w = word.strip()
        if len(w) >= 2 and w not in seen:
            seen.add(w)
            out.append(w)

    for m in _LATIN_ENT.finditer(text or ""):
        if len(out) >= cap:
            break
        w = m.group(1)
        if w.lower() in _LATIN_STOP:
            continue
        push(w)
    if len(out) < cap and re.search(r"[\u4e00-\u9fff]", text or ""):
        try:
            import jieba.posseg as pseg          # 插件本来就依赖 jieba（FTS 分词）
            for word, flag in pseg.cut(text):
                if len(out) >= cap:
                    break
                if flag in _CJK_POS and len(word) >= 2:
                    push(word)
        except Exception as error:               # noqa: BLE001 - 抽不出实体不该让写入失败
            log(f"实体抽取（中文）跳过: {type(error).__name__}: {error}")
    return out[:cap]


def http_ok(url: str, timeout: float = 2.0) -> bool:
    """探活一个本地模型服务（**只测端口通不通**）。"""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return 200 <= resp.status < 300
    except (urllib.error.URLError, OSError, ValueError):
        return False


def probe_embed(base: str, timeout: float = 8.0) -> tuple:
    """**真调一次**嵌入，返回 `(ok, detail)`。

    为什么不能只探 `/health`（2026-10-03 实测教训）：
    服务在跑、`/health` 返回 200，但**嵌入请求本身失败**时，
    检索会落 hash 兜底 ⇒ 余弦全是噪声 ⇒ 被判"无有效匹配"。
    那时 `/health` 报 `embed_reachable=True`，而**每个查询返回空** ——
    一个 200 的健康检查掩盖了一场 0 分的灾难。
    所以这里坚持：**探活要探到"能算出向量"，不是"端口开着"。**
    """
    url = base.rstrip("/") + "/v1/embeddings"
    body = json.dumps({"input": ["health check"], "model": "bge-m3"}).encode("utf-8")
    req = urllib.request.Request(url, data=body,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        vec = ((payload.get("data") or [{}])[0] or {}).get("embedding") or []
        nonzero = sum(1 for x in vec if x)
        if not vec:
            return False, "响应里没有 embedding"
        if nonzero == 0:
            return False, f"向量全零（dim={len(vec)}）"
        return True, f"dim={len(vec)}"
    except Exception as error:                 # noqa: BLE001
        return False, f"{type(error).__name__}: {error}"


def probe_rerank(base: str, timeout: float = 8.0) -> tuple:
    """**真调一次**重排。重排挂掉是 fail-open（按原名次交付），不像嵌入那样致命，
    但仍必须报出来 —— 因为那意味着 CE 这一环的收益完全没拿到。"""
    url = base.rstrip("/") + "/v1/rerank"
    body = json.dumps({"query": "health", "documents": ["a", "b"],
                       "model": "bge-reranker-v2-m3"}).encode("utf-8")
    req = urllib.request.Request(url, data=body,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        return True, f"keys={sorted(payload)[:4]}"
    except Exception as error:                 # noqa: BLE001
        return False, f"{type(error).__name__}: {error}"


# ---------------------------------------------------------------- 服务

class AmlService:
    """把 AML 的 Add/Search 映射到 B3（库）+ B4（检索）。"""

    def __init__(self, data_dir: str, force_config: bool = False):
        self.data_dir = os.path.abspath(data_dir)
        os.environ["SHUFANG_DATA_DIR"] = self.data_dir
        os.makedirs(os.path.join(self.data_dir, "index"), exist_ok=True)
        self._seed_config(force_config)
        self._lock = threading.Lock()
        self._retrieve = None
        self._store = None
        self._added = 0
        self._searched = 0

    # ---- ★ 配置必须**种进数据目录**，否则读了默认值 ----
    def _seed_config(self, force: bool) -> None:
        """把仓库里的 `config.yaml` 复制到数据目录（加载器只认数据目录那份）。

        为什么必须做，而不是"把 config 放仓库根就行"：
        `loader._get_base_dir()` 的逻辑是「**先看 `SHUFANG_DATA_DIR`**，
        有就用它，没有才向上找含 config.yaml 的目录」。而本服务为了隔离
        必须设 `SHUFANG_DATA_DIR`（每个部署/每个用户库各自独立），
        于是数据目录里没有 config.yaml 时，加载器会**静默生成一份默认配置** ——
        `routes_enabled` 默认 False、`rerank_doc` 默认 auto。

        这正是本仓记录过的同类故障（"默认配置被静默采用"），
        而且它的表现极具迷惑性：**服务照常工作、日志一切正常、只是四路没开**
        （实测召回 97.6% → 46.0%）。第一次自检就是被这条断言抓出来的。
        """
        template = os.path.join(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__)))), "config.yaml")
        target = os.path.join(self.data_dir, "config.yaml")
        if not os.path.exists(template):
            log(f"⚠️ 找不到配置模板 {template} —— 将使用内置默认值（四路可能未开）")
            return
        if os.path.exists(target) and not force:
            log(f"配置：沿用数据目录里已有的 {target}"
                "（要强制覆盖：--force-config 或 AML_FORCE_CONFIG=1）")
            return
        with open(template, "r", encoding="utf-8") as src, \
                open(target, "w", encoding="utf-8") as dst:
            dst.write(src.read())
        log(f"配置：已从模板种入 {target}")

    def effective_config(self) -> dict:
        """把**生效的**检索口径报出来（排障时最常需要的就是这三个值）。"""
        try:
            from services.shared.config.loader import get_config
            cfg = get_config()
            ret = cfg.get("retrieval") or {}
            return {"routes_enabled": bool(ret.get("routes_enabled")),
                    "routes": ret.get("routes"),
                    "rerank_doc": ret.get("rerank_doc"),
                    "rerank_enabled": bool(ret.get("rerank_enabled")),
                    "phase1_limit": ret.get("phase1_limit"),
                    "fts_prefilter": bool(ret.get("fts_prefilter")),
                    "expand_window": ret.get("expand_window")}
        except Exception as error:                 # noqa: BLE001
            return {"error": f"{type(error).__name__}: {error}"}

    # ---- 懒加载：让 /health 在模型服务没起时也能立刻 200 ----
    @property
    def store(self):
        if self._store is None:
            with self._lock:
                if self._store is None:
                    from services.B3_header_engine import sqlite_store as SS
                    SS.init_db()
                    SS.ensure_fts()
                    self._store = SS
        return self._store

    @property
    def retrieve(self):
        if self._retrieve is None:
            with self._lock:
                if self._retrieve is None:
                    from services.B4_dual_gate_retrieval import retrieve as fn
                    self._retrieve = fn
        return self._retrieve

    # ------------------------------------------------------------ ADD
    def add(self, body: dict) -> dict:
        request_id = str(body.get("request_id") or "")
        user_id = str(body.get("user_id") or "")
        session_id = str(body.get("session_id") or "")
        messages = body.get("messages") or []
        if not isinstance(messages, list):
            raise ValueError("messages must be a list")
        if not user_id:
            raise ValueError("user_id is required (it is the isolation boundary)")

        # ★ 逐条落库：**不做批量延迟写**（契约要求 200 之前已持久化）。
        #   B3 的连接是 autocommit，所以 insert_memory 返回即已 commit。
        added = 0
        for idx, msg in enumerate(messages):
            if not isinstance(msg, dict):
                continue
            content = msg.get("content")
            if not isinstance(content, str) or not content.strip():
                continue
            # role 契约上"任何非空生产者角色都保留"，不限 user/assistant
            role = str(msg.get("role") or "").strip() or "unknown"
            ts = (msg.get("created_at") or msg.get("timestamp") or msg.get("time")
                  or body.get("created_at") or body.get("timestamp"))
            created_at = to_iso(ts)
            text = f"{role}: {content}"
            self.store.insert_memory(
                summary=text,
                task_type="fact",                 # 参赛版不用它检索，给常量（评测端同做法）
                session_id=session_id,
                created_at=created_at,
                core_entities=extract_entities(content),
                content_path="",                  # ★ 原文索引回退到 summary（不依赖 B1）
                user_id=user_id,
                memory_id=memory_id_for(user_id, session_id, created_at, idx, content),
                layer="cold",
            )
            added += 1
        # 写入缓存失效：让四路的实体/时间索引重建（缓存键含 write_epoch）
        self._added += added
        log(f"add ok user={user_id[:8]}… session={session_id[:12]}… +{added} (total {self._added})")
        return {"success": True, "request_id": request_id,
                "user_id": user_id, "session_id": session_id, "added": added}

    # ---------------------------------------------------------- SEARCH
    def search(self, body: dict) -> dict:
        query = str(body.get("query") or "").strip()
        user_id = str(body.get("user_id") or "")
        if not query:
            raise ValueError("query is required")
        if not user_id:
            raise ValueError("user_id is required (it is the isolation boundary)")
        try:
            top_k = int(body.get("top_k") or DEFAULT_TOP_K)
        except (TypeError, ValueError):
            top_k = DEFAULT_TOP_K
        top_k = max(1, min(top_k, MAX_TOP_K))

        t0 = time.time()
        result = self.retrieve(
            query_text=query,
            # ★★ 隔离边界。空列表 = 不限定 —— 这里**绝不**允许空，
            #    否则 A 的查询会搜到 B 的记忆（见 user_id 的 required 校验）。
            user_ids=[user_id],
            max_items=top_k,
            final_topk=top_k,
            apply_heat=False,        # 参赛版无使用史；B5/B7 已移除
            rerank_doc="raw",        # 读原文（平台提示词："memories are episodic raw observations"）
        )
        items = result.get("items") or []
        data = [self._to_payload(it) for it in items]
        if _RECENT_PATTERNS.search(query):
            data = self._recency_blend(data)
        self._searched += 1
        log(f"search ok user={user_id[:8]}… k={top_k} → {len(data)} "
            f"({(time.time() - t0) * 1000:.0f}ms)")
        return {"data": data}

    @staticmethod
    def _to_payload(item: dict) -> dict:
        """把 B4 的条目转成 AML 交付格式。

        ★ `content` 必须**自带时间戳**：平台固定的回答提示词要求
        "when the memory timestamp makes it clear" 才能把相对时间换算出来。
        原文一并保留（含 "last week" 这类相对表述）—— 官方判词明确
        "Do NOT convert relative ↔ absolute"，所以绝不能只给算好的日期。
        """
        summary = str(item.get("summary") or "")
        role, _, text = summary.partition(": ")
        if not text:
            role, text = "", summary
        created_at = str(item.get("created_at") or "")
        day = str(item.get("source_date") or "") or created_at[:10]
        content = f"[{day}] {summary}" if day else summary
        # ★ 字段名做**冗余**（2026-10-03，读官方契约源码后加）：
        #   不同数据集的编排器读**不同的键** ——
        #     · CL-Bench 的内部清单读 `text`（`format_selected_memories` 用 `item["text"]`）
        #     · 其余用 `content` / `memories` / `retrieved_context`
        #   `{"data":[...]}` 的条目 schema 官方没有逐字规定，所以多给一个别名是**便宜的保险**：
        #   键名不匹配会让整条记忆在拼装时被**静默丢掉**（"检索到了但读者看不到"），
        #   而这类故障在分数上只表现为"答错"，极难定位。
        return {
            "id": item.get("memory_id"),
            # `content` 带日期前缀：服务**不自己加时间戳**的编排器（LongMemEval 风格）
            "content": content,
            # `text` **不带**前缀：CL-Bench 的 `format_selected_memories` 会自己渲染成
            # `- [{created_at}] {text}` —— 我们再带一层就变成
            # `- [2023-05-27T…] [2023-05-27] user: …`（**双时间戳**，实测 1444/1444）。
            # 两个键各服务一类编排器，这才是"键名冗余"的正解。
            "text": summary,
            "role": role or None,
            "session_id": item.get("session_id") or None,
            "user_id": item.get("user_id") or None,
            "created_at": created_at or None,
            "source_date": day or None,
            "score": item.get("_score"),
        }

    @staticmethod
    def _recency_blend(data: list) -> list:
        """**有界的**时效破平：只对"显式要最近"的查询生效。

        为什么必须"有界"（官方契约原话是 bounded）：时效是**排序线索**，
        不是相关性。若让它主导，问"我最喜欢什么"这类题会被"最新那条"顶掉。
        做法：相关性按名次归一（0~1），时效按时间戳归一（0~1），
        按 `RECENCY_WEIGHT` 线性混合后重排 —— 最多把名次挪动两成。
        """
        if len(data) < 2:
            return data
        n = len(data)

        def ts_key(row: dict) -> float:
            raw = row.get("created_at") or row.get("source_date") or ""
            try:
                return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).timestamp()
            except ValueError:
                return 0.0

        stamps = [ts_key(r) for r in data]
        lo, hi = min(stamps), max(stamps)
        span = (hi - lo) or 1.0
        scored = []
        for i, row in enumerate(data):
            rel = (n - i) / n
            rec = (stamps[i] - lo) / span
            scored.append(((1 - RECENCY_WEIGHT) * rel + RECENCY_WEIGHT * rec, i, row))
        scored.sort(key=lambda t: (-t[0], t[1]))       # 同分保持原名次（稳定）
        return [row for _, _, row in scored]

    # ---------------------------------------------------------- HEALTH
    def health(self) -> dict:
        cfg = self.effective_config()
        embed_base = (os.environ.get("AML_EMBED_BASE")
                      or "http://127.0.0.1:18099")
        rerank_base = (os.environ.get("AML_RERANK_BASE")
                       or "http://127.0.0.1:18098")
        embed_ok, embed_detail = probe_embed(embed_base)
        rerank_ok, rerank_detail = probe_rerank(rerank_base)
        return {
            "status": "ok",
            "version": SERVER_VERSION,
            "data_dir": self.data_dir,
            "added": self._added,
            "searched": self._searched,
            # ★ 真探活（不是"端口开着"）：嵌入挂掉 ⇒ 语义路退化 ⇒ 交付质量大降。
            #   实测 2026-10-03：两个服务都拒绝连接，而只探 /health 的版本报 True。
            "embed_ok": embed_ok,
            "embed_detail": embed_detail,
            "rerank_ok": rerank_ok,
            "rerank_detail": rerank_detail,
            # 兼容旧键名（早期版本只报可达性）
            "embed_reachable": embed_ok,
            "rerank_reachable": rerank_ok,
            # ★ 生效口径（不是"模板里写的"）—— 排障第一眼要看的就是它
            "config": cfg,
            "routes_enabled": cfg.get("routes_enabled"),
            "rerank_doc": cfg.get("rerank_doc"),
        }


# ---------------------------------------------------------------- HTTP

class Handler(BaseHTTPRequestHandler):
    server_version = "aml-plugin"
    protocol_version = "HTTP/1.1"
    svc: AmlService = None            # 由 main() 注入
    api_key: str = ""

    # ---- 基础设施 ----
    def _send(self, code: int, payload: dict) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _auth_ok(self) -> bool:
        if not self.api_key:
            return True
        header = self.headers.get("Authorization", "") or ""
        token = ""
        if header.lower().startswith("bearer "):
            token = header[7:].strip()
        elif header.lower().startswith("token "):
            token = header[6:].strip()
        if not token:
            token = (self.headers.get("X-Api-Key") or "").strip()
        return token == self.api_key

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        if length > MAX_BODY_BYTES:
            raise ValueError(f"body too large: {length} > {MAX_BODY_BYTES}")
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def log_message(self, fmt, *args):        # 默认会往 stderr 打两行/请求，太吵
        return

    # ---- 路由 ----
    def do_GET(self):                          # noqa: N802
        path = self.path.split("?")[0].rstrip("/") or "/"
        if path in ("/health", "/healthz"):
            self._send(200, self.svc.health())   # ★ 免鉴权
            return
        self._send(404, {"error": "not found", "path": path})

    def do_POST(self):                         # noqa: N802
        path = self.path.split("?")[0].rstrip("/") or "/"
        if path not in ("/add", "/search"):
            self._send(404, {"error": "not found", "path": path})
            return
        if not self._auth_ok():
            self._send(401, {"error": "unauthorized"})
            return
        try:
            body = self._read_json()
        except Exception as error:             # noqa: BLE001
            self._send(400, {"error": f"bad request body: {type(error).__name__}: {error}"})
            return
        try:
            if path == "/add":
                self._send(200, self.svc.add(body))
            else:
                self._send(200, self.svc.search(body))
        except ValueError as error:            # 契约错误 → 400（平台能看出是自己发错了）
            self._send(400, {"error": str(error)})
        except Exception as error:             # noqa: BLE001 - 其它异常如实 500，不吞
            log(f"ERROR {path}: {type(error).__name__}: {error}")
            self._send(500, {"error": f"{type(error).__name__}: {error}"})


def main() -> int:
    ap = argparse.ArgumentParser(description="AML Add/Search service (shufang-os 参赛版)")
    ap.add_argument("--host", default=os.environ.get("AML_HOST", "0.0.0.0"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("AML_PORT", "8080")))
    ap.add_argument("--data-dir", default=os.environ.get("SHUFANG_DATA_DIR")
                    or os.path.join(os.path.dirname(os.path.dirname(
                        os.path.dirname(os.path.abspath(__file__)))), "data"))
    ap.add_argument("--force-config", action="store_true",
                    default=os.environ.get("AML_FORCE_CONFIG") == "1",
                    help="用仓库模板覆盖数据目录里已有的 config.yaml")
    args = ap.parse_args()

    Handler.svc = AmlService(args.data_dir, force_config=args.force_config)
    Handler.api_key = (os.environ.get("AML_API_KEY") or "").strip()
    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    log(f"listening on http://{args.host}:{args.port}  data_dir={Handler.svc.data_dir}")
    log("auth: " + ("ON (AML_API_KEY)" if Handler.api_key else "OFF (AML_API_KEY 未设置)"))
    # ★ 启动就把**生效**口径打出来：四路没开是最贵的一种静默故障
    eff = Handler.svc.effective_config()
    log(f"effective: routes_enabled={eff.get('routes_enabled')} "
        f"rerank_doc={eff.get('rerank_doc')} rerank={eff.get('rerank_enabled')} "
        f"phase1_limit={eff.get('phase1_limit')} expand_window={eff.get('expand_window')}")
    if eff.get("routes_enabled") is not True:
        log("⚠️⚠️ 四路未开！实测召回会从 97.6% 掉到 46.0% —— "
            "检查数据目录里的 config.yaml（或加 --force-config）")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        log("bye")
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
