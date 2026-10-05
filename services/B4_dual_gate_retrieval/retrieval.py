"""B4 双阶段门控检索管道

规格：《书房 OS 迁入 dsh · 执行任务清单 P1-2》

Phase1 表头过滤：WHERE 条件（task_type, time_bucket, layer, core_entities）把候选从全库压到百级
Phase2 向量精排：候选集内取 Top-K=20 → score = sim + α·heat + β·count → Top-5
预算裁剪：max_tokens / max_items 硬上限，超限 truncated=true

嵌入模型：优先 Ollama bge-m3（1024 维，中文友好），fallback 到 hash embedding

性能（2026-09-21 修复）
----------------------
原实现每次检索都对 Phase1 全量候选逐条调 Ollama（单条实测 2.17s，771 条 ≈ 28 分钟）。
现在改为：
  1. 候选向量落盘缓存（embed_cache.py，键 = sha256(模型+文本)），命中即零成本
  2. 未命中的走 Ollama 原生批量接口 /api/embed，多批次并行
  3. 缓存全空时的冷启动约 30s 建库；建好之后单次检索约 3s（只剩 query 向量）
  4. 维度不一致时用同一套 fallback 重编码 query，不再静默判 0 分

验收：端到端 300ms 内返回（桥模型耗时除外）；召回较纯向量提升 ≥30%
"""
import time, hashlib, math, json, uuid, requests, threading, sys
import concurrent.futures as _futures
from collections import OrderedDict as _OrderedDict
from operator import mul as _mul
from typing import Optional

from . import embed_cache


# ---------- 权重参数（可配置） ----------
# 2026-09-23 从 0.3/0.2 压到 0.1/0.1；2026-09-24 复核注释与参数不一致：
# 0.1+0.1=0.2，注释原写「≤0.1」不对。0.2 上限是合理的 —— boost 吃掉 0.2 后
# 最多能盖过 0.19 的相似度差，但 sim≥0.2 的真正相关条目不会被热度反超。
# （实测 0.3/0.2 时 boost=0.5 能吃掉 0.4 的相似度差，才是真问题）
_ALPHA = 0.1    # heat 权重
_BETA = 0.1     # count 权重

# 归一化常数：heat/count 各自先归一到 [0,1] 再乘权重。
# heat 的 50 = 「被翻出来 50 次即打满热度项」（检索每次 +1.0）。
# count 的 10 = 「被采用 10 次即打满次数项」。
# ⚠️ 2026-09-25：count 的常数从 100 改成 10。原因是 count 的语义变了 ——
# 它不再由检索交付产生（那是每次搜索都涨），改由 `adopted` 显式上报产生，
# 量级下降约两个数量级。常数若还是 100，`count=5` 只贡献 0.005，
# `_BETA` 这一项会实际永久失效（等于把「count 保底」这条设计悄悄废掉）。
_HEAT_NORM = 50.0
_COUNT_NORM = 10.0

_TOPK_PHASE2 = 20
# Phase2 池的**下限**（不是上限）。实际池 = max(它, final_topk, mmr_pool)：
# 20 这个数字当年是"只交付 20 条"的硬编码上限，2026-09-26 改成下限。
# 老语义的害处见 retrieve() 里 phase2_topk 的注释（静默截断让"池子多大才够"
# 这类测量得出假结论）。
# 为什么放大不花钱：phase2_rank 对**全部候选**都算了分，top_k 只决定返回多少。
# Phase2 池的绝对上限，防大库上无界放大内存。当前候选量（5,871 行库 ≈ 1.5k 候选）
# 远低于它，正常调用碰不到；碰到会在 rank_stats["pool_capped"] 里报出来。
_PHASE2_POOL_MAX = 2000
_TOPK_FINAL = 5
_MIN_SIM_THRESHOLD = 0.15   # 低于此值视为"无有效匹配"，不进入 Final Top-N
# ★ 参赛版（2026-10-03）：把它做成**可配**（`retrieval.min_sim_threshold`，缺省仍是 0.15）。
#   为什么：这个阈值的取值依赖"嵌入是否健康" —— 嵌入服务挂掉落 hash 兜底时，
#   余弦全是噪声，0.15 会把**每一条**都判成"无有效匹配"。护栏见 `phase2_rank`。
def _min_sim_threshold() -> float:
    try:
        return float(_cfg_float("min_sim_threshold", _MIN_SIM_THRESHOLD))
    except Exception:            # noqa: BLE001 - 配置坏了不该让检索失败
        return _MIN_SIM_THRESHOLD

# ---------- 进程内候选向量缓存 LRU 上限 ----------
# 2026-09-24 加：A4 修复时顺带加 LRU。10 万行常驻 1024 维向量 ≈ 400MB，
# 5000 是 5 倍于 Phase1 上限（1000）的余量，正常不会触发淘汰。
_CANDIDATE_VEC_LRU_MAX = 5000

# ---------- Embedding 后端配置 ----------
#
# 主后端：llama.cpp 的 bge-m3（OpenAI 兼容 /v1/embeddings）
# 嵌入后端：llama.cpp 的 bge-m3（唯一后端；Ollama 兜底已于 2026-09-22 移除）
#
# 为什么换成 llama.cpp（2026-09-21 实测，见 README「检索为什么是 2.1 秒」）：
#   Ollama      平均 3081ms / 热态 2163ms
#   llama.cpp   平均   96ms / 热态   18ms      —— 快约 30 倍
# 而且两个后端算出的向量**余弦相似度 0.999988 ≈ 1.0**，所以既有向量缓存不用重建。
#
# Ollama 那 2.1 秒与文本长度/条数几乎无关（1 个字符也要 2.1s），是它的请求开销，
# 不是模型计算 —— 所以只能在传输层解决，换个后端立刻好。
_LLAMA_EMBED_BASE = "http://127.0.0.1:18099"
_EMBED_MODEL = "bge-m3"
_EMBED_CACHE: dict[str, list[float]] = {}    # text → vec 缓存
_EMBED_LOCK = threading.Lock()

# 健康缓存：llama 后端挂掉后，短时间内不再每次重试（避免每次都白等一个超时）
_LLAMA_DOWN_UNTIL = 0.0
_LLAMA_RETRY_AFTER = 30.0

# 批量接口参数（可被 config.retrieval 覆盖）
_EMBED_BATCH = 64       # 单次请求送多少条文本
_EMBED_WORKERS = 4      # 并行批次上限

# ---- cross-encoder 重排序服务（第 4 项，2026-09-26 加）----
# 单独一个 llama-server 进程跑 bge-reranker-v2-m3，**不走 8089 的 router**：
# router 是 `--models-max 1`，走它每次重排都要卸载 LLM 再装载 reranker（每次几秒）。
# 启动：dsh-cold-memory-watcher\bin\start-bge-rerank.ps1
_LLAMA_RERANK_BASE = "http://127.0.0.1:18098"
_RERANK_MODEL = "bge-reranker-v2-m3"
# 文档侧截断长度：服务端 ctx-size=1024，中文最坏 1 字符≈1 token，留足余量。
_RERANK_DOC_CHARS = 512
# 健康缓存：重排服务挂掉后短时间内不再每次白等一个超时（同嵌入侧的 _LLAMA_DOWN_UNTIL）
_RERANK_DOWN_UNTIL = 0.0


def _cfg_int(key: str, default: int) -> int:
    try:
        from services.shared.config.loader import get_config
        v = ((get_config() or {}).get("retrieval", {}) or {}).get(key, default)
        return max(1, int(v))
    except Exception:
        return default


def _cfg_int0(key: str, default: int) -> int:
    """读一个**允许为 0** 的整数配置（0 通常表示"关闭"）。

    ⚠️ 为什么不能直接用 `_cfg_int`：它内部是 `max(1, int(v))` —— 它服务的键
    （池子大小、批量、条数）确实都"至少 1"。但拿它读 `0 = 关闭` 的开关，
    **默认值 0 会被静默改成 1**，于是"默认关闭"变成"默认打开"，
    而且**不报错**（2026-10-02 加 `expand_window` 时实测发现）。

    ⇒ 规则：**语义上可以取 0 的键，一律用这个读**；其余仍用 `_cfg_int`。
    """
    try:
        from services.shared.config.loader import get_config
        v = ((get_config() or {}).get("retrieval", {}) or {}).get(key, default)
        return max(0, int(v))
    except Exception:
        return default


def _cfg_str(key: str, default: str) -> str:
    try:
        from services.shared.config.loader import get_config
        v = ((get_config() or {}).get("retrieval", {}) or {}).get(key, default)
        return str(v) if v else default
    except Exception:
        return default


_FTS_READY_CACHE: Optional[bool] = None


def _fts_ready() -> bool:
    """FTS5 预筛表是否可用（进程内缓存一次）。

    缓存是必要的：`retrieve()` 每次调用都会问一次，而这是个 SQLite 查询。
    进程内缓存意味着"库在进程运行期间新建了 FTS 表"不会被察觉 —— 可以接受：
    MCP 每次启动都会走 `ensure_schema()` → `ensure_fts()`，所以正常路径下
    第一个 `retrieve()` 时表已经在了。
    """
    global _FTS_READY_CACHE
    if _FTS_READY_CACHE is None:
        try:
            from services.B3_header_engine import fts_available
            _FTS_READY_CACHE = bool(fts_available())
        except Exception:
            _FTS_READY_CACHE = False
    return _FTS_READY_CACHE


def _cfg_bool(key: str, default: bool) -> bool:
    """读 retrieval 段里的布尔开关。接受 true/false、1/0、yes/no 三种写法。"""
    try:
        from services.shared.config.loader import get_config
        v = ((get_config() or {}).get("retrieval", {}) or {}).get(key, default)
        if isinstance(v, bool):
            return v
        return str(v).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        return default


def _cfg_float(key: str, default: float) -> float:
    try:
        from services.shared.config.loader import get_config
        v = ((get_config() or {}).get("retrieval", {}) or {}).get(key, default)
        return float(v)
    except Exception:
        return default


def _candidate_text(r: dict) -> str:
    """候选条目用于生成向量的文本（摘要 + 核心实体），全流程唯一入口"""
    return ((r.get("summary") or "") + " " + " ".join(r.get("core_entities") or [])).strip()


def _llama_embed_batch(texts: list, timeout: float = 60.0) -> list:
    """调 llama.cpp 的 OpenAI 兼容批量接口 /v1/embeddings。

    实测：8 条 2444ms 是 Ollama 的量级；llama.cpp 单条 18~96ms，
    且批量一次请求即可拿回全部向量。

    Returns: 与 texts 等长的列表，失败位置为 None；整体不可用返回全 None。
    """
    global _LLAMA_DOWN_UNTIL
    texts = list(texts)
    if not texts:
        return []
    if time.time() < _LLAMA_DOWN_UNTIL:
        return [None] * len(texts)
    try:
        r = requests.post(
            f"{_cfg_str('embed_llama_base', _LLAMA_EMBED_BASE)}/v1/embeddings",
            json={"model": _EMBED_MODEL, "input": texts},
            timeout=timeout,
        )
        if r.status_code == 200:
            data = r.json().get("data") or []
            # OpenAI 格式带 index，按 index 归位以免顺序错乱
            out: list = [None] * len(texts)
            for i, item in enumerate(data):
                idx = item.get("index", i)
                if isinstance(idx, int) and 0 <= idx < len(texts):
                    out[idx] = item.get("embedding")
            if any(v is not None for v in out):
                return out
    except Exception:
        pass
    # 标记一段时间内不再尝试，让调用方直接走 hash 兜底
    _LLAMA_DOWN_UNTIL = time.time() + _LLAMA_RETRY_AFTER
    return [None] * len(texts)


def _fallback_embed(text: str) -> list[float]:
    """hash embedding fallback（384 维，确定性）"""
    dim = 384
    h = hashlib.sha256(text.encode()).digest()
    return [(h[i % 32] - 128) / 128.0 for i in range(dim)]


def _embed_text(text: str, use_cache: bool = True) -> list[float]:
    """文本→向量。llama.cpp bge-m3 → Ollama → hash 兜底。

    带线程安全缓存：同一个文本的向量只请求一次后端。
    实测（2026-09-21）：llama.cpp 热态 18ms，Ollama 2163ms，两者向量余弦 0.999988。
    """
    # 先查缓存
    if use_cache:
        with _EMBED_LOCK:
            if text in _EMBED_CACHE:
                return _EMBED_CACHE[text]

    # 唯一后端：llama.cpp（bge-m3，127.0.0.1:18099）
    # 原 Ollama 兜底已移除：它固定 2.1s，且已不随开机启动，留着只会在 llama
    # 挂掉时白等一个超时，最后还是落到 hash 兜底。
    vec = None
    got = _llama_embed_batch([text])
    if got:
        vec = got[0]
        if vec is not None and len(vec) != 1024:
            vec = None      # 维度不符宁可走兜底，也不要污染缓存

    if vec is not None:
        if use_cache:
            with _EMBED_LOCK:
                _EMBED_CACHE[text] = vec
        return vec

    # 最后兜底：hash embedding
    fb = _fallback_embed(text)
    if use_cache:
        with _EMBED_LOCK:
            _EMBED_CACHE[text] = fb
    return fb


def _cosine_sim(a: list[float], b: list[float]) -> float:
    """余弦相似度"""
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def _vec_norm(v: list) -> float:
    return math.sqrt(sum(x * x for x in v))


def _dot(a: list, b: list) -> float:
    """点积：sum(map(mul)) 比生成器表达式快约 3 倍（无 numpy 环境下的主力）"""
    return sum(map(_mul, a, b))


# ---------- 进程内候选向量缓存（文本感知 + LRU） ----------
# A4 根因：原键只用 memory_id，重提炼后 summary/core_entities 变了但进程内还拿旧向量。
# 现在键是 (memory_id, text_hash)，命中时比对 hash，不一致自动重算。
# 同时加 LRU 上限：10 万行常驻 ≈ 400MB，5000 是 Phase1 上限的 5 倍余量。

class _CandidateVecCache:
    """进程内候选向量缓存，带文本感知和 LRU 上限。

    文本感知：memory_id 是 date#entry_id 的 md5，重提炼不会变，
    但 summary/core_entities 会变（UPSERT 覆盖内容字段、保留运行时状态）。
    所以只按 memory_id 查缓存会返回旧向量 —— 这是 A4 的根因。
    """
    def __init__(self, max_size: int = _CANDIDATE_VEC_LRU_MAX):
        self._store: _OrderedDict[str, tuple[str, list]] = _OrderedDict()
        self._max = max_size
        self._lock = threading.Lock()

    def get(self, mid: str, expected_hash: str) -> list | None:
        """返回向量；若 mid 存在但 hash 不匹配，返回 None（让调用方重算）。"""
        with self._lock:
            got = self._store.get(mid)
            if got is None:
                return None
            stored_hash, vec = got
            if stored_hash != expected_hash:
                del self._store[mid]
                return None
            self._store.move_to_end(mid)
            return vec

    def put(self, mid: str, text_hash: str, vec: list) -> None:
        with self._lock:
            self._store[mid] = (text_hash, vec)
            self._store.move_to_end(mid)
            while len(self._store) > self._max:
                self._store.popitem(last=False)

    def invalidate(self, mid: str) -> None:
        with self._lock:
            self._store.pop(mid, None)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def size(self) -> int:
        with self._lock:
            return len(self._store)

# 全局实例 + 模长缓存（模长也按 memory_id 存，向量换了就一起清）
_candidate_vecs = _CandidateVecCache()
_VEC_NORMS: dict[str, float] = {}
_norms_lock = threading.Lock()


def _candidate_norm(mid: str, vec: list) -> float:
    with _norms_lock:
        n = _VEC_NORMS.get(mid)
        if n is None:
            n = _vec_norm(vec)
            _VEC_NORMS[mid] = n
        return n


def invalidate_candidate_cache(mid: str | None = None) -> None:
    """重提炼/重索引后主动清缓存，避免用旧向量。

    mid=None → 清空全部（warm_indexer.index_warm() 重跑后用）；
    mid='xxx' → 只清指定条目（某一行内容变了用）。
    A4 根因就是进程内缓存不感知文本变化，重提炼后还拿旧向量。
    """
    if mid is None:
        _candidate_vecs.clear()
        with _norms_lock:
            _VEC_NORMS.clear()
    else:
        _candidate_vecs.invalidate(mid)
        with _norms_lock:
            _VEC_NORMS.pop(mid, None)


def _embed_batch_with_retry(texts: list, depth: int = 3) -> list:
    """批量嵌入 + 失败二分重试。

    唯一后端 llama.cpp；整批失败或部分失败都用二分重试隔离坏条目。
    二分重试的由来：批量接口偶发整批返回 null（历史实测 771 条中曾有
    1 批 64 条失败，起因是一条 NaN 脏文本毒掉整批），直接兜底成 hash 向量
    会让这批记忆永远匹配不上，所以这里二分重试把坏条目隔离出来。
    """
    vecs = _llama_embed_batch(texts)
    if not any(v is None for v in vecs):
        return vecs
    if len(texts) == 1 or depth <= 0:
        return vecs
    mid = len(texts) // 2
    return (_embed_batch_with_retry(texts[:mid], depth - 1)
            + _embed_batch_with_retry(texts[mid:], depth - 1))


def _embed_candidates(candidates: list[dict], workers: Optional[int] = None,
                      batch_size: Optional[int] = None) -> dict:
    """为候选集补齐向量：进程内缓存(文本感知+LRU) → 落盘缓存 → llama 批量接口 → hash fallback

    2026-09-24 改：进程内缓存键从 memory_id 改成 (memory_id, text_hash)，
    命中时比对 hash，重提炼后 summary 变了会自动丢弃旧向量重算 —— 修 A4。

    Returns: 统计字典 {total, from_memory, from_cache, embedded, fallback, elapsed_ms}
    """
    stats = {"total": len(candidates), "from_memory": 0, "from_cache": 0,
             "embedded": 0, "fallback": 0, "elapsed_ms": 0.0}
    if not candidates:
        return stats

    t0 = time.time()
    use_cache = embed_cache.enabled()

    # 1) 进程内已算过的直接跳过（文本感知：hash 不匹配等于没缓存）
    todo = []           # [(memory_id, text, text_hash)]
    for r in candidates:
        mid = r.get("memory_id")
        if not mid:
            continue
        text = _candidate_text(r)
        text_hash = embed_cache.make_key(text)
        cached_vec = _candidate_vecs.get(mid, text_hash)
        if cached_vec is not None:
            stats["from_memory"] += 1
            continue
        # hash 不匹配 → 旧条目已被 _CandidateVecCache 内部删掉，模长也得清
        _VEC_NORMS.pop(mid, None)
        todo.append((mid, text, text_hash))

    # 2) 落盘缓存命中
    misses = []
    if todo:
        cached = embed_cache.get_vectors([t[2] for t in todo]) if use_cache else {}
        for mid, text, text_hash in todo:
            v = cached.get(text_hash)
            if v:
                _candidate_vecs.put(mid, text_hash, v)
                stats["from_cache"] += 1
            else:
                misses.append((mid, text, text_hash))

    # 3) 未命中：llama.cpp 原生批量接口，多批次并行
    if misses:
        bs = batch_size or _cfg_int("embed_batch", _EMBED_BATCH)
        wk = workers or _cfg_int("embed_workers", _EMBED_WORKERS)
        batches = [misses[i:i + bs] for i in range(0, len(misses), bs)]

        def _run_batch(b):
            return b, _embed_batch_with_retry([x[1] for x in b])

        with _futures.ThreadPoolExecutor(max_workers=max(1, min(wk, len(batches)))) as ex:
            for b, vecs in ex.map(_run_batch, batches):
                to_store = []
                for (mid, text, text_hash), v in zip(b, vecs):
                    if v:
                        _candidate_vecs.put(mid, text_hash, v)
                        to_store.append((text_hash, v))
                        stats["embedded"] += 1
                    else:
                        # llama.cpp 不可用 → 384 维 hash 兜底（phase2 会做维度一致性处理）
                        _candidate_vecs.put(mid, text_hash, _fallback_embed(text))
                        stats["fallback"] += 1
                if to_store and use_cache:
                    try:
                        embed_cache.put_vectors(to_store)
                    except Exception:
                        pass  # 缓存写失败不影响检索

    stats["elapsed_ms"] = round((time.time() - t0) * 1000, 1)
    return stats


# ---------- Phase0: Query 预处理（实体 + task_type 提取） ----------
# 2026-09-24 加：Phase1 之前用 jieba + 规则从 query 里抽 core_entities 和 task_type。
# 原因：Phase1 没过滤时返回全库 433 条（10 万行时是 LIMIT 1000），Phase2 要精排太多。
# 这不是让小模型做语义筛选（A/B 实验否掉的路径 B），而是让 SQLite WHERE 先筛一层。
# 确定性规则 + jieba 分词，零 LLM 开销，延迟 <5ms。

_STOPWORDS = frozenset([
    "的", "了", "在", "是", "我", "有", "和", "就", "不", "人", "都", "一", "一个",
    "上", "也", "很", "到", "说", "要", "去", "你", "会", "着", "没有", "看", "好",
    "自己", "这", "他", "她", "它", "那", "这个", "那个", "这些", "那些", "什么",
    "怎么", "为什么", "哪里", "谁", "几", "多少", "可以", "能", "想", "知道", "觉得",
    "问题", "事情", "东西", "时候", "地方", "方法", "办法", "现在", "以后", "之前",
    "刚才", "刚刚", "已经", "还", "但", "而", "或", "与", "及", "并且", "如果", "因为",
    "所以", "但是", "然而", "虽然", "尽管", "不过", "只是", "而且", "或者", "还是",
    "呢", "吧", "吗", "啊", "哦", "呀", "啦", "哈", "嘛", "唉", "喂",
    "把", "让", "给", "用", "做", "写", "读", "看", "想", "问", "答",
    "我们", "你们", "他们", "咱们", "大家", "自己", "别人", "他人",
    "一下", "一点", "一些", "一样", "一起", "非常", "特别", "比较", "更", "最",
    "第一", "第二", "第三", "最后", "开始", "结束",
])

_PRONOUN_STARTS = ("你", "我", "他", "她", "它", "那么", "这个", "那个", "这些", "那些",
                   "我们", "你们", "他们", "咱们")
_VERB_WORDS = ("把", "让", "做", "写", "看", "想", "给", "用", "去", "来", "说", "读", "问", "答")
_QUESTION_ENDINGS = ("吗", "呢", "吧", "么", "了", "啊", "哦", "呀", "啦")

# task_type 关键词映射（确定性规则，不调 LLM）
_TASK_TYPE_KEYWORDS: dict[str, list[str]] = {
    "决策": ["决定", "选", "选择", "方案", "怎么办", "应该", "建议", "好还是", "权衡", "利弊", "决策", "怎么做"],
    "规划": ["计划", "打算", "准备", "要做", "设计", "架构", "蓝图", "路线", "规划", "方案", "先计划", "工作流"],
    "验证": ["测试", "验证", "证明", "确认", "对不对", "是不是", "检验", "核实", "verify", "test"],
    "研究": ["研究", "探索", "学习", "了解", "分析", "调查", "资料", "文献", "research", "原理"],
    "闲聊": ["聊", "说说", "谈谈", "好玩", "有趣", "哈哈", "有意思", "闲聊"],
    "求助": ["帮", "帮忙", "求助", "求救", "解决", "bug", "报错", "error", "失败", "出错"],
    "事实": ["是什么", "是啥", "什么是", "定义", "意思是", "简称", "全称"],
}

# jieba 用户词典：防止专有名词被拆
_JIEBA_USER_DICT = [
    "九出十二归",
    "星海旅人的独白",
    "书房OS",
    "书房 OS",
    "bge-m3",
    "heat count",
    "heat",
    "heat/50",
    "retrieval_hit",
    "core_entities",
    "task_type",
    "time_bucket",
    "phase1",
    "phase2",
    "Phase1",
    "Phase2",
    "B4",
    "B5",
    "B6",
    "B7",
    "MAP",
    "MAP 协议",
    "小模型",
    "agent",
    "Agent",
]


def extract_query_entities(query: str, max_ents: int = 8) -> list[str]:
    """从用户 query 里抽实体候选（jieba 分词 + 规则过滤）。

    为什么不用小模型：这是规则匹配，确定性、<5ms、零 token。
    小模型做同样的事（路径 B 第一步）要 30s+，而且效果更差（A/B benchmark 已验证）。

    Returns: 实体列表（可能为空；空 = Phase1 不做 entity 过滤）
    """
    if not query or len(query.strip()) < 2:
        return []

    try:
        import jieba
        # 用户词典：防止专有名词被拆（只 load 一次，jieba 内部有缓存）
        for word in _JIEBA_USER_DICT:
            jieba.add_word(word)
        tokens = list(jieba.cut(query))
    except Exception:
        # jieba 不可用时退化成按字符切（极罕见）
        tokens = [query[i:i+2] for i in range(max(0, len(query)-1))]

    ents: list[str] = []
    for tok in tokens:
        t = tok.strip()
        if not t or len(t) < 2:           # 单字/空
            continue
        if t in _STOPWORDS:                # 停用词
            continue
        if t.isdigit():                    # 纯数字
            continue
        if t in _VERB_WORDS:               # 动词
            continue
        if t.startswith(_PRONOUN_STARTS):  # 代词开头
            continue
        if t.endswith(_QUESTION_ENDINGS): # 问句结尾
            continue
        if t not in ents:                  # 去重
            ents.append(t)

        if len(ents) >= max_ents:
            break

    return ents


def extract_task_type(query: str) -> str | None:
    """从 query 里猜 task_type（关键词匹配）。

    7 种 task_type 有限，关键词覆盖够了。猜不出来返回 None（不覆盖显式传入的 filters）。
    """
    if not query or len(query.strip()) < 2:
        return None

    scores: dict[str, int] = {}
    for ttype, keywords in _TASK_TYPE_KEYWORDS.items():
        for kw in keywords:
            if kw in query:
                scores[ttype] = scores.get(ttype, 0) + 1

    if not scores:
        return None

    best = max(scores, key=scores.get)  # type: ignore[arg-type]
    return best


# ---------- Phase1 ----------

def phase1_filter(
    task_types: Optional[list] = None,
    time_buckets: Optional[list] = None,
    layer: Optional[str] = None,
    core_entity_any: Optional[list] = None,
    limit: int = 1000,
    session_ids: Optional[list] = None,
    user_ids: Optional[list] = None,
) -> list[dict]:
    """Phase1：调用 B3 的 SQLite WHERE 过滤"""
    from services.B3_header_engine import phase1_filter as _b3_phase1
    return _b3_phase1(
        task_types=task_types,
        time_buckets=time_buckets,
        layer=layer,
        core_entity_any=core_entity_any,
        limit=limit,
        session_ids=session_ids,
        user_ids=user_ids,
    )


# ---------- Phase2 ----------

def phase2_rank(query_text: str, candidates: list[dict],
                top_k: int = _TOPK_PHASE2, stats: Optional[dict] = None) -> list[dict]:
    """Phase2：候选集内精排（纯向量相似度 + heat/count 加权）

    score = sim + α·heat + β·count

    Args:
        query_text: 用户原始 query（用于生成嵌入）
        candidates: Phase1 输出（最多百级）
        top_k: Phase2 取前 N，默认 20

    Returns: 按最终 score 排序的 top_k 条目
    """
    if not candidates:
        return []

    # 生成 query 向量（bge-m3）
    query_vec = _embed_text(query_text)
    query_norm = _vec_norm(query_vec)
    query_fallback = None   # 懒算：只在遇到维度不一致时才用

    # 给每条候选打分
    scored = []
    degraded = 0
    for r in candidates:
        mid = r["memory_id"]
        text = _candidate_text(r)
        text_hash = embed_cache.make_key(text)
        cand_vec = _candidate_vecs.get(mid, text_hash)

        degraded_row = False
        if cand_vec is not None and len(cand_vec) == len(query_vec):
            nb = _candidate_norm(mid, cand_vec)
            sim = (_dot(query_vec, cand_vec) / (query_norm * nb)) if (query_norm and nb) else 0.0
        else:
            # 维度不一致（典型：query 走 bge-m3 1024 维，候选只有 384 维 hash 兜底）。
            # 旧实现直接判 0 分，会被 _MIN_SIM_THRESHOLD 全量过滤 → 表现为"检索无结果"。
            # 现在用同一套 hash 编码重算 query，保证可比，而不是静默丢弃。
            if query_fallback is None:
                query_fallback = _fallback_embed(query_text)
            fb = _fallback_embed(_candidate_text(r))
            na, nb = _vec_norm(query_fallback), _vec_norm(fb)
            sim = (_dot(query_fallback, fb) / (na * nb)) if (na and nb) else 0.0
            degraded_row = True
            degraded += 1

        heat = r.get("heat", 0.0)
        count = r.get("count", 0)

        # score = sim + α·heat + β·count（heat/count 分别归一到 [0,1]）
        score = sim + _ALPHA * min(1.0, heat / _HEAT_NORM) + _BETA * min(1.0, count / _COUNT_NORM)

        row = dict(r)
        row["_sim"] = round(sim, 4)
        row["_score"] = round(score, 4)
        if degraded_row:
            row["_degraded"] = True   # 该条走的是 hash 兜底比对
        scored.append(row)

    scored.sort(key=lambda x: x["_score"], reverse=True)
    # 最低相似度阈值：过滤掉"完全不相关"条目（硬规则 2 支撑）
    threshold = _min_sim_threshold()
    meaningful = [s for s in scored if s["_sim"] >= threshold]
    # ★★ 参赛版护栏（2026-10-03）：**阈值不许把候选清空**。
    #
    # 为什么必须有：嵌入服务（bge-m3 @18099）不可达时会落 **hash 兜底**，
    # 那时余弦全是噪声（≈0），于是 `threshold=0.15` 把**每一条**都切掉 ⇒
    # `phase2_rank` 返回空 ⇒ `fuse_routes` 因为 `ranked` 为空**整段跳过** ⇒
    # **连词法路/实体路/时间路已经捞到的结果也一起没了**，
    # 服务会对**每一个查询**返回 `{"data": []}`（线上表现 = 分数趋近 0）。
    #
    # 实测（2026-10-03，18099 与 18098 都拒绝连接）：
    #   自检里 bob 的查询 n=0，而**同一时刻** `phase1 / fts / raw` 各有 2 条命中。
    #   这正是本仓最怕的那类故障 —— "检索没找到"与"找到了但被阈值砍了"
    #   在日志和响应上长得一模一样，只在分数上差 100 分。
    #
    # 语义：**阈值只用来降噪，不许用来清零**。真的全被切掉时退回未过滤的 top_k，
    # 并如实记 `threshold_fallback`（不静默）。
    threshold_fallback = False
    if not meaningful and scored:
        threshold_fallback = True
        meaningful = scored
    if stats is not None:
        stats["degraded"] = degraded
        stats["above_threshold"] = sum(1 for s in scored if s["_sim"] >= threshold)
        stats["min_sim_threshold"] = threshold
        stats["threshold_fallback"] = threshold_fallback
    return meaningful[:top_k]


def mmr_select(items: list[dict], k: int, lam: float = 0.7,
               pool: int = 60, stats: Optional[dict] = None) -> list[dict]:
    """MMR（最大边际相关）多样性挑选 —— 第 3 项「cat3 开放域聚合」的第一把刀。

    为什么需要
    ----------
    开放域问题（"他们俩有什么共同爱好？"）的答案**散在多条记忆里**，
    而纯按 score 排序的 top-K 很容易被同一天/同一场会话的近似内容占满 ——
    交付的 15 条其实只覆盖了 2~3 件事，聚合就无从谈起。
    MMR 的判据是「相关性 − 与已选条目的最大相似度」：

        pick = argmax  λ·rel(x) − (1−λ)·max_{s∈selected} cos(x, s)

    λ=1 退化成纯按相关性排（=旧行为）；λ 越小越偏向多样。

    成本控制
    --------
    只在相关性前 `pool` 条里挑（默认 60）。MMR 是 O(k·pool) 次点积，
    k=15 / pool=60 / 1024 维实测约 20ms —— 不这么限，1,500 个候选就是秒级。
    **注意 `phase2_rank` 的排序本身已经算完**，所以把它的返回条数从 20 提到 60
    不额外花钱（只有截断变了），这也是 pool 能开到 60 的原因。

    ⚠️ **实测结论：在本项目的数据上 MMR 是负收益，默认关闭（λ=1.0）。**
    LoCoMo 阶段二（5,871 行 / 全量 1,986 题）实测：

        λ=1.0（关）  全量 52.3%   cat3 开放域 35.4%   cat4 单跳 53.2%  中位 112ms
        λ=0.6        全量 41.7%   cat3 29.2%（−6.2）  cat4 41.3%（−11.9）中位 213ms
        λ=0.3        全量 32.0%   cat3 28.1%（−7.3）  cat4 30.1%（−23.1）中位 211ms

    原因是**前提不成立**：诊断显示 cat3 交付的 top-15 已经跨 8.6 个不同会话（上限 15），
    冗余度本来就不高 —— 瓶颈是"压根没捞到"（召回），不是"捞到了但太重复"。
    于是多样化只是在拿相关性换一个不需要的东西。
    保留这段代码是因为它对"多样性敏感"的场景仍可能有用，但**别默认开**。

    向量拿不到时（缓存未命中且拿不到文本）该条按"不相似"处理（div=0），
    即退化成纯相关性 —— 宁可少多样，不可因此丢掉相关性最高的一条。
    """
    if lam >= 1.0 or len(items) <= 1:
        return items[:k]
    cand = items[:max(pool, k)]
    vecs: dict = {}
    for it in cand:
        mid = it.get("memory_id")
        if not mid:
            continue
        try:
            v = _candidate_vecs.get(mid, embed_cache.make_key(_candidate_text(it)))
        except Exception:
            v = None
        if v:
            vecs[mid] = v

    def _sim(a: str, b: str) -> float:
        va, vb = vecs.get(a), vecs.get(b)
        if not va or not vb:
            return 0.0
        na, nb = _candidate_norm(a, va), _candidate_norm(b, vb)
        if not na or not nb:
            return 0.0
        return _dot(va, vb) / (na * nb)

    selected: list[dict] = []
    remaining = list(cand)
    while remaining and len(selected) < k:
        best, best_val = None, None
        for it in remaining:
            mid = it.get("memory_id")
            rel = float(it.get("_score") or 0.0)
            div = max((_sim(mid, s.get("memory_id")) for s in selected), default=0.0)
            val = lam * rel - (1.0 - lam) * div
            if best_val is None or val > best_val:
                best, best_val = it, val
        selected.append(best)
        remaining.remove(best)
    if stats is not None:
        stats["mmr_lambda"] = lam
        stats["mmr_pool"] = len(cand)
        stats["mmr_selected"] = len(selected)
    return selected


# ---------- 预算裁剪 ----------

def budget_clip(items: list[dict],
                max_tokens: int = 5000,
                max_items: int = 10) -> dict:
    """P1-2 预算裁剪

    Args:
        items: Phase2 输出
        max_tokens: 总 token 硬上限（默认 5000，L1 摘要档）
        max_items: 条目数硬上限（默认 10）

    Returns: {
        "items": [...], "truncated": bool,
        "tokens_used": int, "items_kept": int
    }
    """
    # 粗估 token：字符数 / 2.5（中文大约）
    kept = []
    tokens_used = 0
    truncated = False

    for item in items:
        summary = item.get("summary", "") or ""
        est_tokens = len(summary) / 2.5
        if tokens_used + est_tokens > max_tokens or len(kept) >= max_items:
            truncated = True
            break
        kept.append(item)
        tokens_used += est_tokens

    return {
        "items": kept,
        "truncated": truncated,
        "tokens_used": round(tokens_used),
        "items_kept": len(kept),
    }


# ---------- Cross-encoder 重排序（第 4 项）----------
#
# 为什么必须是 cross-encoder，而不是"再算一遍向量"
# ------------------------------------------------
# 天花板曲线实测（locomo-test/stage2_oracle_curve.py，n=1,982，5,871 行库、只读）：
#   cat3 开放域   证据∈候选集 90.2%   @15 37.0%  @50 58.7%  @100 67.4%  @200 77.2%
# 即：**证据早就在候选集里，只是被排在第 15~50 名**。
# bi-encoder（bge-m3 各自独立编码再算余弦）结构上修不了这件事 —— 它拿不到
# "这两段东西在回答同一个问题"的信息；cross-encoder 把 query+doc 拼起来过一遍
# 模型，天生就是干这个的。
#
# 顺带解释两个已确认的负结果：MMR（多样性）与 PRF（实体扩候选）对 cat3 都无效
# （MMR 全量 −10.6 点、PRF cat3 +0.0 点），因为**它们动的是候选集构成，
# 而 cat3 缺的是排序深度**。补再多候选，排在第 20 名之后一样进不了 top-15。
#
# 本机实测的 logit 分离度（bge-reranker-v2-m3-Q8_0，GPU 全量）：
#   "Joanna said she loves hiking"        -> -3.76   相关
#   "Nate mentioned he enjoys hiking"     -> -4.52   相关
#   "The weather was rainy last Tuesday"  -> -11.04  无关
#   "software architecture for memory"    -> -11.04  无关
# 相关与无关相差约 7 个 logit。分数是**原始 logit，不是 0~1 概率**，只用于排序。
#
# 延迟（热态实测）：30 条 88ms / 50 条 137ms / 100 条 259ms（约 2.6ms/条）。
# 所以 `rerank_pool` 默认 **50**：能吃下 @50 那段最大的余量（cat3 @15→@50 有 +21.7 点），
# 总延迟 101+137 ≈ 238ms，仍在「<300ms」验收线内；调到 100 会到 ~360ms，超线。
#
# ⚠️ 三条硬要求（本模块老规矩：旁路增强永远不许拖垮检索）
#   1. **fail-open**：服务不可用/超时/返回残缺 → 保持原序返回，绝不抛异常；
#   2. **默认关**：`retrieval.rerank_enabled`，先测量再开；
#   3. **不覆盖 `_score`**：重排分数另存 `_rerank_score`。`_score` 是 heat/count
#      加权后的相关性语义，审计与对照实验都依赖它，不能被重排悄悄改掉。

def _rerank_scores(query_text: str, docs: list, timeout: float = 20.0):
    """调 llama.cpp 的 /v1/rerank，返回与 docs 等长的 score 列表。

    服务端响应形如::

        {"results": [{"index": 0, "relevance_score": -3.76}, ...]}

    失败（不可用/超时/条数对不上/字段缺失）一律返回 ``None``，
    由调用方决定退化行为 —— **本函数不抛异常**。

    ⚠️⚠️ 2026-10-01 **加围栏**（原实现是"静默失败 + 熔断级联"，最难排查的那一类）
    ----------------------------------------------------------------------
    实测这个重排器有**两层长度限制**，而原实现一处都不管：

      * **单篇**：1536 字符 OK · **2048 字符失败**  → 现按 `_RERANK_DOC_MAX` 截断；
      * **单批总长**：64 篇 × 1200 = 7.7 万字符**也失败**
        （只按"条数"分块不够）→ 现按 `_RERANK_BATCH_CHARS` **累计字符**分批。

    而失败一旦发生会**熔断** `_RERANK_DOWN_UNTIL`，后续调用**秒返 None** →
    **级联**，外部看起来像"重排完全不能用"。原实现只报一个笼统的
    `unavailable_or_incomplete`，于是：
      * 不知道是**没起服务**、**文档太长**、**批次太大**，还是**熔断中**；
      * 一条超长摘要（`rerank_doc='auto'` 用 `summary`，**没有长度上限**）
        就能把整个重排**静默停摆 30 秒**，而重排是第 5 步、fail-open 不报错。

    现在**每一种失败路径都留痕**（`_RERANK_LAST` + 限频 stderr 告警），
    并可通过 `_rerank_status()` 查。**"没有反馈"本身就是 bug** —— 这条是作者要求的。
    """
    global _RERANK_DOWN_UNTIL, _RERANK_LAST, _RERANK_WARNED_AT
    docs = list(docs)
    if not docs:
        return []
    # ① 兜底：单篇截断（并记账，让"截过"这件事可见）
    n_trunc = 0
    fixed = []
    for d in docs:
        t = '' if d is None else str(d)
        if len(t) > _RERANK_DOC_MAX:
            n_trunc += 1
            t = t[:_RERANK_DOC_MAX]
        fixed.append(t)
    # ② 兜底：按**累计字符**分批（不是按条数）
    batches, buf, blen = [], [], 0
    for t in fixed:
        if buf and blen + len(t) > _RERANK_BATCH_CHARS:
            batches.append(buf)
            buf, blen = [], 0
        buf.append(t)
        blen += len(t)
    if buf:
        batches.append(buf)
    if time.time() < _RERANK_DOWN_UNTIL:
        _rerank_note('breaker_active', f'熔断中，还剩 '
                     f'{_RERANK_DOWN_UNTIL - time.time():.0f}s')
        return None
    out: list = [None] * len(fixed)
    for bi, chunk in enumerate(batches):
        off = sum(len(b) for b in batches[:bi])
        try:
            r = requests.post(
                f"{_cfg_str('rerank_base', _LLAMA_RERANK_BASE)}/v1/rerank",
                json={"model": _cfg_str('rerank_model', _RERANK_MODEL),
                      "query": query_text, "documents": chunk},
                timeout=timeout,
            )
            if r.status_code != 200:
                _RERANK_DOWN_UNTIL = time.time() + _LLAMA_RETRY_AFTER
                _rerank_note('http_%d' % r.status_code,
                             f'第 {bi + 1}/{len(batches)} 批 · '
                             f'该批 {len(chunk)} 篇 {sum(len(x) for x in chunk)} 字符')
                return None
            got = 0
            for it in (r.json().get("results") or []):
                i = it.get("index")
                s = it.get("relevance_score", it.get("score"))
                if isinstance(i, int) and 0 <= i < len(chunk) and s is not None:
                    out[off + i] = float(s)
                    got += 1
            if got != len(chunk):
                # 只要有一条没拿到分就整体放弃：宁可保持原序，也不要"半重排"出
                # 一个分数缺失、顺序自相矛盾的结果（那比起不重排更难排查）。
                _rerank_note('missing_scores',
                             f'第 {bi + 1}/{len(batches)} 批 · 只拿到 '
                             f'{got}/{len(chunk)} 个分')
                return None
        except Exception as e:              # noqa: BLE001 - 旁路增强失败绝不影响检索
            _RERANK_DOWN_UNTIL = time.time() + _LLAMA_RETRY_AFTER
            _rerank_note(f'exception:{type(e).__name__}', str(e)[:120])
            return None
    _RERANK_LAST.update({'ok': True, 'reason': 'ok', 'detail': '', 'ts': time.time(),
                         'n': len(fixed), 'batches': len(batches),
                         'truncated': n_trunc})
    if n_trunc:
        # 截断过就要说 —— 它意味着"重排看到的内容比实际少"，属可预期但必须知情。
        # ⚠️ 这里必须**合并**而不是覆盖 `_RERANK_LAST`（第一版就是覆盖，
        #    于是 `truncated` 计数被冲掉，断言 ①b 抓到）。
        _rerank_note('truncated_docs', f'{n_trunc}/{len(fixed)} 篇超过 '
                     f'{_RERANK_DOC_MAX} 字符被截断', warn=False, ok=True)
    return out if all(v is not None for v in out) else None


_RERANK_DOC_MAX = 1024        # 单篇上限（实测 1536 OK / 2048 失败，留余量）
_RERANK_BATCH_CHARS = 8000    # 单次请求总字符上限（实测 7.7 万会失败）
_RERANK_WARNED_AT = 0.0
_RERANK_LAST: dict = {'ok': None, 'reason': 'never_called', 'ts': 0.0,
                      'fails': 0}


def _rerank_note(reason: str, detail: str = '', warn: bool = True,
                 ok: bool = False) -> None:
    """记一次重排异常/截断，并**限频**告警（默认 60s 最多一条）。

    为什么要限频：检索是每次提问都跑的，不限频会把日志淹掉 ——
    但**完全不报也不行**（这正是原来的毛病）。所以：**记账总是做，告警限频**。

    ⚠️ 必须 **`update` 合并**，不能整个替换 `_RERANK_LAST` ——
    否则会把成功路径刚写进去的 `truncated` / `batches` 冲掉（踩过）。
    """
    global _RERANK_WARNED_AT
    _RERANK_LAST.update({'ok': ok, 'reason': reason, 'detail': detail,
                         'ts': time.time(),
                         'fails': _RERANK_LAST.get('fails', 0) + (0 if ok else 1)})
    if warn and time.time() - _RERANK_WARNED_AT > 60:
        _RERANK_WARNED_AT = time.time()
        print(f'[B4] rerank failed ({reason}): {detail} -- '
              f'delivering in original order (fail-open), '
              f'total failures {_RERANK_LAST["fails"]}', file=sys.stderr)


def _rerank_status() -> dict:
    """重排的健康状态 —— 供诊断/健康检查查（**不许只靠日志**）。"""
    return {**_RERANK_LAST,
            'breaker_active': time.time() < _RERANK_DOWN_UNTIL,
            'breaker_until': _RERANK_DOWN_UNTIL,
            'doc_max': _RERANK_DOC_MAX,
            'batch_chars': _RERANK_BATCH_CHARS}


def _rerank_docs(head: list, doc_source: str = 'auto') -> list:
    """重排这一环该拿**哪份文本**去评（V1.3 增量 1，2026-09-30）。

    * `auto`（**生产默认**）：`_candidate_text` = `summary + core_entities`。
      生产里**摘要跟随用户/源语言**，所以它天然与查询同语言 —— 不需要额外处理。
    * `raw`：**原文**（B3 `memories_fts_raw.head`，索引时存好的前 512 字符）。
      给"摘要语言与源语言不一致"的语料用 —— 本仓的 LoCoMo 评测库正是如此
      （摘要中文 / 原文英文 / 查询英文）。

    为什么必须留这个开关，而不是统一改成 `raw`：
    实测「融合 top30 → cross-encoder」这一环，**读摘要是 72.0%、读原文是 81.8%（+9.8）**，
    而**按现状读摘要时它比"不重排"（75.6%）还低 3.6** —— 即病根是
    "候选靠英文原文入选、CE 却只拿到中文摘要"，**重排与索引看的不是同一份文本**。
    生产上 `auto` 已经是对的（摘要同语言）；评测上不用 `raw` 会把成绩**量低 9.8 点**。
    **两种语料形态，各用各的正确口径。**

    ⚠️ `raw` 走的是索引时存好的 `head`，**不在热路径上现读冷库** ——
    重排池 30 条/查询，现读就是每查询 30 次扫盘。
    """
    if doc_source == 'raw':
        try:
            from services.B3_header_engine import raw_heads
            heads = raw_heads([it.get('memory_id') for it in head])
        except Exception:                      # noqa: BLE001 - 取不到就整体回退
            heads = {}
    else:
        heads = {}
    return [((heads.get(it.get('memory_id')) or _candidate_text(it) or '')
             [:_RERANK_DOC_CHARS]) for it in head]


def rerank_top(query_text: str, ranked: list, pool: int,
               stats: Optional[dict] = None, doc_source: str = 'auto') -> list:
    """用 cross-encoder 重排 ``ranked[:pool]``，返回整体重排后的列表。

    尾部（``ranked[pool:]``）保持原序接在后面 —— 后续 `budget_clip` 是**保序截断**，
    所以交付顺序就等于这里的顺序。

    fail-open：拿不到分数就**原样返回**（顺序与长度都不变）。
    """
    if not ranked:
        return ranked
    pool = max(1, min(int(pool), len(ranked)))
    head = ranked[:pool]
    docs = _rerank_docs(head, doc_source)
    _t0 = time.time()
    scores = _rerank_scores(query_text, docs)
    _ms = round((time.time() - _t0) * 1000, 1)
    if scores is None:
        if stats is not None:
            # ★ 不再只报笼统的 `unavailable_or_incomplete` —— 把**具体原因**带出去
            #   （`http_500` / `missing_scores` / `truncated` / `breaker_active` …），
            #   否则"重排没生效"这件事在诊断时无从下手（作者 2026-10-01 要求）。
            _st = _rerank_status()
            stats["rerank"] = {"applied": False, "pool": pool, "ms": _ms,
                               "reason": _st.get('reason', 'unavailable_or_incomplete'),
                               "detail": _st.get('detail', ''),
                               "breaker_active": _st.get('breaker_active'),
                               "n_fails": _st.get('fails')}
        return ranked
    # 先按重排分定名次，再把名次归一到 [0,1]，**作为原公式里 `sim` 项的替身**。
    #
    # ⚠️ 为什么不直接按重排分排序（2026-09-26 改）
    # ------------------------------------------------
    # 直接按重排分排序会让 `score = sim + α·heat + β·count` 里的 **α·heat / β·count
    # 两项彻底失效** —— 重排只看 (query, doc) 内容相关度，完全不看热度。
    # 这不是"精度微调"，而是**把 B5 热度闭环（"常用浮现"）静默删掉了**：
    # 一条被反复想起 20 次的记忆，不会因为"常用"而排上来。
    # 实测证据：`bin/verify-heat-ranking.py` 直接失败 ——
    #   「升温后排名上升 (8 -> 8)」（加热 8 次，名次一动不动）。
    # 那条断言存在的意义就是"热度必须能改变交付顺序"，所以这里不能靠改测试蒙过去。
    #
    # 改法：**让 cross-encoder 替换 `sim` 这一项，而不是替换整个 score**。
    #   * 旧: score = cosine(query, doc) + α·heat + β·count
    #   * 新: score = rank_norm(rerank_logit) + α·heat + β·count
    # α/β/_HEAT_NORM/_COUNT_NORM 全部沿用原值，所以热度语义没变，只是相关性那一项
    # 换成了更强的信号。
    #
    # 为什么用**名次**归一而不是 min-max 归一 logit：min-max 是池内相对量，
    # 当池子里所有条目都不相关（logit 全挤在一起）时，它会把噪声放大到整个 [0,1]，
    # 于是 α·heat 相对变得无足轻重。
    #
    # ⚠️ 而名次要摊到**多宽**，是这条路唯一需要定的参数，且定错过一次：
    # 第一版摊到 [0,1]，池 30 时每档步长 1/29≈0.0345，而 heat=9 的加成只有
    # `0.1×(9/50)=0.018` —— **跨不过一格**，于是 `verify-heat-ranking` 仍然报
    # 「升温后排名上升 (8 -> 8)」。
    # 正确做法是摊到**池内 `_sim` 的实际跨度**：重排决定顺序，但相关性那一项的
    # 量纲与它替换掉的 `sim` 保持一致，于是 α/β/_HEAT_NORM/_COUNT_NORM 的权重语义
    # **原样保留**（架构书那条「热度最多能反超 0.20 的相似度差」的不变式也才继续成立）。
    order = sorted(range(pool), key=lambda i: scores[i], reverse=True)
    rank_of = {idx: r for r, idx in enumerate(order)}
    denom = max(1, pool - 1)
    sims = [float(head[i].get("_sim") or 0.0) for i in range(pool)]
    span = max(sims) - min(sims)
    if span <= 0.01:
        # 池内相似度确实不分高下：此时相关性排序本就没什么信息量，让热度主导是合理的。
        # 给个下限只是防止 span=0 时步长退化成 0（全体同分）。
        span = 0.01
    out = []
    for i in range(pool):
        it = head[i]
        rel = span * (1.0 - rank_of[i] / denom)   # 名次 → 与 `sim` 同量纲，越大越好
        heat = float(it.get("heat") or 0.0)
        cnt = float(it.get("count") or 0.0)
        it["_rerank_score"] = round(scores[i], 4)
        it["_final_score"] = round(
            rel + _ALPHA * min(1.0, heat / _HEAT_NORM)
                + _BETA * min(1.0, cnt / _COUNT_NORM), 6)
        out.append(it)
    # `_score` 保持 Phase2 原值不动（审计/对照实验依赖它）；
    # 这里的稳定排序保证同分者维持 Phase2 原序。
    out.sort(key=lambda x: x["_final_score"], reverse=True)
    blend_moved = sum(1 for pos, it in enumerate(out)
                      if it is not head[order[pos]])
    out.extend(ranked[pool:])
    if stats is not None:
        stats["rerank"] = {"applied": True, "pool": pool, "ms": _ms,
                           "reason": "ok",
                           "truncated": _rerank_status().get('truncated', 0),
                           "batches": _rerank_status().get('batches'),
                           "best": round(scores[order[0]], 3),
                           "worst": round(scores[order[-1]], 3),
                           "top1_changed": order[0] != 0,
                           "blend_moved": blend_moved}
    return out


# ---------- V1.3 增量 1：四路召回 + RRF 融合（2026-09-30）----------
#
# 架构图 §四 增量 1：
#     路1 语义（bge-m3，已有） ┐
#     路2 实体（core_entities） ├→ RRF k=60 等权 → top-30 → cross-encoder → top-15
#     路3 时间（source_date）   │
#     路4 词法（**原文** FTS5）  ┘
#
# 为什么路4 索引的是**原文**而不是现有 `memories_fts`（摘要）：
# 实测 `locomo-test/_route2_lexical_verify.txt`，1974 题 —— 索引摘要 11.9%、
# 索引原文 **63.9%**，差 5 倍。根因是**语言错配**（查英文 / 摘要中文），
# 不是分词器。所以规则是「**索引原文实际说的语言**」。
#
# 实测结论（`_route4_fusion_verify.txt` / `_route5b_rerank_doc_verify.txt`）：
#   * 四路等权融合 75.6% vs 最好单路 63.9%（+11.8，CI[+9.8,+13.7]，p=0.0000）
#   * **等权最优** —— 任何降权/加权都是负的（语义降权0.5 −2.5、词法加权2.0 −2.4）
#   * ★ **实体路的增益在词法路存在时被吸收**（留一法 −0.2，不显著）：
#     词法路用整篇原文、实体路只用 20 个实体名，前者是后者的超集。
#     保留它是因为它**不依赖原文**（原文取不到时的兜底）。
#   * 端到端 9B：F1 0.335 → 0.400（+0.065，p=0.0000，1986 题全量）
#
# ⚠️ 本块**默认关闭**（`retrieval.routes_enabled`），因为：
#   1. 它动的是主检索管道，必须先能一键对照回退；
#   2. 评测侧与生产侧要能各自选口径（见 `_rerank_docs` 的 `doc_source`）。

_ROUTE_CACHE: dict = {}
_LEGACY_WARNED = False           # `routes_legacy` 的告警只喊一次（见 retrieve）

# ---------- ★ 语言：在边界判**一次**，所有路读同一张策略表（V1.3.4 步骤 1）----------
#
# 为什么要有这一块（2026-10-01）：V1.3.3 里语言判断是**碎在各路里**的 ——
# `pick_lexical` 自己算一次 CJK 比，再打算给路2/路3 各加一套。那是"同一件事判多次"，
# 迟早不一致。IR 的标准做法是：**语言是文本的属性**，
#   * **文档语言**在**建索引时**定死（原文表=英 / 摘要表=中，B3 已经如此）；
#   * **查询语言**在**入口判一次**，作为标签往下传。
# 所以这里只留一个判断点 + 一张表；**各路不得自己再判语言**。
LANG_CJK_MIN = 0.15               # 中文查询里常混英文专名（"卡罗琳的 LGBTQ 小组"），故不取 1.0

# 语言相关的**唯一**决策处。含义：
#   True/False  → 该路在该语言下是否参与融合
#   'raw'/'summary' → 词法路打哪张索引表
# ⚠️ 初始值全部来自 V1.3.4 文档 §1.1 的**实测**，不是拍的：
#   entity 英文 −0.0441（关掉更好）/ 中文 +0.0369~+0.0494
#   time   中文 +0.0611 / 英文 +0.0323（**两语言都为正 → 不门控**）
#   lexical 中文打摘要表（打原文表命中 0/50）、英文打原文表
ROUTE_LANG = {
    'semantic': {'zh': True,      'en': True},
    'entity':   {'zh': True,      'en': False},
    'time':     {'zh': True,      'en': True},
    'lexical':  {'zh': 'summary', 'en': 'raw'},
}


def query_lang(text: str) -> str:
    """查询语言 —— **整个检索链里只在这里判一次**。纯函数、可复现。"""
    return 'zh' if _cjk_ratio(text) >= LANG_CJK_MIN else 'en'
_ROUTE_NAMES = ('semantic', 'entity', 'time', 'lexical')


def invalidate_route_cache() -> None:
    """丢掉实体路/时间路的进程内索引缓存。

    写入之后由调用方（或本模块的 `retrieve` 检测到写纪元变化）自动失效；
    需要手工失效的场景：**别的进程**直接改了 header.db。
    """
    _ROUTE_CACHE.clear()


def enabled_routes() -> set:
    """哪几路参与融合（`retrieval.routes`，逗号分隔）。

    ★ 为什么需要**按路开关**（2026-09-30）
    ------------------------------------
    实测四路里**只有两路在挣饭吃**（`_route4_fusion_verify.txt` 留一法）：

        去掉 词法   −12.6        去掉 语义  −4.9
        去掉 时间   −0.4（非显著） 去掉 实体  −0.2（非显著）

    原因是具体的、不是"玄学"：
    * **实体路的增益被词法路吸收** —— 词法路用**整篇原文**，实体路只用 20 个实体名，
      前者是后者的超集（LoCoMo 的实体表里也基本是人名）。
    * **时间路在这个语料上没有输入** —— 查询里抽得出可用时间窗口的只有 **12.9%**，
      其余 87% 交空表（英文问句里几乎没有绝对时间词；路线书写的 81.6% 复现不出来）。

    ⚠️ 但**生产与测试的结论可能不同**，所以这里做成**可配**而不是写死删掉：
    * **测试端**（LoCoMo）：原文 100% 可得 → 实体路纯冗余 → 只开 `semantic,lexical`。
    * **生产端**：`has_l3=0`（取不到原文）时**词法路整条没输入**，那时实体路是唯一
      的"结构化"信号；时间路对中文查询（"上个月做了什么"）也比英文语料有用得多。
      → 生产**先保留全部**，等真实中文流量量过再定。

    未知路名只告警、不报错（免得一个笔误让整条检索失效）。
    """
    raw = _cfg_str('routes', 'semantic,entity,time,lexical')
    want = {x.strip().lower() for x in (raw or '').split(',') if x.strip()}
    unknown = want - set(_ROUTE_NAMES)
    if unknown:
        print(f"[B4] ⚠️ retrieval.routes 里有未知路名 {sorted(unknown)}，已忽略"
              f"（可选 {list(_ROUTE_NAMES)}）", file=sys.stderr)
    got = want & set(_ROUTE_NAMES)
    return got or {'semantic'}          # 全写错时至少保住语义路，不能退化成"没有候选"


def _cjk_ratio(text: str) -> float:
    """文本里 CJK 字符占"非空白字符"的比例。判查询语言用。"""
    t = (text or '').strip()
    if not t:
        return 0.0
    nz = [c for c in t if not c.isspace()]
    if not nz:
        return 0.0
    return sum(1 for c in nz if '\u4e00' <= c <= '\u9fff') / len(nz)


def pick_lexical(query_text: str, legacy: bool = False,
                 table: Optional[str] = None):
    """选词法索引表，返回 `(搜索函数, 表名标签)`；不可用返回 `(None, 'none')`。

    `table`：显式指定 `'raw'`（原文表）/ `'summary'`（摘要+实体表）。
        V1.3.4 起**由 `ROUTE_LANG` 决定并传进来** —— 本函数不再自己判语言。
    `legacy=True`：还原 V1.3.3 修复前行为：一律打原文索引（中文查询于是命中 0）。
        与 `routes_legacy` 配套，供"修复前 vs 修复后"的同运行内配对使用。
    `table=None`：向后兼容 —— 按查询语言（= V1.3.3 修复后的行为）。
    """
    from services.B3_header_engine import raw_available, raw_search
    want = 'raw' if legacy else (table or ('summary' if _cjk_ratio(query_text) >= LANG_CJK_MIN
                                           else 'raw'))
    if want == 'summary':
        try:
            from services.B3_header_engine import fts_search
            return (fts_search, 'summary')
        except Exception:                      # noqa: BLE001
            return (None, 'none')
    return (raw_search, 'raw') if raw_available() else (None, 'none')


def _route_objects(user_ids: Optional[list] = None):
    """按 `(库行数, 写纪元, 用户作用域)` 缓存实体路/时间路的索引结构。

    为什么必须缓存：`EntityRoute.from_store()` 要扫全表 `core_entities`、
    `TimeRoute.from_store()` 要扫全表 `source_date` —— 每查询建一次就是
    **每查询一次全表扫描**（stage2 5,871 行还行，真实库 10 万行就完了）。

    为什么要 `write_epoch()` 而不只看行数：`count(*)` **只在增删时变**，
    而 `core_entities` / `source_date` 的**内容更新**不改行数 ——
    只看行数会让缓存在"重新提炼了实体"之后静默陈旧。

    ★★ 参赛版：缓存键**必须带上用户作用域** ——
    否则 A 用户的实体倒排会被 B 用户的查询复用（跨用户泄漏），
    而且是最隐蔽的一种：查询本身没问题，错的是被复用的索引。
    """
    from services.B3_header_engine import count_total, write_epoch
    uk = tuple(sorted(user_ids)) if user_ids else ()
    key = (count_total(), write_epoch(), uk)
    if _ROUTE_CACHE.get('key') == key:
        return _ROUTE_CACHE['ent'], _ROUTE_CACHE['time']
    from services.B4_dual_gate_retrieval.routes import EntityRoute, TimeRoute
    ent, tm = (EntityRoute.from_store(user_ids=list(uk) or None),
               TimeRoute.from_store(user_ids=list(uk) or None))
    _ROUTE_CACHE.update({'key': key, 'ent': ent, 'time': tm})
    return ent, tm


def fuse_routes(query_text: str, ranked: list, filters: Optional[dict] = None,
                stats: Optional[dict] = None,
                routes: Optional[set] = None, legacy: bool = False,
                q_lang: Optional[str] = None,
                lang_gate: bool = True, time_order: bool = True) -> list:
    """四路名次表 → RRF 等权融合 → **物化成候选 dict 列表**（顺序 = 融合名次）。

    Args:
        query_text: 查询原文。
        ranked: 路1 语义的候选（`phase2_rank` 的输出，已按 `_score` 降序）。
        filters: **调用方的显式过滤条件**，原样转给路4（键与 `raw_search` 同名）。
            为什么是传参而不是在这里复制一份过滤语义：过滤契约只有一处实现
            （`phase1_filter` / `fts_search` 那一套），复制到别处迟早不一致。
        stats: 诊断字典（写进 `rank_stats["routes"]`）。
        routes: 参与融合的路名集合；`None` = 听 config（`enabled_routes()`）。
        q_lang: 查询语言（`'zh'`/`'en'`）。`None` = 自己判一次（**仅向后兼容**；
            V1.3.4 起正规路径由 `retrieve()` 判一次后传进来，见 `ROUTE_LANG`）。
        lang_gate: `True`（默认）= 按 `ROUTE_LANG[q_lang]` 决定每路是否参与。
            `False` = **绕过策略表**（各路按 `routes`/config 原样参与、词法一律打原文表），
            供"门控值多少分"的**同运行内配对**使用 —— 这种开关必须显式可配。

    Returns: 候选 dict 列表，顺序即融合名次。
    """
    from services.B3_header_engine import get_by_ids, raw_available, raw_search
    from services.B4_dual_gate_retrieval.routes import (
        rrf_fuse, VEC_N, ENT_N, TIME_N, LEX_N, SEED_FROM_VEC, RRF_K,
    )
    if not ranked:
        return ranked
    # ★ 语言：只用入口传来的那一个值；没传才自己判（向后兼容）。**不再二次判断**。
    if q_lang is None:
        q_lang = query_lang(query_text)
    want = enabled_routes() if routes is None else set(routes)
    # 语言门控：按策略表把该语言下不参与的路摘掉
    dropped: list = []
    if lang_gate:
        for r in list(want):
            if r in ROUTE_LANG and ROUTE_LANG[r].get(q_lang) is False:
                want.discard(r)
                dropped.append(r)
    lex_table = None if lang_gate else 'raw'
    if lang_gate and 'lexical' in want:
        lex_table = ROUTE_LANG['lexical'][q_lang]

    vec_ids = [it.get('memory_id') for it in ranked[:VEC_N] if it.get('memory_id')]
    ent_ids: list = []
    time_ids: list = []
    lex_ids: list = []
    # 路2/路3：实测在测试端几乎不贡献（见 `enabled_routes` 的说明），
    # 所以**没开就一趟都不跑** —— 不是"跑了再扔掉"，那会白花全表扫描的时间。
    if want & {'entity', 'time'}:
        try:
            ent_route, time_route = _route_objects((filters or {}).get('user_ids'))
            if 'entity' in want:
                # ★ 必须把**查询原文**传进去：不传的话这一路只看种子记忆的实体，
                #   等于"从路1 邻域再走一跳"，不是独立维度（见 `entities_in_text`）。
                # ★ `min_hits=2`：共现约束。实测独占覆盖在**两个语料**上都以 2 最优
                #   （英文 2.2%→2.8%、中文 4.0%→6.0%；3 反而更差）。
                #   机制：这是 BM25 的词项求和**表达不出来的**结构信号。
                # `legacy=True`：不传查询、min_hits=1 = 修复前行为。
                ent_ids = ent_route.rank(
                    vec_ids[:SEED_FROM_VEC], top_n=ENT_N,
                    query_text=None if legacy else query_text,
                    min_hits=1 if legacy else _cfg_int('entity_min_hits', 2))
            if 'time' in want:
                # `legacy=True`：`anchor=''` = 明确弃权（修复前"没锚点就不解析"的行为）
                # ★ V1.3.4 步骤 2：`order_with` 把**窗口内的破平键**从 `memory_id`
                #   （代码自己都注明"没有信息量"）换成语义相关性 —— 时间路只**划范围**，
                #   范围内的顺序**继承**相关性。`time_order=False` 则退回旧行为（供 A/B）。
                _ord = None
                if time_order:
                    _ord = {it.get('memory_id'): i for i, it in enumerate(ranked)
                            if it.get('memory_id')}
                time_ids = time_route.rank(query_text, top_n=TIME_N,
                                           anchor='' if legacy else None,
                                           order_with=_ord)
        except Exception as error:             # noqa: BLE001 - 少一路不能拖垮检索
            if stats is not None:
                stats['routes_error'] = f'{type(error).__name__}: {error}'
    # 路4：词法。**按查询语言选表** —— 英文打原文索引、中文打摘要+实体索引
    # （见 `pick_lexical` 的说明：中文题上打原文索引实测命中 0/50）。
    # **不可用就整路跳过**（不报错）—— 老库没建表时行为与改动前一致。
    lex_tag = 'none'
    if 'lexical' in want:
        lex_fn, lex_tag = pick_lexical(query_text, legacy=legacy, table=lex_table)
        if lex_fn is not None:
            try:
                # ⚠️ 必须带上调用方的过滤条件（与 `phase1_filter` / `fts_search` 同契约），
                #    否则这一路会把调用方明确排除的记忆塞回候选里 —— 那是静默绕过过滤契约，
                #    还会让 `empty=True`（硬规则 2「筛完真没有就诚实说没有」）失效。
                lex_ids = lex_fn(query_text, k=LEX_N, **(filters or {}))
            except Exception as error:         # noqa: BLE001
                if stats is not None:
                    stats['routes_error'] = f'{type(error).__name__}: {error}'

    lists = [x for x in (vec_ids, ent_ids, time_ids, lex_ids) if x]
    if not lists:
        return ranked
    fused = rrf_fuse(lists, k=RRF_K) if len(lists) > 1 else lists[0]

    # ---- 物化：融合可能带进 Phase1 候选池**之外**的条目（路3/路4 尤其）
    by_id = {it.get('memory_id'): it for it in ranked}
    missing = [m for m in fused if m and m not in by_id]
    brought = 0
    if missing:
        try:
            for row in (get_by_ids(missing) or []):
                d = dict(row)
                mid = d.get('memory_id')
                if mid:
                    # 路2/3/4 独有的条目没有 Phase2 的语义分。给 `_score = 0`：
                    # 融合后**不按 `_score` 排序**（`top = ranked[:final_topk]` 是
                    # 保序截断），给 0 只是为了万一有人按它排也不会 KeyError。
                    d.setdefault('_score', 0.0)
                    by_id[mid] = d
                    brought += 1
        except Exception as error:             # noqa: BLE001
            if stats is not None:
                stats['routes_error'] = f'{type(error).__name__}: {error}'
            # 取不到就只保留已有的（宁可少给，也不要凭空造条目）
            fused = [m for m in fused if m in by_id]
    out = [by_id[m] for m in fused if m in by_id]
    if stats is not None:
        stats['routes'] = {
            'enabled': True, 'k': RRF_K, 'want': sorted(want),
            'q_lang': q_lang, 'lang_gate': bool(lang_gate),
            'dropped_by_lang': sorted(dropped),
            'time_order': bool(time_order),
            'lex_table': lex_tag,
            'n_vec': len(vec_ids), 'n_ent': len(ent_ids),
            'n_time': len(time_ids), 'n_lex': len(lex_ids),
            'fused': len(out), 'brought_in': brought,
        }
    return out


# ---------- 端到端主流程 ----------

def expand_hits(items: list, window: int = 2, budget_chars: int = 24000) -> dict:
    r"""**命中即扩窗**：把每条命中换成"它 + 同一会话里前后各 window 条"。

    ## 这是什么（E 形态的插件实现）

    2026-10-02 LongMemEval 实测：**轮级排序比会话排序召回到 +8.2pp**
    （Recall@5 89.4% → 97.0%，`single-session-user` 65.7% → 100%），
    但**只交那一轮**会掉分（对照组 −4pp，机制：跨会话计数少算、时间线方向反）。
    两件事合起来的正解是：

        **按轮排序（精度）→ 按会话补全（完整度）→ 交付**

    这就本函数。它与 `routes`/`rerank` **无关**：那些决定"谁排前面"，
    本函数只决定"排前面的那条，交付时带多少邻居"。

    ## 三条不许违反的规矩

    1. **不参与排序**：只追加、不改顺序、不改 `_rerank_score`/`_final_score`。
       命中的第一条永远是第一条（调用方按顺序读上下文，顺序变了等于换了交付物）。
    2. **邻居不吃热度**：调用点必须在 `_apply_retrieval_heat()` **之前**扩窗、
       并把邻居标 `_expanded=True`，升温只对真命中做 —— 否则"挨着热门条目"
       就会自己变热，B5 的热度闭环被静默污染。
    3. **预算硬约束**：`budget_chars` 是**追加部分**的上限，超了就停止追加
       （宁可少给邻居，也不许把上下文撑爆；本仓在 64K 上下文上已经踩过超限）。

    Returns:
        `{"items": [...], "added": n, "chars": n, "skipped": 原因}` —— 
        `items` 是**新的列表**（不原地改 `items`），前 `len(items)` 条仍依次是原命中。
    """
    if not items or window <= 0:
        return {"items": list(items or []), "added": 0, "chars": 0,
                "skipped": "window<=0（关闭）"}
    try:
        from services.B3_header_engine import neighbours_of
    except Exception as error:                              # noqa: BLE001
        return {"items": list(items), "added": 0, "chars": 0,
                "skipped": f"neighbours_of 不可用：{type(error).__name__}"}
    hit_ids = [it.get("memory_id") for it in items if it.get("memory_id")]
    have = set(hit_ids)
    try:
        nb = neighbours_of(hit_ids, window=window)
    except Exception as error:                              # noqa: BLE001
        return {"items": list(items), "added": 0, "chars": 0,
                "skipped": f"扩窗失败（fail-open）：{type(error).__name__}"}

    added, chars = [], 0
    for row in nb:
        mid = row.get("memory_id")
        if not mid or mid in have:
            continue
        text = str(row.get("summary") or "")
        if budget_chars and chars + len(text) > budget_chars:
            break
        have.add(mid)
        chars += len(text)
        added.append({**row, "_expanded": True, "summary": text,
                      # 邻居**没有**参与排序，所以不许假装有精排分
                      "_rerank_score": None, "_final_score": None})
    return {"items": list(items) + added, "added": len(added), "chars": chars,
            "skipped": ""}


def retrieve(
    query_text: str,
    task_types: Optional[list] = None,
    time_buckets: Optional[list] = None,
    layer: Optional[str] = None,
    core_entity_any: Optional[list] = None,
    core_entity_all: Optional[list] = None,
    # ★ 2026-10-01：按 `session_id` 限定候选。LongMemEval 每道题有**自己的** haystack，
    #   一个库里躺着 500 题共约 24,000 个会话，必须把候选限定在本题那 ~48 个会话内，
    #   否则召回口径与论文**完全不可比**（还白烧算力）。
    session_ids: Optional[list] = None,
    # ★★ 参赛版（AML）：`user_id` 是官方契约的**唯一检索隔离边界**。
    #    它必须穿到**每一条**召回路径（Phase1 / FTS 预筛 / 四路各自的检索），
    #    漏掉任何一路都是跨用户泄漏 —— 这正是 `session_ids` 当初漏传时踩过的坑
    #    （2026-10-01：FTS 把别题的会话塞回候选，被 verify_session_filter 抓到）。
    user_ids: Optional[list] = None,
    max_tokens: int = 5000,
    max_items: int = 10,
    phase1_limit: Optional[int] = None,
    fts_prefilter: Optional[bool] = None,
    fts_k: Optional[int] = None,
    mmr_lambda: Optional[float] = None,
    mmr_pool: Optional[int] = None,
    prf_enabled: Optional[bool] = None,
    prf_k: Optional[int] = None,
    prf_seed: Optional[int] = None,
    final_topk: Optional[int] = None,
    phase2_topk: Optional[int] = None,
    rerank_enabled: Optional[bool] = None,
    rerank_pool: Optional[int] = None,
    route_fuse: Optional[bool] = None,
    rerank_doc: Optional[str] = None,
    routes: Optional[list] = None,
    routes_legacy: Optional[bool] = None,
    routes_lang_gate: Optional[bool] = None,
    routes_time_order: Optional[bool] = None,
    # ★ 2026-10-02（E 形态）：**命中即扩窗** —— 按轮排序、按会话补全。
    #   实测依据见 `expand_hits()` 的文档（轮级排序 +8.2pp 召回，
    #   但只交轮会掉分；"轮级找 + 会话交"两臂都不掉）。
    #   `0` = 关闭（默认，行为与改动前**逐字一致**）。
    expand_window: Optional[int] = None,
    expand_budget_chars: Optional[int] = None,
    apply_heat: bool = True,
    trace_id: Optional[str] = None,
    phase0_auto_extract: bool = False,
) -> dict:
    """端到端检索管道。

    验收：端到端 300ms 内（桥模型除外）

    Args:
        max_items: **本次调用希望交付的条数**（也是预算裁剪的条数上限）。
        phase1_limit: Phase1 最多把多少行喂给 Phase2。
            ⚠️ 2026-09-26 从 1000 改成 10000。原值在**万条级库上会静默吃掉召回**：
            LoCoMo 阶段二实测（10 段对话合并、5,871 行、heat 全为 0），
            `LIMIT 1000` 只让 17% 的行进入 Phase2，被截掉的是**日期靠后的整段**
            （B3 的排序是 `heat DESC`，heat 全 0 时退化成插入顺序 = 日期顺序），
            于是 recall@15 从 ~55% 塌到 10.8%，而且**不报任何错**。
            代价是 Phase2（纯 Python 余弦）变慢：5,871 候选实测端到端中位 ~350ms，
            已超「<300ms」的验收线。所以这个默认值不是"越大越好"，
            再往上（>1 万行）应当先上 ANN / FTS5 做相关性预筛，而不是继续调大它。
        fts_prefilter: **默认开启**。开启后 Phase1 的候选是**两条来源的并集**：
            ① 原有的 SQL WHERE 过滤（按 heat 排序、截到 `phase1_limit`）
            ② FTS5 BM25 相关性预筛（取 `fts_k` 条）
            取并集而非交集：FTS 只可能"多给候选"，不可能把向量通道已找到的结果挤掉；
            FTS 不可用或没命中时自动退化为 ①，行为与改动前**完全一致**。
            这是"扩展性"的正解：候选选择从「按热度截断」换成「按相关性补足」，
            于是 `phase1_limit` 可以调回小值换速度，而召回不再被它决定。
        fts_k: FTS5 这一路取多少条（默认 200）。只影响候选量，最终排序仍由 Phase2 决定。
        mmr_lambda: **MMR 多样性的权重，默认 1.0 = 关闭**（= 纯按相关性，旧行为）。
            第 3 项「cat3 开放域聚合」用它：开放域问题的答案散在多条记忆里，
            而纯按相关性排的 top-K 常被同一天/同一场会话的近似内容占满 ——
            交付 15 条其实只覆盖 2~3 件事，聚合无从谈起。λ<1 时按
            `λ·相关性 − (1−λ)·与已选条目的最大相似度` 逐条挑，把 top-K 摊开。
            ⚠️ 它可能帮 cat3 但伤 cat4（单跳只需要一条精确的），**必须实测再定默认值**。
        mmr_pool: MMR 的候选池大小（默认 60）。Phase2 的排序本来就对全部候选算完了，
            多返回只是少截一次、**不额外花钱**；池子太小 MMR 没空间。
        prf_enabled: **伪相关反馈，默认关**（第 3 项 cat3 开放域聚合）。
            开启后：取第一轮 top-`prf_seed` 条的 `core_entities`，用它们再查一次 FTS5，
            把新命中的记忆补进候选池重排。针对的是"证据不长得像问题"的开放域题 ——
            实测 cat3 里证据 ≥3 条的 22 题命中率只有 31.8%。
            只对**新增**候选打分，不重算已有候选，所以不会让 Phase2 跑两遍。
        prf_k: PRF 这一轮 FTS 取多少条（默认 200）。
        prf_seed: 用第一轮前几条的实体做种子（默认 8）。
        final_topk: 再单独指定「最终交付条数」。
            ⚠️ 2026-09-26 修：此前它的默认值是常量 `_TOPK_FINAL = 5`，
            于是「传 `max_items=15` 却只拿回 5 条」——`max_items` 只喂 `budget_clip`
            （只能再减、不能增），真正管交付条数的是这个参数，而它**没写进文档**。
            实测代价很大：LoCoMo conv-42 上 recall@5 = 34.6%、recall@15 = 48.1%
            （623 条候选），也就是**默认值把召回砍掉了 13.5 个点**。
            现在默认 `None` = **跟随 `max_items`**，即 `max_items` 说了算，符合调用方直觉；
            需要「想要 15 条上限、但只要 5 条」这种组合时才显式传它。
        apply_heat: 是否对本次最终入选的记忆施加「检索命中」升温
            （`heat+=1.0`，见 B5 `ACTIONS["retrieval_hit"]`）。
            这是「流通」闭环的最后一环：命中 → 升温 → 下次排序更靠前。
            ⚠️ 2026-09-25 起**它不再动 count** —— count 已改由 `adopted`
            （调用方显式上报「这条我真的用了」）产生，见 B5 模块开头的分工说明。
            默认开启；只想读不想改库（例如做评测、跑对照）时传 False。
        trace_id: 调用方自带链路 id（B6 MAP 的 `request_id`/`trace_id`）。
            传了就与调用方的 id 对齐，于是「MCP 返回的响应」和
            「audit_log 里的证据」可以按同一个 id 串起来；不传则自己生成。
        phase0_auto_extract: **默认 False**。设为 True 时，若调用方没传任何
            显式 filters，会自动用 jieba + 规则从 query 里抽 core_entities
            传给 Phase1。⚠️ 默认关闭是故意的：entity 过滤太激进，433 行库上
            实测 6 条 query 有 3 条被误杀——SQL 里存的 core_entities 是 B2 提炼
            的名词，跟 query 词不完全对得上。这个开关留给：
            ① 10 万行以上 + 有 FTS5 索引时 ② 调用方确信 query 实体与库匹配时。

    Returns: {
        "items": [...],                    # Final Top-N 记忆条目
        "total_filtered": int,             # Phase1 过滤后数量
        "total_ranked": int,               # Phase2 精排后数量
        "truncated": bool,                 # 预算裁剪触发
        "tokens_used": int,
        "elapsed_ms": float,
        "embed_stats": dict,               # 向量来源统计（缓存/批量/兜底）
        "rank_stats": dict,                # 精排统计（含降级条数与阈值）
        "heat_stats": dict,                # 升温统计（命中数/失败数）
        "phase0": dict,                    # Phase0 状态（实体/task_type 提取结果）
        "trace_id": str,                   # B7 审计链路 id（检索与升温共用）
        "pipeline": ["phase0", "phase1", "phase2", "budget", "heat"],
    }
    """
    t0 = time.time()
    # `final_topk=None` = 跟随 max_items（见 docstring 里 2026-09-26 的说明）。
    # 这一行就是「传 max_items=15 却只拿回 5 条」那个坑的修复点：
    # 在此之前的默认值是常量 `_TOPK_FINAL = 5`，而 `max_items` 只喂 budget_clip。
    if final_topk is None:
        final_topk = max_items
    # Phase2 返回条数上限。`None` = 听 config.yaml（`retrieval.phase2_topk`），默认 20。
    #
    # ⚠️ 2026-09-26 加这个参数，是因为发现 **`final_topk` 被静默钳到 20**：
    # 老代码这一行是硬编码常量 `_TOPK_PHASE2 = 20`，于是
    # `retrieve(max_items=60, final_topk=60)` 只会拿回 20 条，**不报错、不告警**。
    # 实测（D:\Deepseek Harness\locomo-full\stage2，5,871 行）：
    #   final_topk=15 → 15 条；final_topk=30/60/200 → 全都是 20 条。
    # 危害不止"少给几条"：所有**依赖大池子**的机制会静默失效 ——
    #   * MMR 的 `mmr_pool=60`、cross-encoder 重排序要的都是百级池子；
    #   * 我一度按 `max_items=60` 去量"池子多大才够"，量出来的 oracle@60 与
    #     oracle@20 **完全相等**，差点得出"第一阶段没有余量"的错误结论 ——
    #     真因是池子根本没超过 20，这个读数**不构成任何证据**。
    # 排序本身对全部候选都算了分（见 `phase2_rank`），放大这个上限**不额外花钱**，
    # 只是少截一次。默认保持 20 以不动线上行为；要重排序/聚合必须先把它调大。
    if phase2_topk is None:
        phase2_topk = _cfg_int("phase2_topk", _TOPK_PHASE2)
    # cross-encoder 重排序：None = 听 config.yaml（`retrieval.rerank_enabled`），默认关。
    if rerank_enabled is None:
        rerank_enabled = _cfg_bool("rerank_enabled", False)
    # V1.3 增量 1：四路融合 + 重排读哪份文本。**默认关**（见模块内那一块的说明）。
    if route_fuse is None:
        route_fuse = _cfg_bool("routes_enabled", False)
    if rerank_doc is None:
        rerank_doc = _cfg_str("rerank_doc", "auto")
    # 哪几路参与融合：`None` = 听 config（`retrieval.routes`）。
    # 显式传列表可覆盖 —— 测试端与生产端的"该开哪几路"结论不同（见 `enabled_routes`）。
    _want_routes = set(routes) if routes else enabled_routes()
    # ★ `routes_legacy`：还原**2026-10-01 修复前**的路由行为（三处一起还原）——
    #   `pick_lexical` 一律打原文表 · 路2 不看查询且 `min_hits=1` · 时间路锚点弃权。
    #   存在的理由：要回答"这次修复值多少分"，唯一干净的做法是**同一次运行内**
    #   把修复前后的两档并排跑（跨运行的差会被 `temperature 0.1` 的噪声淹掉）。
    if routes_legacy is None:
        routes_legacy = _cfg_bool('routes_legacy', False)
    # ★ V1.3.4 步骤 1：**查询语言在入口判一次**，作为标签往下传（各路不得再判）。
    #   `routes_lang_gate=False` = 绕过 `ROUTE_LANG` 策略表（供 A/B，不藏在代码里）。
    _q_lang = query_lang(query_text)
    if routes_lang_gate is None:
        routes_lang_gate = _cfg_bool('routes_lang_gate', True)
    # ★ V1.3.4 步骤 2：时间路的破平键 = 语义相关性（默认开）。
    #   `False` = 退回 `memory_id`（旧行为），供同运行内配对量"这一处值多少分"。
    if routes_time_order is None:
        routes_time_order = _cfg_bool('routes_time_order', True)
    if routes_legacy and route_fuse:
        # 只喊一次：这是**每次检索**都会走的路径，逐次打印会把日志淹掉
        global _LEGACY_WARNED
        if not _LEGACY_WARNED:
            _LEGACY_WARNED = True
            print('[B4] ⚠️ routes_legacy=True：本次走**修复前**行为'
                  '（原文词法表 / 路2 不看查询且 min_hits=1 / 时间路锚点弃权）',
                  file=sys.stderr)
    if rerank_pool is None:
        rerank_pool = _cfg_int("rerank_pool", 50)
    # FTS5 预筛开关：None = 听 config.yaml（`retrieval.fts_prefilter` / `retrieval.fts_k`）
    if fts_prefilter is None:
        fts_prefilter = _cfg_bool("fts_prefilter", True)
    if fts_k is None:
        fts_k = _cfg_int("fts_k", 2000)
    # ⚠️ phase1_limit 的默认值与 FTS 是否可用**绑定**，这是安全设计：
    #   * FTS 可用 → 1000（快，召回由 FTS 的相关性候选补足；实测反而比全量候选更高）
    #   * FTS 不可用 → 10000（退回大上限，靠"全都喂给 Phase2"保住召回）
    # 若不管这一条，在不支持 FTS5 的环境上会静默掉到 8.3% 召回 —— 那正是本模块
    # 一路在防的"静默失效"。想强制某个值就在 config 里写 `retrieval.phase1_limit`。
    if phase1_limit is None:
        phase1_limit = _cfg_int("phase1_limit", 1000 if _fts_ready() else 10000)
    # MMR 多样性开关：默认 1.0 = 关闭（纯按相关性），需要开放域聚合时调小
    if mmr_lambda is None:
        mmr_lambda = _cfg_float("mmr_lambda", 1.0)
    if mmr_pool is None:
        mmr_pool = _cfg_int("mmr_pool", 60)
    # 伪相关反馈（PRF）：默认关，第 3 项测量后再定
    if prf_enabled is None:
        prf_enabled = _cfg_bool("prf_enabled", False)
    if prf_k is None:
        prf_k = _cfg_int("prf_k", 200)
    if prf_seed is None:
        prf_seed = _cfg_int("prf_seed", 8)
    # B7 审计链路 id：本次检索与随之发生的升温共用，便于把「谁被想起、被加了热」串成因果链。
    # 调用方（B6 MAP）自带 id 时沿用它 —— 否则 MCP 返回的 trace_id 与审计里的对不上，
    # 手里拿着响应却查不到证据链。
    trace_id = trace_id or uuid.uuid4().hex[:16]

    # Phase0: 自动抽 query 里的 entities（只在调用方没传显式 filters 时用）。
    # 原因：filters=None 时 Phase1 返回全库，Phase2 精排成本随库大小线性增长。
    # 这不是路径 B 的「小模型语义筛选」（那个被 A/B benchmark 否了），
    # 而是让 Python 层 entity 过滤先筛一层。确定性规则 + jieba，<5ms。
    #
    # ⚠️ 只抽 entities，不自动猜 task_type。task_type 猜错会让 SQL WHERE 全 miss
    # （实测 "记忆插件怎么做" 被猜成决策 → SQL IN ('决策') 只返回 18 行 → 全 miss）。
    # entity 过滤是 Python 层 OR（任何实体匹配就行），安全多了。
    auto_task_types = task_types
    auto_entity_any = core_entity_any
    auto_layer = layer
    auto_time_buckets = time_buckets
    phase0_stats: dict = {}
    # 关键：用 `is not None` 而不是 truthy check。
    # 空列表 [] 是 falsy，但调用方传 core_entity_any=[] 表示「显式不给 entity 过滤」，
    # 不能让 auto-extract 覆盖它。只有 caller 真的没传（None）才自动抽。
    has_explicit = any([
        task_types is not None,
        time_buckets is not None,
        layer is not None,
        core_entity_any is not None,
    ])
    if phase0_auto_extract and not has_explicit:
        # 调用方显式开启 Phase0 且没传任何过滤条件 → 自动抽 entities
        guessed_ents = extract_query_entities(query_text)
        auto_entity_any = guessed_ents if guessed_ents else None
        phase0_stats = {
            "entities_found": guessed_ents,
            "task_type_guessed": None,  # 故意不猜：猜错代价太大
            "mode": "auto",
        }
    elif has_explicit:
        phase0_stats = {"mode": "explicit"}
    else:
        phase0_stats = {"mode": "off"}

    # Phase1：候选 = ① SQL 过滤 ∪ ② FTS5 相关性预筛（2026-09-26）
    #
    # 为什么是**并集**：FTS 只可能"多给候选"，不可能把向量通道已经找到的好结果挤掉；
    # 反过来若取交集，一个 FTS 没命中的词就会把整条 SQL 候选砍掉，风险大得多。
    # FTS 不可用 / 没命中 / 抛异常 → 自动退化成 ①，行为与改动前**完全一致**。
    t_p1 = time.time()
    candidates = phase1_filter(
        task_types=auto_task_types,
        time_buckets=auto_time_buckets,
        layer=auto_layer,
        core_entity_any=auto_entity_any,
        limit=phase1_limit,
        session_ids=session_ids,
        user_ids=user_ids,
    )
    phase1_stats = {"sql": len(candidates),
                    "phase1_limit": phase1_limit,
                    # 护栏：SQL 候选**正好等于上限**说明截断发生了。
                    # 这不是错，但必须可见 —— 2026-09-26 的教训就是这个截断
                    # 一声不吭地把 83% 的记忆排除掉了（recall 55% → 9.2%）。
                    # 单看这个标记就能判断"现在是靠上限硬扛还是靠 FTS 补足"。
                    "cap_bound": len(candidates) >= phase1_limit,
                    "fts": 0, "fts_added": 0,
                    "merged": len(candidates), "fts_on": bool(fts_prefilter),
                    "fts_k": fts_k}
    # 最危险的组合：上限被顶满 **且** FTS 关着 → 候选完全由热度截断决定。
    # 这里明确喊一声（只进 stats 和 stderr，不影响检索）。
    if phase1_stats["cap_bound"] and not fts_prefilter:
        print(f"[B4] ⚠️ Phase1 候选被上限顶满（{phase1_limit}）且 FTS 预筛关闭 —— "
              f"召回正在被截断决定，建议开 `retrieval.fts_prefilter`",
              file=sys.stderr)
    if fts_prefilter:
        try:
            from services.B3_header_engine import fts_search, get_by_ids
            # ⚠️ 必须把**同一套过滤条件**传进去：否则 FTS 会把调用方明确排除掉的记忆
            # 塞回候选（2026-09-26 被 verify-b6 的「空结果」用例抓到）。
            fts_ids = fts_search(query_text, k=fts_k,
                                 task_types=auto_task_types,
                                 time_buckets=auto_time_buckets,
                                 layer=auto_layer,
                                 core_entity_any=auto_entity_any,
                                 # ★ 2026-10-01：`session_ids` **必须一起传** ——
                                 #   这条注释上面写的就是这个教训，而我加 `session_ids`
                                 #   时正好又犯了一次：漏了它，FTS 就把**别题的会话**
                                 #   塞回候选（实测 15 条里混进 3 条，
                                 #   被 `verify_session_filter.py` 抓到）。
                                 # ★★ 参赛版：`user_ids` 同理，而且是**隔离边界**，
                                 #   漏了它就是跨用户泄漏 —— 比"混进别题"严重得多。
                                 session_ids=session_ids,
                                 user_ids=user_ids)
            phase1_stats["fts"] = len(fts_ids)
            if fts_ids:
                have = {c.get("memory_id") for c in candidates}
                extra_ids = [i for i in fts_ids if i not in have]
                if extra_ids:
                    candidates = candidates + get_by_ids(extra_ids)
                    phase1_stats["fts_added"] = len(extra_ids)
            phase1_stats["merged"] = len(candidates)
        except Exception as error:  # noqa: BLE001 - 预筛坏掉绝不能拖垮检索
            phase1_stats["fts_error"] = f"{type(error).__name__}: {error}"
    phase1_stats["ms"] = round((time.time() - t_p1) * 1000, 1)
    total_filtered = len(candidates)

    # Phase1→Phase2 交接：补齐候选向量（落盘缓存 → Ollama 批量接口 → hash 兜底）
    embed_stats = _embed_candidates(candidates)

    # Phase2
    rank_stats = {}
    # Phase2 池大小 = max(配置的池下限, 调用方要的条数, MMR 需要的池)。
    # 排序本来就对全部候选算完了，多返回只是少截一次，**不额外花钱**
    # （实测 5,871 行库上 ranked 1166 条仍只要 55ms）。
    #
    # ⚠️ 这里**必须带上 `final_topk`**。老代码是硬编码 `_TOPK_PHASE2 = 20`，于是
    # `retrieve(max_items=60, final_topk=60)` 静默只给 20 条：调用方拿到被截断的结果，
    # 不报错不告警。这个坑的代价很实在 —— 我按 `max_items=60` 去量"池子多大才够"，
    # 量出 oracle@60 与 oracle@20 **完全相等**，差点写下"第一阶段没有余量、重排序无用"
    # 的结论；真因只是池子从没超过 20，那个读数不构成任何证据。
    # 取 max 让"少给"在结构上不可能发生，而不是靠一句告警去提醒。
    _phase2_base = max(phase2_topk, final_topk, rerank_pool if rerank_enabled else 0)
    _phase2_topk = max(_phase2_base, mmr_pool) if mmr_lambda < 1.0 else _phase2_base
    # 天花板仍然要有：池子随候选数无界增长会在大库上放大内存。这个值远高于
    # 当前候选量（5,871 行库 ≈ 1.5k 候选），正常调用碰不到；碰到就喊一声。
    if _phase2_topk > _PHASE2_POOL_MAX:
        rank_stats["pool_capped"] = {"asked": _phase2_topk, "pool": _PHASE2_POOL_MAX}
        print(f"[B4] ⚠️ Phase2 池被压到上限 {_PHASE2_POOL_MAX}（请求 {_phase2_topk}）—— "
              f"候选集可能大于排序池，重排序/大交付会少拿", file=sys.stderr)
        _phase2_topk = _PHASE2_POOL_MAX
    rank_stats["phase2_topk"] = _phase2_topk
    ranked = phase2_rank(query_text, candidates, top_k=_phase2_topk, stats=rank_stats)
    total_ranked = len(ranked)

    # ---- 伪相关反馈（PRF）：用第一轮 top-N 的实体再扩一轮候选（第 3 项 cat3 用）----
    #
    # 为什么要这一刀：开放域问题（"他们俩有什么共同爱好？"）的证据**不长得像问题**，
    # 第一轮的向量/词法都捞不到它 —— 实测 cat3 里证据 ≥3 条的 22 题命中率只有 31.8%。
    # 但同一场对话里**已经被捞到**的记忆，其 `core_entities` 与那些没捞到的证据
    # 高度重合（同一个人、同一件事、同一个地点）。所以拿 top-N 的实体再查一次 FTS，
    # 等于"顺着已找到的线索再摸一圈"。
    #
    # 成本控制：**只对新增候选打分**，不重算已有候选（否则 Phase2 要跑两遍，
    # 101ms 直接变 200ms）。新增候选通常几十条，实测 +40ms 量级。
    # ★ 2026-10-01：PRF 与路2 是**同一个想法的两份实现**，不能同时开。
    #
    # 两者的机制都是「拿已命中的 top-N 的 `core_entities` 再检索一轮」：
    #   * PRF（下面这段）：把扩出来的 id 并进**候选池**，再由 Phase2 重排；
    #   * 路2（`EntityRoute`）：把扩出来的 id 做成**一张名次表**交给 RRF 融合。
    # 同时开 = 同一批候选被扩张两次，既多花一次全库 FTS，又让融合里出现
    # 两条高度相关的名次表（RRF 会把"同一信号"投两票）。
    #
    # 为什么是"开融合时跳过 PRF"而不是反过来：路2 在**融合框架内**（有 RRF 席位、
    # 有留一法可量），PRF 是融合之外的一条旁路增强；而且 PRF 默认是关的
    # （`_cfg_bool("prf_enabled", False)`，两个 config 都没有这个键）。
    # → 需要在融合里做实体扩张时，**用路2**；只在单路语义下才用 PRF。
    if prf_enabled and route_fuse:
        rank_stats["prf_skipped"] = "route_fuse_on：路2 与 PRF 同源，避免重复扩张"
    elif prf_enabled and ranked:
        try:
            from services.B3_header_engine import fts_search, get_by_ids
            seed_ents: list[str] = []
            for it in ranked[:prf_seed]:
                for e in (it.get("core_entities") or []):
                    if isinstance(e, str) and len(e) >= 2 and e not in seed_ents:
                        seed_ents.append(e)
            if seed_ents:
                have = {c.get("memory_id") for c in candidates}
                extra_ids = [i for i in
                             fts_search(" ".join(seed_ents), k=prf_k,
                                        task_types=auto_task_types,
                                        time_buckets=auto_time_buckets,
                                        layer=auto_layer,
                                        core_entity_any=auto_entity_any)
                             if i and i not in have]
                if extra_ids:
                    extra_rows = get_by_ids(extra_ids)
                    scored_extra = phase2_rank(query_text, extra_rows,
                                               top_k=len(extra_rows)) if extra_rows else []
                    already = {it.get("memory_id") for it in ranked}
                    added = [it for it in scored_extra
                             if it.get("memory_id") not in already]
                    if added:
                        ranked = sorted(ranked + added,
                                        key=lambda x: x["_score"], reverse=True)
                        total_ranked = len(ranked)
                    rank_stats["prf"] = {"seed_entities": len(seed_ents),
                                         "extra_ids": len(extra_ids),
                                         "added": len(added)}
                else:
                    rank_stats["prf"] = {"seed_entities": len(seed_ents),
                                         "extra_ids": 0, "added": 0}
        except Exception as error:  # noqa: BLE001 - 扩展失败绝不能拖垮检索
            rank_stats["prf_error"] = f"{type(error).__name__}: {error}"

    # ---- 四路融合（V1.3 增量 1，默认关）----
    # 插在 Phase2 之后、cross-encoder 之前：这样最后那一环重排看到的是**融合后的池**
    # （架构图写的正是"四路 → RRF → top-30 → cross-encoder → top-15"）。
    # ⚠️ 融合会带进 Phase1 候选池之外的条目，所以这里**必须把过滤条件原样传给路4**。
    if route_fuse and ranked:
        try:
            ranked = fuse_routes(
                query_text, ranked,
                filters={'task_types': auto_task_types,
                         'time_buckets': auto_time_buckets,
                         'layer': auto_layer,
                         'core_entity_any': auto_entity_any,
                         # ★ 必须一起传：否则词法路（路4）会**绕过** session 限定，
                         #   在全库 24,000 条里按词法命中塞进别题的会话 ——
                         #   这正是 `_filter_where` 抽出来时防的那类"显式过滤被旁路"。
                         'session_ids': session_ids,
                         # ★★ 参赛版：隔离边界也必须进 filters ——
                         #    四路（尤其路2 实体路/路4 词法路）会在**全库**上做检索，
                         #    不过滤就是跨用户泄漏。见 routes.fuse_routes 的消费点。
                         'user_ids': user_ids},
                stats=rank_stats, routes=_want_routes, legacy=routes_legacy,
                q_lang=_q_lang, lang_gate=routes_lang_gate,
                time_order=routes_time_order)
            total_ranked = len(ranked)
        except Exception as error:             # noqa: BLE001 - 旁路增强绝不拖垮检索
            rank_stats['routes_error'] = f'{type(error).__name__}: {error}'

    # ---- cross-encoder 重排序（第 4 项）----
    # 放在 PRF 之后、取 Final Top-N 之前：这样重排看到的是"最终候选全貌"，
    # 包括 PRF 补进来的那些。池大小已由上面的 `_phase2_base` 保证 >= rerank_pool。
    if rerank_enabled and ranked:
        try:
            ranked = rerank_top(query_text, ranked, rerank_pool, stats=rank_stats,
                                doc_source=rerank_doc)
            total_ranked = len(ranked)
        except Exception as error:  # noqa: BLE001 - 旁路增强失败绝不能拖垮检索
            rank_stats["rerank_error"] = f"{type(error).__name__}: {error}"

    # 取 Final Top-N：MMR 开启时按多样性挑，否则保持"纯按相关性"的旧行为
    if mmr_lambda < 1.0:
        top = mmr_select(ranked, final_topk, lam=mmr_lambda, pool=mmr_pool,
                         stats=rank_stats)
    else:
        top = ranked[:final_topk]

    # 预算裁剪
    budgeted = budget_clip(top, max_tokens=max_tokens, max_items=max_items)

    # 升温闭环：本次真正交付给调用方的记忆算「被翻出来过」（只加热度，不动 count）
    heat_stats = _apply_retrieval_heat(budgeted["items"]) if apply_heat else {"skipped": True}

    # ★ 命中即扩窗（E 形态）—— **必须在升温之后**：
    #   邻居是被"顺带带出来"的上下文，不是检索命中，给它升温会污染 B5 热度闭环
    #   （一条记忆只因为挨着热门条目就变热）。顺序反了不报错，只是数字悄悄变歪。
    _ew = expand_window
    if _ew is None:
        _ew = _cfg_int0("expand_window", 0)          # ⚠️ 必须用 _cfg_int0（0=关闭）
    _eb = expand_budget_chars
    if _eb is None:
        _eb = _cfg_int0("expand_budget_chars", 24000)
    exp = expand_hits(budgeted["items"], window=int(_ew or 0),
                      budget_chars=int(_eb or 0))
    deliv = exp["items"]
    rank_stats["expand"] = {"window": int(_ew or 0), "added": exp["added"],
                            "chars": exp["chars"], "skipped": exp["skipped"]}

    elapsed_ms = round((time.time() - t0) * 1000, 1)

    result = {
        "items": deliv,
        "total_filtered": total_filtered,
        "total_ranked": total_ranked,
        "truncated": budgeted["truncated"],
        # ⚠️ 预算是对**命中条目**算的；扩窗追加的字符**另计**并如实报出来，
        #    否则"tokens_used"会让人以为交付量没变（本仓反复踩的"读数与实物不符"）。
        "tokens_used": budgeted["tokens_used"],
        "expand_chars": exp["chars"],
        "elapsed_ms": elapsed_ms,
        "embed_stats": embed_stats,
        "rank_stats": rank_stats,
        "heat_stats": heat_stats,
        "phase0": phase0_stats,
        "phase1": phase1_stats,
        "trace_id": trace_id,
        "pipeline": ["phase0", "phase1", "phase1_fts", "phase2", "budget",
                     "heat", "expand"],
    }

    # B7 审计：检索 +（同一 trace_id 下的）升温改库。
    #
    # `apply_heat=False` 表示「只读探查」——常用于评测、对照实验、验证脚本。
    # 这类调用**不写审计**，否则自检会产生一堆 `query=自检…` 的假记录污染真实证据链。
    # 审计与升温要么一起开，要么一起关，语义才自洽。
    if apply_heat:
        _audit_search(query_text, result, trace_id, {
            "task_types": task_types,
            "time_buckets": time_buckets,
            "layer": layer,
            "core_entity_any": core_entity_any,
            "max_items": max_items,
            "final_topk": final_topk,
        })
        _audit_heat(budgeted["items"], heat_stats, trace_id)

    return result


def _audit_search(query_text: str, result: dict, trace_id: str, filters: dict) -> None:
    """B7 审计：记录一次检索。

    ★★ 参赛版（AML）：**整体停用**，函数保留为 no-op。
    理由（三条，都不是"省事"）：
      1. AML 的 `POST /search` 是**同步**接口，平台在等结果；每次检索多写一次
         SQLite 只是给延迟加账，对分数 0 贡献；
      2. 审计的意义是"事后追责 / 数据主权可查"，那是**长期产品**的属性；
         单次比赛没有"事后"；
      3. 官方契约里参评方只负责 Add/Search，审计不属于交付面。
    ⚠️ 保留函数外壳、不在 import 处删掉：这样与上游 diff 最小，
    将来要开审计只需把 `return` 去掉。
    """
    return


def _audit_heat(items: list[dict], heat_stats: dict, trace_id: str) -> None:
    """B7 审计：记录一次升温改库（与同一次检索共用 trace_id）。

    ★★ 参赛版：停用（与 `_audit_search` 同因）。升温本身也已停用，
    所以这个函数在参赛路径上根本不会被调用到有内容的 `touched`。
    """
    return


def _apply_retrieval_heat(items: list[dict]) -> dict:
    """对本次入选的记忆施加「检索命中」升温（B5 ACTIONS["retrieval_hit"]）。

    这是架构书 §5.7「检索升温」的落地点，也是整条链路唯一缺少的反馈回路：
    在此之前 `heat`/`count` 从建库起一直是 0，所以「常用自然浮现」永远不发生 ——
    实测 sum(heat)=0.0、max(count)=2（那 2 还是迁移时带进来的）。

    ⚠️ 2026-09-25 起这个动作**只加热度、不动 count**。`count` 的语义已改为
    「被采用次数」，只由 B5 `adopted` 动作产生（调用方显式上报）。
    原先这里是 `(1.0, 1)`，于是 count 实际等于「被检索交付的次数」——
    可检索之后还有相关性过滤 / 用户确认过滤 / preview 预览三道关卡，
    被拦下的记忆 count 也照样涨，语义对不上。

    只对 `budget_clip` **之后**仍然留下的条目升温，也就是真正交付出去的 Top-N：
    被 Phase2 排到后面、又被预算裁掉的条目，等于没被想起，不该升温。

    失败绝不抛出：升温属于反馈信号，坏掉不能让检索本身失败。
    """
    from services.B3_header_engine import update_heat_count, get_by_id

    stats = {"applied": 0, "failed": 0, "ids": []}
    # ★★ 参赛版（AML）：**整段停用**。理由：
    #   ① 没有"使用史"—— 单次比赛里，第 1 题写下的热度对第 2 题没有意义
    #      （每题的数据互相独立，且 platform 的 query 到达顺序无关紧要）；
    #   ② `user_id` 隔离下，热度是"跨用户共享的排序先验"的温床，风险大于收益；
    #   ③ 官方判分只看答案正确率，热度不进入任何可见指标。
    #   保留函数与返回值形状（调用方读 `stats`），但**不写库**。
    return stats

    machine = HeatStateMachine()
    for item in items:
        memory_id = item.get("memory_id")
        if not memory_id:
            continue
        try:
            row = get_by_id(memory_id)
            current_heat = float((row or {}).get("heat") or 0.0)
            current_count = int((row or {}).get("count") or 0)
            new_heat, new_count, _ = machine.apply_action(
                current_heat, current_count, "retrieval_hit"
            )
            update_heat_count(
                memory_id,
                heat_delta=new_heat - current_heat,
                count_delta=new_count - current_count,
            )
            # 回填到返回条目上，调用方/测试能直接看到升温结果
            item["heat"] = new_heat
            item["count"] = new_count
            item["_heat_applied"] = True
            stats["applied"] += 1
            stats["ids"].append(memory_id)
        except Exception as error:  # noqa: BLE001 - 反馈信号失败不影响检索
            stats["failed"] += 1
            print(f"[B4] 升温失败 {memory_id}: {type(error).__name__}: {error}")
    return stats


def commit_retrieval(result: dict, query_text: str = "",
                     filters: Optional[dict] = None) -> dict:
    """把一次 `apply_heat=False` 的检索结果「提交」为副作用：升温 + B7 审计。

    为什么需要单独一步（B6 validator 的「重试不重复升温」）
    --------------------------------------------------
    架构书 §5.8 要求 MAP validator「响应格式不合法时自动重试 1 次」。而 `retrieve()`
    的升温（`heat+1, count+1`）是**写库副作用**：若先升温再校验格式，重试就会把同一批
    记忆再加一次热；更糟的是 heat 一变，Phase2 排序 `sim + 0.3·heat + 0.2·count` 也跟着变，
    第二次响应与第一次不可比（重试不可复现）。

    所以正确顺序只能是：
        1) `retrieve(..., apply_heat=False)` 生成响应  —— 纯读，零副作用
        2) validator 校验响应格式；不合法就重试（仍然零副作用）
        3) 校验通过后调本函数**提交且只提交一次**副作用

    见 `services/B6_map_protocol/protocol.py: guarded_request_handler`。

    Returns: heat_stats（与 `retrieve()` 返回里的同名字段一致）
    """
    items = result.get("items") or []
    trace_id = result.get("trace_id", "")
    heat_stats = _apply_retrieval_heat(items)
    result["heat_stats"] = heat_stats
    # 审计与升温共用 trace_id：一条 trace 两条动作（search + write），
    # audit_log 的主键是 (trace_id, action)，所以两者不会互相覆盖。
    _audit_search(query_text, result, trace_id, filters or {})
    _audit_heat(items, heat_stats, trace_id)
    return heat_stats


# ---------- 缓存维护 ----------

def warm_embed_cache(limit: int = 100000) -> dict:
    """把 Phase1 全量候选的向量预先算好并落盘（冷启动实测约 11s）。

    跑完之后所有检索都只剩 query 向量开销（约 2~3s）。
    CLI 入口见 services.B4_dual_gate_retrieval.build_cache
    """
    t0 = time.time()
    candidates = phase1_filter(limit=limit)
    stats = _embed_candidates(candidates)
    stats["candidates"] = len(candidates)
    stats["wall_ms"] = round((time.time() - t0) * 1000, 1)
    stats["cache"] = embed_cache.stats()
    return stats
