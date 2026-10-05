"""B3 表头引擎 · SQLite 存储层

主索引：SQLite（本文件）   ← V2 新主管道
Fallback：Whoosh（memory_indexer_legacy.py） ← V1 兼容，不阻塞

表结构见 schema.sql。
"""
import os, re, sys, sqlite3, hashlib, json, time, uuid, datetime, threading
from typing import Optional


_DEFAULT_DB_PATH = "data/index/header.db"
_lock = threading.Lock()

# 进程内写入计数器：任何经 B3 的写入都 +1。
# 用途：上层（B4 的四路融合）要把"实体倒排 / 日期表"缓存在进程内，否则
# **每次查询都要全表扫一遍**。而"缓存什么时候该失效"必须有依据 ——
# `count(*)` 只在增删时变，**内容更新（core_entities / source_date 被重新提炼）
# 不会变行数**，只看行数会让缓存静默陈旧。
# ⚠️ 只覆盖**本进程**的写入。跨进程直连同一个 header.db 的写入不在计数内 ——
#    与本模块既有的 `_FTS_READY_CACHE` 是同一类已知边界，都在进程生命周期内收敛。
_WRITE_EPOCH = 0


def write_epoch() -> int:
    """本进程经 B3 的写入次数（供上层的进程内缓存判断失效）。"""
    return _WRITE_EPOCH


def _get_db_path() -> str:
    """从 config 解析，fallback 到 v2/data/index/header.db"""
    try:
        from services.shared.config.loader import get_config
        cfg = get_config()
        idx_dir = cfg.get("paths", {}).get("index_dir", "./data/index")
        os.makedirs(idx_dir, exist_ok=True)
        return os.path.join(idx_dir, "header.db")
    except Exception:
        p = os.path.join(_DEFAULT_DB_PATH.replace("/", os.sep))
        os.makedirs(os.path.dirname(p), exist_ok=True)
        return p


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _memory_id(date_str: str, entry_id: str) -> str:
    """生成 memory_id：md5(date#entry_id) — 与 V1 温记忆一致。

    ⚠️ 2026-09-30：实现已**收敛**到 `cold_locate.memory_id_for`（唯一一份）。
    这条规则原先在本仓有三份拷贝（这里 / `warm_indexer.memory_id_for` /
    B6 `hashlib_md5_12`），而路4 的原文索引还要按它反查冷库 —— 再抄一份就是
    第四处可能不一致的地方，不一致的表现是"反查不到"甚至**张冠李戴**。
    保留本函数名是为了不动调用方，语义逐字相同。
    """
    from .cold_locate import memory_id_for
    return memory_id_for(date_str, entry_id)


def _integrity_hash(header_snapshot: str, payload_ref: str) -> str:
    """P1-5 规定：sha256(header_snapshot + payload_ref)"""
    return hashlib.sha256(
        (header_snapshot or "") + "|" + (payload_ref or "")
    ).hexdigest()


# ---------- 连接与建库 ----------

def get_conn(db_path: Optional[str] = None) -> sqlite3.Connection:
    """获取连接（WAL 模式，线程安全）"""
    path = db_path or _get_db_path()
    with _lock:
        conn = sqlite3.connect(path, timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA synchronous = NORMAL")
        return conn


def init_db(db_path: Optional[str] = None) -> str:
    """落地 schema.sql，返回实际路径"""
    path = db_path or _get_db_path()
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, "r", encoding="utf-8") as f:
        ddl = f.read()
    conn = get_conn(path)
    conn.executescript(ddl)
    conn.close()
    print(f"  [B3] SQLite 表头已落地 -> {path}")
    return path


# ---------- schema 对齐（防漂移，2026-09-25）----------
#
# 为什么需要：`schema.sql` 是表结构的唯一真源，但 `init_db()` 此前**只被两个
# 一次性迁移脚本调用**，启动路径上没有任何入口。后果是「改了 schema.sql，
# 库没变」：09-23 加的两个复合索引就是靠手工跑迁移脚本才进库的（复核时库里有，
# 才没被发现是漏的）。而漂移本身**不会被任何验证脚本抓到** —— 它们测的是行为，
# 不是「schema 到底落地没有」，所以库缺列/缺索引时照样全绿。
#
# 这里把「启动时对齐」变成默认行为，分两步：
#   ① 幂等重放 `CREATE ... IF NOT EXISTS` → 新表、新索引自动进库；
#   ② 解析 schema.sql 里每个 CREATE TABLE 的列清单，与 `PRAGMA table_info`
#      比对，缺列自动 `ALTER TABLE ... ADD COLUMN`（SQLite 没有
#      `ADD COLUMN IF NOT EXISTS`，只能自己比）。
#
# 列表源在 schema.sql 里，Python 侧**不另存一份列名表** —— 两处手写正是漂移的温床。

_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(\w+)\s*\((.*?)\n\s*\)\s*;",
    re.IGNORECASE | re.DOTALL,
)
# 表级约束（不是列定义）：解析列清单时跳过这些行
_TABLE_CONSTRAINT_KEYWORDS = ("PRIMARY", "UNIQUE", "FOREIGN", "CHECK", "CONSTRAINT")


def _parse_declared_columns(ddl: str) -> dict:
    """解析出 {表名: [(列名, 列定义), ...]}（只取 CREATE TABLE 的列清单）。"""
    declared = {}
    for match in _CREATE_TABLE_RE.finditer(ddl):
        table, body = match.group(1), match.group(2)
        columns = []
        for raw_line in body.splitlines():
            # 去掉行内 `--` 注释与结尾逗号；审计表的主键说明就是一段多行注释
            line = raw_line.split("--", 1)[0].strip().rstrip(",").strip()
            if not line:
                continue
            if line.split()[0].upper() in _TABLE_CONSTRAINT_KEYWORDS:
                continue
            columns.append((line.split()[0], line))
        declared[table] = columns
    return declared


def ensure_schema(db_path: Optional[str] = None) -> dict:
    """把库结构对齐到 schema.sql（幂等，启动时调）。

    Returns: {"added_columns": [...], "skipped_columns": [...]}
        skipped 是「缺列但无法自动补」的（NOT NULL 且无 DEFAULT，SQLite 的
        ALTER 加不了）—— 这种必须人工介入，所以记下来而不是悄悄跳过。
    """
    path = db_path or _get_db_path()
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, "r", encoding="utf-8") as f:
        ddl = f.read()

    conn = get_conn(path)
    report = {"added_columns": [], "skipped_columns": []}
    try:
        conn.executescript(ddl)
        for table, columns in _parse_declared_columns(ddl).items():
            existing = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
            if not existing:
                continue  # 表不存在（executescript 失败时才会），不在此处掩盖
            for name, definition in columns:
                if name in existing:
                    continue
                upper = definition.upper()
                if "NOT NULL" in upper and "DEFAULT" not in upper:
                    report["skipped_columns"].append(f"{table}.{name}")
                    continue
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {definition}")
                report["added_columns"].append(f"{table}.{name}")
    finally:
        conn.close()

    # FTS5 相关性预筛表：不存在就建 + 首次索引（存量库也能一次补齐）。
    # 不支持 FTS5 的环境只告警、不报错 —— 检索会退化成原来的 SQL 候选。
    try:
        ensure_fts()
    except Exception:  # noqa: BLE001
        pass

    # 路4 原文索引（2026-09-30）：同样"不存在就建 + 首次回填"。
    #
    # ⚠️ 为什么必须在 `ensure_schema` 里就把它建起来：一个**没被维护的索引**比
    # 没有索引更坏 —— 它会被当成"有这一路"来用，然后静默给出陈旧结果
    # （步骤 2 那次残索引 419/5871 就是这么把结论带反的）。
    # 这一步与 `ensure_fts` 同形：失败只告警，检索退化为没有这一路。
    try:
        ensure_raw_fts()
    except Exception:  # noqa: BLE001
        pass

    # 一律走 stderr：stdio 模式下 stdout 是 JSON-RPC 通道，写 stdout 会破坏协议。
    if report["added_columns"]:
        print(f"  [B3] schema 对齐：补列 {report['added_columns']} -> {path}", file=sys.stderr)
    if report["skipped_columns"]:
        print(
            f"  [B3] ⚠️ 以下列在 schema.sql 里但无法自动补（NOT NULL 无 DEFAULT），需人工处理："
            f"{report['skipped_columns']}",
            file=sys.stderr,
        )
    return report


# ---------- memories 写入 ----------
#
# ⚠️ 为什么不是 INSERT OR REPLACE（2026-09-22 修）
# ---------------------------------------------
# `memories` 表里混了**两类字段**：
#   * 索引器写的「内容/表头」：summary / content_path / task_type / core_entities …
#   * 运行时状态：`heat` / `count` / `updated_at`（B4 升温、B5 衰减的状态机唯一依据），
#     以及 `layer`（B5 按热度百分位派生的层）、`data_sovereignty`（主权，用户设过就不能被改回 app）。
#
# `INSERT OR REPLACE` 会把**整行替换**掉：温层重索引时，那条记忆的 heat/count
# 会被温层文件里的旧值（实测都是 0）覆盖 —— 实测确认：把 34 行 heat 堆到 306 之后
# 再跑一次 `index_warm()`，sum(heat) 直接回 0，`count` 也回 0。
# 而 `warm_indexer.index_warm()` 每次都把**全部**温层条目重写一遍，
# 于是「检索命中 → 升温 → 常用浮现」这条反馈回路每次提炼都会被清空。
#
# 所以改成 UPSERT：**冲突时只更新内容字段，运行时状态一律保留**。
_MEMORY_UPSERT_SQL = """INSERT INTO memories
         (memory_id, user_id, layer, content_path, summary, task_type, session_id,
          time_bucket, core_entities, emotional_tone, source_weight,
          token_peaks, heat, count, confidence, created_at, updated_at, data_sovereignty,
          relations, source_date, has_l3)
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
         ON CONFLICT(memory_id) DO UPDATE SET
             content_path   = excluded.content_path,
             summary        = excluded.summary,
             task_type      = excluded.task_type,
             session_id     = excluded.session_id,
             time_bucket    = excluded.time_bucket,
             core_entities  = excluded.core_entities,
             emotional_tone = excluded.emotional_tone,
             source_weight  = excluded.source_weight,
             token_peaks    = excluded.token_peaks,
             confidence     = excluded.confidence,
             created_at     = excluded.created_at,
             relations      = excluded.relations,
             source_date    = excluded.source_date,
             has_l3         = excluded.has_l3
         -- 刻意**不**更新的列：heat / count / updated_at（B4·B5 运行时状态，
         -- 且 updated_at 是衰减基准时间）、layer（B5 派生）、data_sovereignty（数据主权）
         -- ★★ 参赛版再补一条：**user_id 也刻意不更新** —— 它是隔离边界，
         --    如果两条不同用户的数据算出同一个 memory_id（纯内容哈希就会这样），
         --    让后来的写入改写 user_id 等于把 A 的记忆挪给 B。
         --    正确做法是 memory_id 本身就把 user_id 拌进哈希（见 Add 服务）。
         """


def insert_memory(
    summary: str = "",
    task_type: str = "其他",
    session_id: str = "",
    time_bucket: Optional[str] = None,
    core_entities: Optional[list] = None,
    emotional_tone: str = "",
    source_weight: float = 1.0,
    content_path: str = "",
    confidence: float = 1.0,
    memory_id: Optional[str] = None,
    layer: str = "cold",
    user_id: str = "",
    created_at: Optional[str] = None,
) -> str:
    """插入一条记忆，返回 memory_id。

    已存在同 memory_id 时**只更新内容字段**，保留 heat/count/updated_at/layer
    （原因见上方 `_MEMORY_UPSERT_SQL` 的说明）。

    ★★ 参赛版新增两个参数，**它们都是 AML 契约里的必填信息**：

    * `user_id` —— 官方契约的**唯一隔离边界**（"sole retrieval isolation boundary"）。
    * `created_at`（ISO 8601）—— 由 Add 里的 **Unix 毫秒时间戳**归一而来。

    ⚠️ **`time_bucket` 与 `source_date` 一律从 `created_at` 派生，不再取"现在"**：
    原版由实时写入链调用，用"现在"是对的；但 AML 是**把历史一次性推给你**，
    若用摄取时刻当桶，所有记忆都会挤进本月 —— **时间题直接错**，
    而且 `time_bucket` 过滤（时间路的一部分）会失效。这是个静默错，必须在这里挡掉。
    """
    if created_at:
        created_at = str(created_at)
        if time_bucket is None:
            time_bucket = created_at[:7]              # YYYY-MM
    now = _now_iso()
    if memory_id is None:
        memory_id = uuid.uuid4().hex[:16]
    if time_bucket is None:
        time_bucket = datetime.datetime.now().strftime("%Y-%m")
    if core_entities is None:
        core_entities = []

    row = (
        memory_id, user_id, layer, content_path, summary, task_type, session_id,
        time_bucket, json.dumps(core_entities, ensure_ascii=False),
        emotional_tone, source_weight, 0, 0.0, 0, confidence,
        created_at or now, now, "app",
        "[]", (created_at or "")[:10], 0,  # relations, source_date(YYYY-MM-DD), has_l3
    )
    conn = get_conn()
    try:
        conn.execute(_MEMORY_UPSERT_SQL, row)
    finally:
        conn.close()
    # 单条写入（MCP `memory_write` / 旧 API）：只切这一条。
    # 这里若是全量重建，每写一条记忆都要 3~5 秒 —— 增量更新是必须的。
    fts_upsert_ids([memory_id])
    # 路4 原文索引同样只切这一条。**这一步会读一次冷库原文** —— 那是本方案
    # 唯一的显著代价（见 `PLAN-V1.3-增量1` §五 步骤 4「真正成本在写入路径」）。
    # 内部已捕获异常并返回 0：原文取不到时退回摘要，**绝不让写入失败**。
    try:
        raw_upsert_ids([memory_id])
    except Exception:  # noqa: BLE001 - 旁路增强绝不拖垮写入
        pass
    global _WRITE_EPOCH
    _WRITE_EPOCH += 1
    return memory_id


def bulk_insert(memories: list[dict]) -> int:
    """批量写入，返回行数。

    **UPSERT 语义**：同 memory_id 已存在时只更新内容字段，
    heat/count/updated_at/layer 保留（详见 `_MEMORY_UPSERT_SQL`）。
    这一点很关键 —— 原先的 `INSERT OR REPLACE` 会让「重跑一次温层索引」
    把全库累积的检索热度清零。
    """
    if not memories:
        return 0
    rows = []
    now = _now_iso()
    for m in memories:
        mid = m.get("memory_id") or uuid.uuid4().hex[:16]
        ca = str(m.get("created_at") or "")            # ★ 参赛版：桶/日期一律从记忆自身时间派生
        tb = m.get("time_bucket") or (ca[:7] if ca else
                                      datetime.datetime.now().strftime("%Y-%m"))
        ce = m.get("core_entities") or []
        rows.append((
            mid, m.get("user_id", ""),                       # ★ 隔离边界
            m.get("layer", "cold"), m.get("content_path", ""),
            m.get("summary", ""), m.get("task_type", "其他"),
            m.get("session_id", ""), tb,
            json.dumps(ce, ensure_ascii=False),
            m.get("emotional_tone", ""), m.get("source_weight", 1.0),
            m.get("token_peaks", 0), m.get("heat", 0.0),
            m.get("count", 0), m.get("confidence", 1.0),
            ca or now, now, "app",
            json.dumps(m.get("relations") or [], ensure_ascii=False),
            m.get("source_date", "") or (ca[:10] if ca else ""),
            1 if (m.get("content_path") and os.path.exists(m.get("content_path", ""))) else 0,
        ))
    conn = get_conn()
    try:
        conn.executemany(_MEMORY_UPSERT_SQL, rows)
    finally:
        conn.close()
    # 内容字段变了 → 同步这几条的 FTS 行。
    # 用**增量**而不是全量重建：这是批量入口，但也可能是"每次写一条"的调用方；
    # 全量重建在 5,871 行上要 3~5 秒（jieba 分词），增量只切这几条。
    fts_upsert_ids([r[0] for r in rows])
    # 路4 原文索引同样只切这几条（代价：读这几条的冷库原文）
    try:
        raw_upsert_ids([r[0] for r in rows])
    except Exception:  # noqa: BLE001 - 旁路增强绝不拖垮写入
        pass
    global _WRITE_EPOCH
    _WRITE_EPOCH += 1
    return len(rows)


def update_heat_count(memory_id: str, heat_delta: float = 0.0,
                      count_delta: int = 0) -> bool:
    """热度/次数更新（B5 调用）"""
    sql = """UPDATE memories
             SET heat = MAX(0, heat + ?),
                 count = count + ?,
                 updated_at = ?
             WHERE memory_id = ?"""
    conn = get_conn()
    try:
        cur = conn.execute(sql, (heat_delta, count_delta, _now_iso(), memory_id))
        return cur.rowcount > 0
    finally:
        conn.close()


def update_layer(memory_id: str, new_layer: str, touch: bool = True) -> bool:
    """层迁移：cold → warm → hot

    ⚠️ `touch=False` 的用途（2026-09-22）
    ----------------------------------
    `updated_at` 不是「最后修改时间」那么简单 —— 它是 B5 衰减的**基准时间**：

        不变式：heat 永远表示「截止 updated_at 时刻的热度」
        heat(t_now) = heat(t_old) × e^(−λ·(t_now − t_old))

    层迁移是**派生字段**的改动（layer 由 heat 百分位算出来），它**不改 heat**。
    如果它顺手把 updated_at 推到当下，这个不变式就断了：库里存的 heat 还是
    「t_old 时刻的热度」，时间戳却说是「现在」—— 于是 [t_old, now] 这段
    以后再也不会被衰减（heat 永远偏高，记忆沉不下去）。

    `migrate_layers()` 因此传 `touch=False`。默认 True 只是保持「写库就更新时间」
    的常规语义，给未来的普通写入用。
    """
    sql = ("UPDATE memories SET layer = ? WHERE memory_id = ?" if not touch else
           "UPDATE memories SET layer = ?, updated_at = ? WHERE memory_id = ?")
    params = (new_layer, memory_id) if not touch else (new_layer, _now_iso(), memory_id)
    conn = get_conn()
    try:
        cur = conn.execute(sql, params)
        return cur.rowcount > 0
    finally:
        conn.close()


def update_heat_all(heat_decay_func) -> int:
    """全库时间衰减（B5 调用）。heat_decay_func(old_heat, updated_at_ts) → new_heat"""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT memory_id, heat, updated_at FROM memories"
        ).fetchall()
        updated = 0
        now = time.time()
        for r in rows:
            try:
                ts = datetime.datetime.fromisoformat(r["updated_at"]).timestamp()
            except Exception:
                ts = now
            new_heat = heat_decay_func(r["heat"], ts, now)
            conn.execute(
                "UPDATE memories SET heat = ?, updated_at = ? WHERE memory_id = ?",
                (max(0.0, new_heat), _now_iso(), r["memory_id"])
            )
            updated += 1
        return updated
    finally:
        conn.close()


# ---------- Phase1 表头过滤 ----------

def _filter_where(task_types, time_buckets, layer, prefix: str = "",
                  session_ids=None, user_ids=None):
    """把 Phase1 的过滤条件拼成 WHERE 片段。

    抽出来是因为 `phase1_filter` 与 `fts_search` **必须用同一套条件** ——
    否则显式过滤会被 FTS 这一路绕过（2026-09-26 实测被 verify-b6 的
    「空结果 → 诚实返回空」用例抓到：调用方指定 `task_type='绝不存在的类型XYZ'`，
    SQL 返回 0 行，但 FTS 照样按词法命中塞了条目进来，`empty` 就变成 False 了）。

    ★★ 参赛版（AML）：新增 `user_ids`，**放在最前面** ——
    官方契约把 `user_id` 定为**唯一检索隔离边界**。它在语义上与
    `session_ids` 同类（"限定语料范围"），但**优先级更高**：
    session 是"同一用户的某段对话"，user 是"谁的数据"。
    只传 session 不传 user 的调用应当被视为**不安全**（见 `assert_isolation`）。
    """
    where, params = [], []
    # ★ 隔离边界必须第一优先。空列表 = 不限定（沿用旧行为，评测兼容）。
    if user_ids:
        ph = ",".join("?" * len(user_ids))
        where.append(f"{prefix}user_id IN ({ph})")
        params.extend(user_ids)
    if task_types:
        ph = ",".join("?" * len(task_types))
        where.append(f"{prefix}task_type IN ({ph})")
        params.extend(task_types)
    if time_buckets:
        ph = ",".join("?" * len(time_buckets))
        where.append(f"{prefix}time_bucket IN ({ph})")
        params.extend(time_buckets)
    if layer:
        where.append(f"{prefix}layer = ?")
        params.append(layer)
    # ★ 2026-10-01 加：按 `session_id` 过滤。
    #   为什么需要它：**LongMemEval 的每一道题都有自己的 haystack** ——
    #   一个库里躺着 500 道题共 ~24,000 个会话，检索时必须把候选限定在
    #   "本题自己的那 ~48 个会话"内，否则召回与论文口径完全不可比。
    #   （也适用于任何"一份库存多份互不相关语料"的批量评测。）
    if session_ids:
        ph = ",".join("?" * len(session_ids))
        where.append(f"{prefix}session_id IN ({ph})")
        params.extend(session_ids)
    return where, params


def _entity_ok(core_entities_raw, core_entity_any) -> bool:
    """`core_entity_any` 的语义与 phase1_filter 保持一致（JSON 数组包含任一即可）。"""
    if not core_entity_any:
        return True
    try:
        ents = set(json.loads(core_entities_raw or "[]") or [])
    except Exception:
        ents = set()
    return any(e in ents for e in core_entity_any)


def max_source_date(task_types=None, time_buckets=None, layer=None,
                    session_ids=None, user_ids=None) -> str:
    """**范围内最新的记忆日期**（YYYY-MM-DD），无记忆时返回 `''`。

    ★ 参赛版新增（2026-10-03）。存在的理由很具体：
    平台的回答提示词写着 "Convert relative times like 'yesterday', 'last month' …
    **when the memory timestamp makes it clear**" —— 但它**从不告诉模型"今天是几号"**，
    而 `/search` 的请求体只有 `{query, user_id, top_k}`，**也没有日期**。

    于是"几天前"这类题只能靠猜 → 实测 50 题里**4 道错在日期算术**
    （答 10 days 参考 4、答 14 days 参考 24、把相对表述换成绝对日期而与判词规则冲突）。

    这个函数给出的**不是**平台意义上的"今天"，而是"**你记得的最近一件事**"——
    在记忆系统里它是"现在"的最佳可得代理，而且**完全由我们自己的数据算出来**，
    不依赖平台多给我们任何字段。

    ⚠️ 与 `_filter_where` 用**同一套过滤条件**（含 `user_ids` 隔离）：
    否则 A 用户会看到 B 用户的"最近一天"，那既是泄漏也是错误的时间锚。
    """
    where, params = _filter_where(task_types, time_buckets, layer,
                                  session_ids=session_ids, user_ids=user_ids)
    sql = "SELECT MAX(source_date) FROM memories"
    if where:
        sql += " WHERE " + " AND ".join(where)
    conn = get_conn()
    try:
        row = conn.execute(sql, params).fetchone()
    except Exception:                      # noqa: BLE001 - 锚点算不出来不该让检索失败
        return ""
    finally:
        conn.close()
    return str((row[0] if row else "") or "")


def phase1_filter(
    task_types: Optional[list] = None,
    time_buckets: Optional[list] = None,
    layer: Optional[str] = None,
    core_entity_any: Optional[list] = None,
    limit: int = 1000,
    session_ids: Optional[list] = None,
    user_ids: Optional[list] = None,
) -> list[dict]:
    """Phase1: WHERE 组合过滤。把候选从全库压到百级。

    ⚠️ 2026-09-26 修：`ORDER BY heat DESC` 在**刚建好的库上等于没排序** ——
    新索引进来的行 heat 全是 0，并列时 SQLite 退化成插入顺序（=按日期），
    于是 `LIMIT` 截掉的是**日期靠后的整段记忆**，而不是"不重要的记忆"。
    实测 LoCoMo 阶段二（10 段合并、5,871 行、heat 全 0）：`LIMIT 1000` 只放行 **17%**，
    被截掉的正好是全部 2023 年（全库最大头），recall@15 因此从 ~55% 塌到 10.8%。
    修法：并列时再按 `updated_at DESC` 排 —— 至少让"新近的"优先存活，且结果**确定**
    （同一份库每次跑取到的候选集一致，便于复现和排障）。

    注意这只是把截断变确定、变合理，**没有解决"候选上限"本身**：
    真正的解法是 ①继续调大 limit（Phase2 是 O(n) 纯 Python 余弦）或
    ②上 ANN / FTS5 先做相关性预筛。见 `retrieval.retrieve` 的 `phase1_limit` 说明。
    """
    where, params = _filter_where(task_types, time_buckets, layer,
                                  session_ids=session_ids, user_ids=user_ids)

    sql = "SELECT * FROM memories"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY heat DESC, updated_at DESC LIMIT ?"
    params.append(limit)

    conn = get_conn()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    results = []
    for r in rows:
        row = dict(r)
        try:
            row["core_entities"] = json.loads(row.get("core_entities") or "[]")
        except Exception:
            row["core_entities"] = []

        # core_entity_any 额外过滤（JSON 包含）
        if core_entity_any:
            ents = set(row["core_entities"])
            if not any(e in ents for e in core_entity_any):
                continue

        results.append(row)
    return results


# ---------- Phase1 的相关性预筛：FTS5（2026-09-26 新增） ----------
#
# 为什么需要
# ----------
# 原 Phase1 是「`SELECT * FROM memories` [WHERE ...] `ORDER BY heat DESC, updated_at DESC LIMIT ?`」：
# 它**没有相关性概念**，只能按热度截断。在 LoCoMo 阶段二（10 段合并、5,871 行）实测：
# 上限 1,000 只放行 17% 的行，被截掉的是日期靠后的整段（新建库 heat 全 0 → 排序退化成日期序），
# `recall@15` 直接从 ~55% 塌到 **9.2%**，而且不报错。
# 把上限抬到 10,000 能救回召回（47.4%），但 Phase2 是纯 Python 余弦，
# 5,871 个候选就要 ~230ms，端到端中位 **311ms**，已越过「<300ms」的验收线。
#
# 所以真正要的不是"把上限调更大"，而是**换一种挑候选的方式**：按相关性挑，
# 而不是按热度截断。FTS5（SQLite 自带，**不需要 numpy/FAISS**）就能做这件事，
# 且 trigram 分词器对中文/英文专名都友好（对 `Joanna`、`Counter-Strike` 这类
# 库里实际存的英文专名可以直接子串命中）。
#
# 设计要点
# --------
# * **独立 FTS 表 + 显式 rebuild**，不做 external-content + 触发器：写入路径只有
#   `bulk_insert` / `insert_memory` 两处，rebuild 挂在那里足够，且不会因为触发器
#   在热路径（`update_heat_count` 每次检索都会走）上白跑。
# * **失败必须能退化**：FTS5 不可用（老 SQLite）、表不存在、MATCH 语法异常 ——
#   `fts_search` 一律返回 `[]`，调用方按原样走 SQL 路径，行为与今天完全一致。
# * **只做候选生成，不做排序**：返回 memory_id 列表，最终排序仍由 Phase2 的
#   向量余弦 + heat/count 决定。这样 FTS 只可能"多给几个候选"，不可能把
#   向量通道已经找到的好结果挤掉（取并集，不取交集）。

_FTS_TABLE = "memories_fts"
# ⚠️ 用 unicode61 + **自己先分词**，不用 trigram。原因（2026-09-26 实测）：
# trigram 的匹配单位是"3 个字符"，而中文里绝大多数实词是 **2 个字**
# （记忆 / 检索 / 插件 / 并行 …）—— jieba 切出来的词全部短于 3 字符，
# 于是要么被 tokenizer 忽略、要么被我这边的长度过滤丢掉，**中文查询几乎永远 0 命中**。
# 英文查询不受影响（单词 ≥3 字符），所以 LoCoMo 上测不出来 —— 属于"只有真实中文库才暴露"的坑。
# 改法：索引时把 `summary + core_entities` 用 jieba 切好、空格连接，再用 unicode61
# 按**词**建索引；查询侧同样切词。这样 2 字中文词和英文单词都是正常 token。
_FTS_TOKENIZER = "unicode61"
_FTS_MAX_TOKENS = 24            # 一次 MATCH 里最多放几个 token（约束查询代价）

# 停用词：这些词在几乎所有记忆里都出现，放进 OR 只会把 BM25 拉平、白烧时间。
# 中英各取最小必要集合 —— 目标是"别让疑问词主导匹配"，不是做完整 NLP。
_FTS_STOPWORDS = {
    "what", "when", "where", "who", "whom", "whose", "why", "how", "which",
    "did", "does", "do", "is", "are", "was", "were", "be", "been", "being",
    "the", "a", "an", "and", "or", "of", "to", "in", "on", "at", "for", "with",
    "by", "from", "as", "that", "this", "these", "those", "it", "its", "he",
    "she", "they", "them", "his", "her", "their", "you", "your", "we", "our",
    "about", "into", "than", "then", "there", "here", "not", "no", "yes",
    "的", "了", "是", "在", "和", "与", "有", "我", "你", "他", "她", "它",
    "什么", "怎么", "为什么", "哪", "哪个", "多少", "以及", "这个", "那个",
}


def _fts_tokens(text: str) -> list[str]:
    """把 query 切成 token（索引侧用同一个切法，两边必须一致）。

    优先 jieba；不在就退化成"按非字母数字切"。
    ⚠️ **不要**加 `len(w) < 3` 这类过滤：中文实词大多 2 字，
    过滤掉就等于把中文查询全废掉（这正是从 trigram 换过来的原因）。
    """
    raw = (text or "").strip()
    if not raw:
        return []
    words: list[str] = []
    try:
        import jieba
        words = [w.strip() for w in jieba.cut(raw)]
    except Exception:
        words = re.split(r"[^0-9A-Za-z\u4e00-\u9fff]+", raw)
    out: list[str] = []
    for w in words:
        if not w or w.lower() in _FTS_STOPWORDS:
            continue
        if not re.search(r"[0-9A-Za-z\u4e00-\u9fff]", w):
            continue
        if len(w) < 2:          # 单字符（如"的""a"）噪声太大
            continue
        if w not in out:
            out.append(w)
        if len(out) >= _FTS_MAX_TOKENS:
            break
    return out


def _fts_seg(text: str) -> str:
    """索引侧的分词结果：`" ".join(_fts_tokens(...))`。

    索引与查询必须用**同一个**切词函数，否则 token 对不上、
    表现为"明明有这条记忆却搜不到"，而且不报错。
    """
    return " ".join(_fts_tokens(text))


def _ensure_fts_table() -> bool:
    """只建表、不填数据（`fts_rebuild` / `fts_upsert_ids` 的前置步骤，避免递归）。"""
    conn = None
    try:
        conn = get_conn()
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                        (_FTS_TABLE,)).fetchone():
            return True
        conn.execute(f"CREATE VIRTUAL TABLE {_FTS_TABLE} USING fts5("
                     f"seg, tokenize='{_FTS_TOKENIZER}')")
        return True
    except Exception as error:  # noqa: BLE001
        print(f"  [B3] ⚠️ FTS5 不可用（将退化为原 SQL 候选）："
              f"{type(error).__name__}: {error}", file=sys.stderr)
        return False
    finally:
        if conn is not None:
            conn.close()


def ensure_fts() -> bool:
    """确保 FTS 表存在；**新建时立刻全量索引**（存量库一次补齐）。

    为什么是**独立表 + 自己分词**，不是外部内容表
    ------------------------------------------------
    外部内容表只能索引 `memories` 里原样存在的列；而我们要索引的是
    "jieba 切好的文本"——那一列在 `memories` 里并不存在。
    （外部内容表的好处是 `'rebuild'` 能一条命令重填，但换不来分词。）

    ⚠️ 独立表上 `INSERT INTO fts(fts) VALUES('rebuild')` 是**无效操作**：
    既不报错也不填一行数据。实测踩到过——表建好了、`fts_count()=0`、
    查询永远返回空，而 `fts_search` 按设计静默返回 `[]`，
    于是整条 FTS 通道"看起来在跑、实际从没生效"。所以这里必须显式 INSERT，
    并且 `fts_rebuild()` 会把行数返回给调用方核对。

    Returns: True=可用 / False=环境不支持（调用方据此退化）。
    """
    if not _ensure_fts_table():
        return False
    try:
        conn = get_conn()
        n = conn.execute(f"SELECT count(*) FROM {_FTS_TABLE}").fetchone()[0]
        total = conn.execute("SELECT count(*) FROM memories").fetchone()[0]
        conn.close()
    except Exception:  # noqa: BLE001
        return False
    if n == 0 and total > 0:
        got = fts_rebuild()
        print(f"  [B3] FTS5 预筛表已建并完成首次索引（{got} 行）-> {_FTS_TABLE}",
              file=sys.stderr)
    return True


def fts_rebuild() -> int:
    """全量重建 FTS 索引，返回行数。-1 表示不可用。

    分词在 Python 侧做（jieba）—— 这是"独立表"的代价，换来的是中文 2 字词可检索。
    5,871 行实测约 3~5 秒（jieba 冷启动另算），只在建表/大批写入后跑。
    """
    if not _ensure_fts_table():
        return -1
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT rowid, summary, core_entities FROM memories").fetchall()
        batch = []
        for r in rows:
            try:
                ents = json.loads(r["core_entities"] or "[]")
            except Exception:
                ents = []
            if not isinstance(ents, list):
                ents = []
            seg = _fts_seg((r["summary"] or "") + " " + " ".join(
                e for e in ents if isinstance(e, str)))
            if seg:
                batch.append((r["rowid"], seg))
        conn.execute(f"DELETE FROM {_FTS_TABLE}")
        conn.executemany(f"INSERT INTO {_FTS_TABLE}(rowid, seg) VALUES (?,?)", batch)
        return int(conn.execute(f"SELECT count(*) FROM {_FTS_TABLE}").fetchone()[0])
    except Exception as error:  # noqa: BLE001
        print(f"  [B3] FTS 重建失败：{type(error).__name__}: {error}", file=sys.stderr)
        return -1
    finally:
        conn.close()


def fts_upsert_ids(memory_ids: list) -> int:
    """只更新这几条记忆的 FTS 行（写完库后调用），避免每次写入全量重建。

    单条写入（MCP `memory_write`）走这里就是 O(1) 次分词，
    而不是 O(全库) —— 否则每写一条记忆都要 3~5 秒。
    """
    ids = [i for i in dict.fromkeys(memory_ids or []) if i]
    if not ids or not _ensure_fts_table():
        return 0
    conn = get_conn()
    try:
        touched = 0
        for i in range(0, len(ids), 400):
            chunk = ids[i:i + 400]
            ph = ",".join("?" * len(chunk))
            for r in conn.execute(
                    f"SELECT rowid, summary, core_entities FROM memories "
                    f"WHERE memory_id IN ({ph})", chunk):
                try:
                    ents = json.loads(r["core_entities"] or "[]")
                except Exception:
                    ents = []
                if not isinstance(ents, list):
                    ents = []
                seg = _fts_seg((r["summary"] or "") + " " + " ".join(
                    e for e in ents if isinstance(e, str)))
                conn.execute(f"DELETE FROM {_FTS_TABLE} WHERE rowid=?", (r["rowid"],))
                if seg:
                    conn.execute(
                        f"INSERT INTO {_FTS_TABLE}(rowid, seg) VALUES (?,?)",
                        (r["rowid"], seg))
                touched += 1
        return touched
    except Exception as error:  # noqa: BLE001
        print(f"  [B3] FTS 增量更新失败：{type(error).__name__}: {error}", file=sys.stderr)
        return 0
    finally:
        conn.close()


def fts_available() -> bool:
    conn = get_conn()
    try:
        return bool(conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (_FTS_TABLE,)).fetchone())
    except Exception:
        return False
    finally:
        conn.close()


def fts_count() -> int:
    """FTS 表里有多少行（自检用：应该等于 memories 行数）。"""
    conn = get_conn()
    try:
        return int(conn.execute(f"SELECT count(*) FROM {_FTS_TABLE}").fetchone()[0])
    except Exception:
        return -1
    finally:
        conn.close()


def fts_search(query_text: str, k: int = 200, task_types=None, time_buckets=None,
               layer=None, core_entity_any=None, session_ids=None,
               user_ids=None) -> list[str]:
    """按 BM25 取相关性最高的 k 个 memory_id。

    ⚠️ **必须带上调用方的显式过滤条件**（`task_types` / `time_buckets` / `layer` /
    `core_entity_any`），与 `phase1_filter` 用同一套语义。否则 FTS 这一路会把
    调用方明确排除掉的记忆塞回候选里 —— 那是**静默绕过过滤契约**，
    而且会让 `empty=True`（硬规则 2：筛完真没有就诚实说没有）失效。

    **任何异常都返回 `[]`** —— 调用方会退化成原来的 SQL 候选，
    所以这个方法坏掉只会"少一路召回"，绝不会让检索失败。
    """
    tokens = _fts_tokens(query_text)
    if not tokens:
        return []
    # 用引号把每个 token 包成短语：避免 `-`/`*`/`OR` 之类被当 FTS5 语法。
    expr = " OR ".join('"' + t.replace('"', '""') + '"' for t in tokens)
    where, params = _filter_where(task_types, time_buckets, layer, prefix="m.",
                                  session_ids=session_ids, user_ids=user_ids)
    # core_entity_any 是 JSON 数组，只能在 Python 侧过滤 —— 那就多取一些再截，
    # 否则过滤完可能不足 k 条。
    fetch_k = int(k) * 3 if core_entity_any else int(k)
    sql = (f"SELECT m.memory_id AS mid, m.core_entities AS ents FROM {_FTS_TABLE} f "
           f"JOIN memories m ON m.rowid = f.rowid "
           f"WHERE {_FTS_TABLE} MATCH ?")
    if where:
        sql += " AND " + " AND ".join(where)
    sql += f" ORDER BY bm25({_FTS_TABLE}) LIMIT ?"
    conn = get_conn()
    try:
        rows = conn.execute(sql, [expr] + params + [fetch_k]).fetchall()
        out = []
        for r in rows:
            if not _entity_ok(r["ents"], core_entity_any):
                continue
            out.append(r["mid"])
            if len(out) >= int(k):
                break
        return out
    except Exception:  # noqa: BLE001 - 退化，不抛
        return []
    finally:
        conn.close()


# ============================================================
# 路4 原文索引（2026-09-30，V1.3 增量 1）
# ============================================================
# 为什么需要**第二张** FTS 表，而不是把现有 `memories_fts` 改一改
# ---------------------------------------------------------------
# 现有 `memories_fts` 索引的是 `summary + core_entities` —— 那是**摘要**。
# 实测（`locomo-test/_route2_lexical_verify.txt`，1974 题）：
#
#     索引用摘要（中文）   BM25 路 recall@15 = 11.9%
#     索引用原文（英文）   BM25 路 recall@15 = **63.9%**
#
# 差 5 倍。根因不是分词器（`seg` 早就是 jieba 预分词串，换分词器一个字都不会变），
# 而是**语言错配**：查询是英文，而摘要是中文 —— 词法匹配当然对不上。
#
# ★ 所以规律是「**索引原文实际说的语言**」，而不是"用某种固定语言索引"。
#   生产里摘要跟随用户/源语言，所以这条不是给 LoCoMo 打的补丁，而是让索引
#   不再被"提炼"这一步换掉语言。
#
# 实现要点（对齐 `PLAN-V1.3-增量1` §五 步骤 4 的「路4 语言方案」）
# ---------------------------------------------------------------
# 1. **一张表**，不按语言分表；中英 token 混存同一列 —— 现有表本来也中英混着。
# 2. **两侧都跑两套分词**（jieba + 英文按词），取并集去重。这样
#    「索引与查询必须同一切法」这条约束**不靠语言检测**来满足：
#    中文查询的 jieba token 命中中文文档，英文查询的英文 token 命中英文文档，
#    中英混排的文档两套 token 都在。语言标记只用于**诊断**，不参与路由。
# 3. **`lang` 软标记**存进 meta 表（CJK 占比），不阻止跨语言命中
#    （真实记忆常中英混排：`Counter-Strike` / `LGBTQ` / "用 jieba 分词"）。
# 4. **兜底链 + 记录用了哪个**：原文 → `text` → `summary`，`src` 如实落进 meta 表，
#    好让"没召回"可解释（是"库里没有原文"还是"检索没中"）。
# 5. **索引完整性可验证**：`raw_count()` / `raw_stats()` 必须能回答"装了多少"。
#    —— 步骤 2 踩过"残索引 419/5871 被静默复用、结论被带反"的坑，
#    所以这张表**拒绝在信息不足时假装成功**。

_RAW_FTS_TABLE = "memories_fts_raw"
_RAW_META_TABLE = "memories_raw_meta"
_RAW_MAX_TOKENS = 48            # 比摘要那路宽：原文更长，且中英两套 token 合并
# `head` 列存多少字符。⚠️ 必须 **≥ B4 的 `_RERANK_DOC_CHARS`（512）** ——
# 那个常量在 `retrieval.py` 里，而 B3 是下层、**不能反向 import B4**（会成环），
# 所以这里自己的常量与它对齐：**多存无害**（B4 会自己截断），存少了才会让重排看到残文。
_RAW_HEAD_CHARS = 512

_EN_STOP = set("""
a an the and or but if then than that this these those there here
is are was were be been being am do does did doing done have has had having
i you he she it we they me him her us them my your his its our their
what when where who whom whose why how which
of to in on at for with by from as into about over under again further once
not no nor only own same so too very can will just should now
""".split())


def _en_tokens(text: str, cap: int = 32) -> list:
    """英文侧分词：小写化、只留字母数字、去停用词、去重、封顶。

    ⚠️ **不要**加 `len(w) < 3` 这类过滤 —— 会和中文那路的 trigram 坑同源；
    这里保留 `>= 2`，丢掉的是 `I`/`a` 这种本来就无检索价值的。
    """
    out: list = []
    for w in re.findall(r"[a-z0-9]+", (text or "").lower()):
        if len(w) < 2 or w in _EN_STOP:
            continue
        if w not in out:
            out.append(w)
        if len(out) >= cap:
            break
    return out


def raw_tokens(text: str) -> list:
    """**两侧通用**的分词：jieba 与英文按词**取并集**，按小写去重，封顶。

    为什么并集而不是"先判语言再选一套"：
    判语言本身会错，而错了就表现为"静默搜不到"。并集的代价只是一点点索引体积，
    换来的是**不依赖语言检测的正确性** —— 中英混排（很常见）也自然覆盖。
    """
    out: list = []
    seen: set = set()

    def _add(words):
        for w in words:
            k = (w or '').lower()
            if not k or k in seen:
                continue
            seen.add(k)
            out.append(w)
            if len(out) >= _RAW_MAX_TOKENS:
                return True
        return False

    if _add(_fts_tokens(text)):
        return out
    _add(_en_tokens(text))
    return out


def raw_lang(text: str) -> str:
    """软语言标记（只用于诊断）：`zh` / `en` / `mix` / `unk`。"""
    t = text or ''
    cjk = len(re.findall(r'[\u4e00-\u9fff]', t))
    lat = len(re.findall(r'[A-Za-z]', t))
    tot = cjk + lat
    if not tot:
        return 'unk'
    r = cjk / tot
    if r >= 0.6:
        return 'zh'
    if r <= 0.1:
        return 'en'
    return 'mix'


def _ensure_raw_tables() -> bool:
    """只建表、不填数据（避免与 `raw_rebuild` 递归）。

    `head` 列：被选中文本的前 `_RERANK_DOC_CHARS` 个字符，**专供 cross-encoder 重排用**。
    为什么不重排时现读冷库：重排池是 30 条/查询，现读就是**每次查询 30 次冷库扫盘**
    （实测 stage2 一个日文件几十行，但真实库会更大）—— 那会把重排这一环变成
    热路径上最贵的一步。索引时存一次，重排时只读 SQLite。
    """
    conn = None
    try:
        conn = get_conn()
        conn.execute(f"CREATE VIRTUAL TABLE IF NOT EXISTS {_RAW_FTS_TABLE} "
                     f"USING fts5(seg, tokenize='{_FTS_TOKENIZER}')")
        conn.execute(f"CREATE TABLE IF NOT EXISTS {_RAW_META_TABLE} ("
                     f"rowid INTEGER PRIMARY KEY, memory_id TEXT UNIQUE, "
                     f"lang TEXT, src TEXT, n_tok INTEGER, chars INTEGER, head TEXT)")
        # 存量表补列（老库没有 head）
        cols = {r[1] for r in conn.execute(f"PRAGMA table_info({_RAW_META_TABLE})")}
        if 'head' not in cols:
            conn.execute(f"ALTER TABLE {_RAW_META_TABLE} ADD COLUMN head TEXT")
        return True
    except Exception as error:  # noqa: BLE001
        print(f"  [B3] ⚠️ 原文 FTS 表不可用（路4 将退化为只用摘要那路）："
              f"{type(error).__name__}: {error}", file=sys.stderr)
        return False
    finally:
        if conn is not None:
            conn.close()


def _raw_rows_for(memory_ids=None):
    """取 `[(memory_id, content_path, summary)]`；不给 id 就是全库。"""
    conn = get_conn()
    try:
        if memory_ids is None:
            return conn.execute(
                "SELECT memory_id, content_path, summary FROM memories").fetchall()
        ids = [i for i in dict.fromkeys(memory_ids or []) if i]
        out = []
        for i in range(0, len(ids), 400):
            chunk = ids[i:i + 400]
            ph = ",".join("?" * len(chunk))
            out += conn.execute(
                f"SELECT memory_id, content_path, summary FROM memories "
                f"WHERE memory_id IN ({ph})", chunk).fetchall()
        return out
    finally:
        conn.close()


def _raw_seg_for(mid, content_path, summary, day_cache):
    """算一条记忆的 `(seg, lang, src, n_tok, chars, head)`。

    `day_cache`：`{path: {memory_id: entry}}` —— 批量时**一个日文件只扫一遍**。
    逐条查是 O(记忆数 × 当天行数)，实测 stage2 那样要扫 ~600 万行；
    按文件扫一遍只要 ~6 千行。

    `head` = 被选中文本的前 `_RERANK_DOC_CHARS` 个字符，供 cross-encoder 重排
    （见 `_ensure_raw_tables` 的说明：不让重排去热路径上读冷库）。
    """
    from .cold_locate import day_index, source_text
    entry = None
    if content_path:
        if content_path not in day_cache:
            day_cache[content_path] = (day_index(content_path)[0]
                                       if os.path.exists(content_path) else {})
        entry = day_cache[content_path].get(mid)
    if entry is None:
        # 兜底链的第一跳就断了：没有可读原文 → 退回摘要，并如实记 `src`
        text = str(summary or '').strip()
        src = 'summary' if text else 'none'
    else:
        text, src = source_text(entry)
    toks = raw_tokens(text)
    return (' '.join(toks), raw_lang(text), src, len(toks), len(text),
            text[:_RAW_HEAD_CHARS])


def raw_rebuild(verbose: bool = False) -> dict:
    """全量重建原文索引。返回统计（**含每一档的条数**，便于核对）。

    Returns: `{rows, scanned, src, lang, no_text, missing_file, orphan}`
        * `rows`        真正写进 FTS 的行数
        * `src`         `{'orig': n, 'text': n, 'summary': n, 'none': n}` 兜底链各档
        * `missing_file` `content_path` 指向的文件不存在
        * `orphan`      有 `content_path` 但在那个日文件里反查不到（id 对不上）
    """
    if not _ensure_raw_tables():
        return {'rows': -1}
    rows = _raw_rows_for(None)
    day_cache: dict = {}
    stat = {'rows': 0, 'scanned': len(rows), 'no_text': 0, 'missing_file': 0,
            'orphan': 0, 'src': {}, 'lang': {}}
    batch, metabatch = [], []
    for i, r in enumerate(rows, 1):
        mid, cp, sm = r['memory_id'], r['content_path'] or '', r['summary'] or ''
        if cp and not os.path.exists(cp):
            stat['missing_file'] += 1
        seg, lang, src, n_tok, chars, head = _raw_seg_for(mid, cp, sm, day_cache)
        stat['src'][src] = stat['src'].get(src, 0) + 1
        stat['lang'][lang] = stat['lang'].get(lang, 0) + 1
        if cp and os.path.exists(cp):
            ent = (day_cache.get(cp) or {}).get(mid)
            if ent is None:
                stat['orphan'] += 1
        if not seg:
            stat['no_text'] += 1
            continue
        batch.append((i, seg))
        metabatch.append((i, mid, lang, src, n_tok, chars, head))
    conn = get_conn()
    try:
        conn.execute(f"DELETE FROM {_RAW_FTS_TABLE}")
        conn.execute(f"DELETE FROM {_RAW_META_TABLE}")
        conn.executemany(f"INSERT INTO {_RAW_FTS_TABLE}(rowid, seg) VALUES (?,?)", batch)
        conn.executemany(f"INSERT INTO {_RAW_META_TABLE}"
                         f"(rowid, memory_id, lang, src, n_tok, chars, head) "
                         f"VALUES (?,?,?,?,?,?,?)", metabatch)
        stat['rows'] = int(conn.execute(
            f"SELECT count(*) FROM {_RAW_FTS_TABLE}").fetchone()[0])
        return stat
    except Exception as error:  # noqa: BLE001
        print(f"  [B3] 原文索引重建失败：{type(error).__name__}: {error}", file=sys.stderr)
        stat['rows'] = -1
        return stat
    finally:
        conn.close()


def raw_upsert_ids(memory_ids: list) -> int:
    """只更新这几条记忆的原文索引行（写完库后调用）。

    与 `fts_upsert_ids` 同形：单条写入走这里就是 O(当天文件 + 该条分词)，
    而不是 O(全库) —— 否则每写一条记忆都要重扫整个冷库。
    """
    ids = [i for i in dict.fromkeys(memory_ids or []) if i]
    if not ids or not _ensure_raw_tables():
        return 0
    rows = _raw_rows_for(ids)
    day_cache: dict = {}
    touched = 0
    conn = get_conn()
    try:
        for r in rows:
            mid, cp, sm = r['memory_id'], r['content_path'] or '', r['summary'] or ''
            seg, lang, src, n_tok, chars, head = _raw_seg_for(mid, cp, sm, day_cache)
            old = conn.execute(f"SELECT rowid FROM {_RAW_META_TABLE} "
                               f"WHERE memory_id=?", (mid,)).fetchone()
            rid = old['rowid'] if old else None
            if rid is None:
                rid = (conn.execute(f"SELECT COALESCE(MAX(rowid),0)+1 "
                                    f"FROM {_RAW_META_TABLE}").fetchone()[0])
            conn.execute(f"DELETE FROM {_RAW_FTS_TABLE} WHERE rowid=?", (rid,))
            conn.execute(f"DELETE FROM {_RAW_META_TABLE} WHERE rowid=?", (rid,))
            if seg:
                conn.execute(f"INSERT INTO {_RAW_FTS_TABLE}(rowid, seg) VALUES (?,?)",
                             (rid, seg))
            conn.execute(f"INSERT INTO {_RAW_META_TABLE}"
                         f"(rowid, memory_id, lang, src, n_tok, chars, head) "
                         f"VALUES (?,?,?,?,?,?,?)",
                         (rid, mid, lang, src, n_tok, chars, head))
            touched += 1
        return touched
    except Exception as error:  # noqa: BLE001
        print(f"  [B3] 原文索引增量更新失败：{type(error).__name__}: {error}",
              file=sys.stderr)
        return 0
    finally:
        conn.close()


def raw_available() -> bool:
    conn = get_conn()
    try:
        return bool(conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (_RAW_FTS_TABLE,)).fetchone())
    except Exception:      # noqa: BLE001
        return False
    finally:
        conn.close()


def raw_count() -> int:
    """原文索引里有多少行（**自检用**：健康时应当 ≈ `memories` 行数）。"""
    conn = get_conn()
    try:
        return int(conn.execute(f"SELECT count(*) FROM {_RAW_FTS_TABLE}").fetchone()[0])
    except Exception:      # noqa: BLE001
        return -1
    finally:
        conn.close()


def raw_stats() -> dict:
    """原文索引现状：行数、库行数、兜底链各档、语言分布、无 token 条数。

    存在的理由与 `fts_count` 相同但更强：**"索引装了多少"必须能被问出来**。
    步骤 2 那次"残索引 419/5871 被静默复用"就是没法问这个问题才把结论带反的。
    """
    out = {'table': _RAW_FTS_TABLE, 'fts_rows': raw_count()}
    conn = get_conn()
    try:
        out['mem_rows'] = int(conn.execute(
            "SELECT count(*) FROM memories").fetchone()[0])
        try:
            out['meta_rows'] = int(conn.execute(
                f"SELECT count(*) FROM {_RAW_META_TABLE}").fetchone()[0])
            out['src'] = {r['src']: r['c'] for r in conn.execute(
                f"SELECT src, count(*) c FROM {_RAW_META_TABLE} GROUP BY src")}
            out['lang'] = {r['lang']: r['c'] for r in conn.execute(
                f"SELECT lang, count(*) c FROM {_RAW_META_TABLE} GROUP BY lang")}
            out['no_tok'] = int(conn.execute(
                f"SELECT count(*) FROM {_RAW_META_TABLE} WHERE n_tok=0").fetchone()[0])
        except Exception:  # noqa: BLE001 - meta 表可能是老的
            out['meta_rows'] = -1
    except Exception:      # noqa: BLE001
        out['mem_rows'] = -1
    finally:
        conn.close()
    if out.get('mem_rows', 0) > 0:
        out['coverage'] = out['fts_rows'] / out['mem_rows']
    return out


def ensure_raw_fts() -> bool:
    """确保原文索引存在；**新建时立刻全量回填**（存量库一次补齐）。"""
    if not _ensure_raw_tables():
        return False
    try:
        conn = get_conn()
        n = conn.execute(f"SELECT count(*) FROM {_RAW_FTS_TABLE}").fetchone()[0]
        total = conn.execute("SELECT count(*) FROM memories").fetchone()[0]
        conn.close()
    except Exception:      # noqa: BLE001
        return False
    if n == 0 and total > 0:
        st = raw_rebuild()
        print(f"  [B3] 原文索引已建并完成首次回填（{st['rows']} 行）-> "
              f"{_RAW_FTS_TABLE}", file=sys.stderr)
    return True


def raw_search(query_text: str, k: int = 200, task_types=None, time_buckets=None,
               layer=None, core_entity_any=None, session_ids=None,
               user_ids=None) -> list:
    """按 BM25 取相关性最高的 k 个 memory_id（**原文索引**版）。

    与 `fts_search` 同契约：**必须带上调用方的显式过滤条件**，任何异常返回 `[]`
    （调用方退化成没有这一路，绝不因为这一路坏掉让检索失败）。
    """
    tokens = raw_tokens(query_text)
    if not tokens:
        return []
    expr = " OR ".join('"' + t.replace('"', '""') + '"' for t in tokens)
    where, params = _filter_where(task_types, time_buckets, layer, prefix="m.",
                                  session_ids=session_ids, user_ids=user_ids)
    fetch_k = int(k) * 3 if core_entity_any else int(k)
    sql = (f"SELECT m.memory_id AS mid, m.core_entities AS ents "
           f"FROM {_RAW_FTS_TABLE} f "
           f"JOIN {_RAW_META_TABLE} x ON x.rowid = f.rowid "
           f"JOIN memories m ON m.memory_id = x.memory_id "
           f"WHERE {_RAW_FTS_TABLE} MATCH ?")
    if where:
        sql += " AND " + " AND ".join(where)
    sql += f" ORDER BY bm25({_RAW_FTS_TABLE}) LIMIT ?"
    conn = get_conn()
    try:
        rows = conn.execute(sql, [expr] + params + [fetch_k]).fetchall()
        out = []
        for r in rows:
            if not _entity_ok(r["ents"], core_entity_any):
                continue
            out.append(r["mid"])
            if len(out) >= int(k):
                break
        return out
    except Exception:      # noqa: BLE001 - 退化，不抛
        return []
    finally:
        conn.close()


def raw_heads(memory_ids: list) -> dict:
    """`{memory_id: head}` —— 供 cross-encoder 重排取"与查询同语言的文本"。

    为什么走这张表而不是现读冷库：重排池 30 条/查询，现读就是**每查询 30 次扫盘**；
    索引时存一次 `head`，重排时只读 SQLite（一次 IN 查询）。

    取不到就**不返回这个键** —— 调用方据此回退到 `_candidate_text`，
    而不是拿空串去重排（空文档会让 cross-encoder 给出无意义的分）。
    """
    ids = [i for i in dict.fromkeys(memory_ids or []) if i]
    if not ids:
        return {}
    out: dict = {}
    conn = get_conn()
    try:
        for i in range(0, len(ids), 400):
            chunk = ids[i:i + 400]
            ph = ",".join("?" * len(chunk))
            for r in conn.execute(
                    f"SELECT memory_id, head FROM {_RAW_META_TABLE} "
                    f"WHERE memory_id IN ({ph})", chunk):
                if r['head']:
                    out[r['memory_id']] = r['head']
        return out
    except Exception:      # noqa: BLE001 - 读不到就让调用方回退，不抛
        return out
    finally:
        conn.close()


# ---------- 全库查询（B5 四象限用） ----------

def list_all_members() -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT memory_id, heat, count, layer FROM memories"
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def get_heat_percentile(pct: float = 0.9) -> float:
    """取全库热度第 pct 百分位（B5 热层阈值）"""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT heat FROM memories ORDER BY heat DESC"
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        return 0.0
    idx = max(0, int(len(rows) * (1 - pct)))
    return rows[idx]["heat"]


def count_total() -> int:
    conn = get_conn()
    try:
        return conn.execute("SELECT COUNT(*) FROM memories").fetchone()[0]
    finally:
        conn.close()


def get_by_id(memory_id: str) -> Optional[dict]:
    """按 memory_id 查单条记录。返回 dict 或 None。"""
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM memories WHERE memory_id=?", (memory_id,)
        ).fetchone()
        if row:
            return dict(row)
        return None
    finally:
        conn.close()


def get_by_ids(memory_ids: list) -> list[dict]:
    """按 memory_id 批量取整行（FTS5 预筛只给 id，需要取回整行喂 Phase2）。

    ⚠️ `core_entities` 要像 `phase1_filter` 一样**解析成 list** ——
    Phase2 的 `_candidate_text()` 会 `" ".join(core_entities)`，
    给字符串的话会被逐字符 join 成一堆垃圾，而且不报错（静默算错向量）。
    """
    ids = [i for i in dict.fromkeys(memory_ids or []) if i]
    if not ids:
        return []
    out: list[dict] = []
    conn = get_conn()
    try:
        # 分批：SQLite 的变量上限默认 999，FTS 可能一次给回几百条
        for i in range(0, len(ids), 400):
            chunk = ids[i:i + 400]
            ph = ",".join("?" * len(chunk))
            for r in conn.execute(
                    f"SELECT * FROM memories WHERE memory_id IN ({ph})", chunk):
                row = dict(r)
                try:
                    row["core_entities"] = json.loads(row.get("core_entities") or "[]")
                except Exception:
                    row["core_entities"] = []
                if not isinstance(row["core_entities"], list):
                    row["core_entities"] = []
                out.append(row)
    finally:
        conn.close()
    return out


def neighbours_of(memory_ids: list, window: int = 2) -> list[dict]:
    r"""命中即扩窗：取这些记忆在**同一会话**内的邻近条目（会话内前后各 `window` 条）。

    ## 为什么要有它（实测依据，不是设计偏好）

    2026-10-02 LongMemEval 实测（同一批 500 题，唯一变量是**粒度**）：

    | | Recall@5 | Recall@10 |
    |---|---|---|
    | 会话粒度（一条记忆 = 一整个会话） | 89.4% | 91.8% |
    | **轮级**（一条记忆 = 一轮） | **97.0%** | **97.6%** |

    但**只交那一轮**会掉分：100 题分层实验里，纯轮交付在"检索本来就命中"的对照组
    **−4pp**，机制可回读（跨会话计数少算、时间线方向反、以及那条证据轮没进 top-10）。
    而"**用轮级信号找到会话、仍按会话交付**"两臂都不掉分、捞回组 **+58pp**。

    ⇒ 正解不是"把粒度改成轮"，而是「**按轮排序、按会话补全**」—— 本函数就是那个"补全"。

    ## ★ 窗口必须按**会话内位置**算，不能按全表 `rowid` 区间算

    第一版写的是 `WHERE session_id=? AND rowid BETWEEN hit-window AND hit+window`，
    **是错的**（被 `verify_expand_window.py` 当场抓住）：会话在表里是**交错**的
    （a1,a2,**b1**,a3,**b2**,a4），别的会话的行会**吃掉窗口配额**，
    于是"前后各 2 条"实际只取到 1 条 —— 而且**不报错**。

    生产上交错是常态（watcher 按真实到达顺序写入多个对话），
    评测库里"同一题的会话连续插入"才让这个 bug 藏住了。
    现在改成**两条按会话位置取的有界查询**：

        向前：WHERE session_id=? AND rowid <= hit_rowid ORDER BY rowid DESC LIMIT window+1
        向后：WHERE session_id=? AND rowid >  hit_rowid ORDER BY rowid ASC  LIMIT window

    两边都带 `LIMIT`，所以**代价与会话长度无关**（不会为了取邻居把长会话整段拉出来）。

    ## 为什么仍用 `rowid` 当序

    `rowid` 是插入序，而冷记忆 watcher 是**顺序写入**的，所以在同一个 `session_id` 内
    `rowid` ≈ 时间序 —— 不需要新增列、不需要改入库链。

    Args:
        memory_ids: 命中条目的 id（去重）。
        window: 会话内前后各取几条。`0` 直接返回 `[]`（= 关闭，行为与改动前一致）。

    Returns:
        邻近条目整行（`core_entities` 已解析成 list），**按会话分组、会话内 rowid 升序**。
        取不到就返回空/少 —— 调用方必须**容忍为空**，不许因此丢内容。
    """
    ids = [i for i in dict.fromkeys(memory_ids or []) if i]
    if not ids or window <= 0:
        return []
    out: list[dict] = []
    seen: set = set()

    def _push(r) -> None:
        row = dict(r)
        if row["memory_id"] in seen:
            return
        seen.add(row["memory_id"])
        try:
            row["core_entities"] = json.loads(row.get("core_entities") or "[]")
        except Exception:                                  # noqa: BLE001
            row["core_entities"] = []
        if not isinstance(row["core_entities"], list):
            row["core_entities"] = []
        out.append(row)

    conn = get_conn()
    try:
        for i in range(0, len(ids), 200):                  # SQLite 变量上限 999
            chunk = ids[i:i + 200]
            ph = ",".join("?" * len(chunk))
            hits = conn.execute(
                f"SELECT rowid, session_id FROM memories "
                f"WHERE memory_id IN ({ph})", chunk).fetchall()
            for h in hits:
                sid, rid = h["session_id"], h["rowid"]
                if not sid:
                    continue                               # 没会话键就无从"扩窗"，跳过（不猜）
                before = conn.execute(
                    "SELECT * FROM memories WHERE session_id=? AND rowid<=? "
                    "ORDER BY rowid DESC LIMIT ?",
                    (sid, rid, window + 1)).fetchall()
                after = conn.execute(
                    "SELECT * FROM memories WHERE session_id=? AND rowid>? "
                    "ORDER BY rowid ASC LIMIT ?",
                    (sid, rid, window)).fetchall()
                for r in list(reversed(before)) + list(after):   # 会话内升序
                    _push(r)
    finally:
        conn.close()
    return out


def get_layer_distribution() -> dict:
    """返回各层数量分布 {'L1': N, 'L2': N, 'L3': N}"""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT layer, COUNT(*) as c FROM memories GROUP BY layer"
        ).fetchall()
        return {r["layer"] or "unknown": r["c"] for r in rows}
    finally:
        conn.close()


# ---------- Fallback 开关 ----------

def is_sqlite_ready() -> bool:
    path = _get_db_path()
    if not os.path.exists(path):
        return False
    try:
        conn = get_conn()
        conn.execute("SELECT 1 FROM memories LIMIT 1")
        conn.close()
        return True
    except Exception:
        return False
