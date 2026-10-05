-- B3 表头引擎 · SQLite Schema（**参赛版 / AML**）
-- 由 `shufang-os/v2` 的 schema 派生；相对原版**只加了一列 user_id**（隔离边界）。
-- audit_log / frames 两张表**保留定义但不写入** —— 参赛路径已无 B7，
-- 但 B3 的 `write_epoch` / `_integrity_hash` 仍引用它们，删表会让那几个函数炸。

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- ============================================================
-- memories · 记忆主表（物理防火墙）
-- 10 万条规模带过滤查询目标 < 10ms
-- ============================================================
CREATE TABLE IF NOT EXISTS memories (
    memory_id       TEXT PRIMARY KEY,                -- 全局唯一 ID (md5 或 uuid4)
    -- ★★ 参赛版新增：AML 的**唯一检索隔离边界**
    --   官方契约原文："`user_id` is the sole retrieval isolation boundary"。
    --   原版没有这一列 —— 生产是单用户的，评测靠 `session_id` 限定 haystack。
    --   在 AML 里每个 Add 都带 user_id，**所有四个召回路都必须按它过滤**，
    --   否则就是跨用户泄漏（成绩作废）。
    user_id         TEXT,                            -- 隔离边界（AML Add 必带）
    layer           TEXT NOT NULL DEFAULT 'cold',    -- cold/warm/hot
    content_path    TEXT,                            -- 原文路径（冷记忆文件位置）
    summary         TEXT,                            -- 文本本体（参赛版=原文）
    task_type       TEXT,                            -- 七类：研究/决策/验证/规划/闲聊/求助/其他
    session_id      TEXT,                            -- 来源会话（AML Add 必带）
    time_bucket     TEXT,                            -- YYYY-MM 分桶，加速按月过滤
    core_entities   TEXT NOT NULL DEFAULT '[]',     -- JSON 数组，核心名词/项目/技术
    emotional_tone  TEXT,                            -- 情感基调（如中性/积极/焦虑）
    source_weight   REAL NOT NULL DEFAULT 1.0,      -- 来源权重（用户确认可加）
    token_peaks     INTEGER NOT NULL DEFAULT 0,      -- token 峰值（用于预算裁剪参考）
    heat            REAL NOT NULL DEFAULT 0.0,        -- ⚠️ 参赛版**恒为 0**（B5 已移除，不参与打分）
    count           INTEGER NOT NULL DEFAULT 0,       -- ⚠️ 参赛版**恒为 0**（无人反馈）
    legacy_retrieval_count INTEGER NOT NULL DEFAULT 0, -- ⚠️ 参赛版**恒为 0**（语义已作废）
    confidence      REAL NOT NULL DEFAULT 1.0,        -- 置信度（0~1）
    created_at      TEXT NOT NULL,                   -- ISO 8601（AML 由 Unix 毫秒时间戳归一而来）
    updated_at      TEXT NOT NULL,                   -- ISO 8601
    data_sovereignty TEXT NOT NULL DEFAULT 'app',      -- 数据主权：app/user
    relations       TEXT NOT NULL DEFAULT '[]',      -- Phase2: JSON 数组，人物/概念关系链
    source_date     TEXT,                            -- Phase2: YYYY-MM-DD 源文件日期
    has_l3          INTEGER NOT NULL DEFAULT 0        -- Phase2: 1=content_path 文件存在
);

-- Phase1 过滤核心索引（WHERE 组合条件）
CREATE INDEX IF NOT EXISTS idx_memories_task_type  ON memories(task_type);
CREATE INDEX IF NOT EXISTS idx_memories_time_bucket ON memories(time_bucket);
CREATE INDEX IF NOT EXISTS idx_memories_layer       ON memories(layer);
-- 复合索引：覆盖检索最常见的过滤组合
CREATE INDEX IF NOT EXISTS idx_memories_combo  ON memories(task_type, layer, time_bucket);
CREATE INDEX IF NOT EXISTS idx_memories_layer_task ON memories(layer, task_type);

-- ★★ 参赛版：隔离边界索引（**所有召回路径的第一道 WHERE**）
CREATE INDEX IF NOT EXISTS idx_memories_user        ON memories(user_id);
-- (user_id, session_id)：隔离 + 会话限定是参赛检索最常见的组合；
-- 且「命中即扩窗」的 `WHERE user_id=? AND session_id=? AND rowid<=?` 也吃这个索引。
CREATE INDEX IF NOT EXISTS idx_memories_user_session ON memories(user_id, session_id);
-- ★★ 2026-10-02 补（原版）：`session_id` 索引 —— 扩窗靠它做范围查。
--    没有它时 `WHERE session_id=? AND rowid<=?` 会一路向前扫，
--    实测代价：全量跑到 **1 题/分钟**（LLM 只占 12.7s），整卷要 8 小时以上。
CREATE INDEX IF NOT EXISTS idx_memories_session     ON memories(session_id);

-- core_entities JSON 查询支持（SQLite 3.38+ 的 json_each）
-- 不建索引，运行时用 json_each(memories.core_entities) 过滤
-- 10 万行以上考虑 FTS5 虚拟表，但当前规模单列索引 + 复合索引够用

-- ============================================================
-- audit_log · 审计日志（全动作覆盖）
-- 完整性保护：integrity_hash = sha256(header_snapshot || payload_ref)
-- ============================================================
CREATE TABLE IF NOT EXISTS audit_log (
    -- 主键是 (trace_id, action)，不是 trace_id 单列。
    --
    -- 为什么：架构书 §5.9 要求审计「覆盖 search/write/drilldown/export/delete 全动作」，
    -- 一条 trace 里本就会发生多个动作 —— 例如「检索命中(search) + 命中升温改库(write)」
    -- 是同一条因果链的两条证据。若主键只有 trace_id，配合 INSERT OR REPLACE 会互相覆盖：
    -- 实测升温和检索共用 trace_id 时，后写的把先写的顶掉，**检索记录直接消失**。
    trace_id          TEXT NOT NULL,                  -- 追踪 ID（同一链路的多个动作共用）
    request_id        TEXT,                           -- 请求 ID
    session_id        TEXT,                           -- 会话 ID
    ts                TEXT NOT NULL,                   -- ISO 8601
    actor             TEXT,                           -- 动作发起者（agent/user/system）
    action            TEXT NOT NULL,                  -- 动作类型：search/write/drilldown/export/delete
    header_snapshot   TEXT,                           -- 动作发生时的表头快照（JSON）
    payload_ref       TEXT,                           -- 载荷引用（memory_id 列表或文件路径）
    decided_by        TEXT NOT NULL DEFAULT 'user',   -- 硬规则：记忆层只执行，决策权在用户
    integrity_hash    TEXT NOT NULL,                  -- sha256(header_snapshot + payload_ref)
    PRIMARY KEY (trace_id, action)
);

CREATE INDEX IF NOT EXISTS idx_audit_log_action ON audit_log(action);
CREATE INDEX IF NOT EXISTS idx_audit_log_ts     ON audit_log(ts);
CREATE INDEX IF NOT EXISTS idx_audit_log_trace  ON audit_log(trace_id);

-- ============================================================
-- frames · 协议帧（MAP 三包落地）
-- 版本化的请求/响应包，支持 replay、审计签名
-- ============================================================
CREATE TABLE IF NOT EXISTS frames (
    frame_id          TEXT PRIMARY KEY,              -- 帧 ID
    frame_type        TEXT NOT NULL,                  -- request/response/drilldown
    protocol_ver      TEXT NOT NULL DEFAULT 'v0.1',   -- MAP 版本
    header_json       TEXT NOT NULL,                  -- 帧头（JSON）
    metadata_json     TEXT,                           -- 元数据（JSON）
    payload_path      TEXT,                           -- 载荷文件路径（或内联）
    footer_crc32      TEXT,                           -- 帧尾 CRC32 校验
    audit_signature   TEXT,                           -- 审计签名（关联 audit_log.trace_id）
    created_at        TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_frames_type     ON frames(frame_type);
CREATE INDEX IF NOT EXISTS idx_frames_created  ON frames(created_at);
CREATE INDEX IF NOT EXISTS idx_frames_audit    ON frames(audit_signature);
