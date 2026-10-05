"""B3 表头引擎 — SQLite 主索引"""
from .sqlite_store import (
    init_db, ensure_schema, get_conn, insert_memory, bulk_insert,
    update_heat_count, update_layer, update_heat_all,
    phase1_filter, list_all_members, get_heat_percentile,
    count_total, get_by_id, get_layer_distribution, is_sqlite_ready,
    get_by_ids,
    # 命中即扩窗（2026-10-02，E 形态）：轮级排序 + 会话补全的"补全"那一半
    neighbours_of,
    # FTS5 相关性预筛（2026-09-26）：候选生成换一种方式，替代"按热度截断"
    ensure_fts, fts_available, fts_count, fts_rebuild, fts_search, fts_upsert_ids,
    # 路4 原文索引（2026-09-30，V1.3 增量 1）：索引"原文实际说的语言"，
    # 而不是摘要 —— 实测词法路 recall@15 11.9% → 63.9%
    ensure_raw_fts, raw_available, raw_count, raw_rebuild, raw_search,
    raw_upsert_ids, raw_stats, raw_tokens, raw_lang, raw_heads,
)
from .sqlite_store import _integrity_hash, _memory_id, write_epoch
from .cold_locate import (
    memory_id_for, hashlib_md5_12, locate_jsonl_entry, day_index, source_text,
)
