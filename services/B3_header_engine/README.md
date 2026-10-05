# B3 表头引擎 (Header Engine)

## 目标
记忆的物理防火墙 — SQLite 表头索引 + FAISS 向量索引。

## 当前状态
- `memory_indexer_legacy.py` — V1 Whoosh 实现，保留作为 Fallback

## V2 新增（待实现）
- Schema DDL: memories / audit_log / frames 三表
- heat/count 更新逻辑与四象限排序
- 10 万条过滤延迟 < 10ms
