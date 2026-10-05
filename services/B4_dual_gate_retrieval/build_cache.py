"""B4 向量缓存维护 CLI

用法（在 v2 目录下执行）：

  python -m services.B4_dual_gate_retrieval.build_cache --warm        # 预热全库向量
  python -m services.B4_dual_gate_retrieval.build_cache --stats       # 查看缓存概况
  python -m services.B4_dual_gate_retrieval.build_cache --search 书房 # 跑一次检索看耗时
  python -m services.B4_dual_gate_retrieval.build_cache --clear       # 清空（换嵌入模型后）

什么时候需要 --warm
-------------------
缓存是按 `sha256(模型名 + 文本)` 存的，所以：
- 正常检索会自动按需补算，不预热也能用（首次调用慢一点）
- 批量导入旧记忆后建议跑一次 --warm，避免第一次检索等待
"""
import argparse
import json

from . import embed_cache
from .retrieval import retrieve, warm_embed_cache


def main():
    p = argparse.ArgumentParser(description="B4 向量缓存维护")
    p.add_argument("--warm", action="store_true", help="预热：为全量候选生成并落盘向量")
    p.add_argument("--stats", action="store_true", help="查看缓存概况")
    p.add_argument("--clear", action="store_true", help="清空缓存（换嵌入模型后使用）")
    p.add_argument("--limit", type=int, default=100000, help="预热候选上限")
    p.add_argument("--search", default="", help="跑一次检索并打印耗时明细")
    a = p.parse_args()

    if a.clear:
        print(json.dumps({"cleared": embed_cache.clear()}, ensure_ascii=False))

    if a.warm:
        print("[B4] 预热中…（冷启动约 10~30s）")
        print(json.dumps(warm_embed_cache(a.limit), ensure_ascii=False))

    if a.search:
        r = retrieve(query_text=a.search, max_items=3)
        print(json.dumps({
            "query": a.search,
            "elapsed_ms": r["elapsed_ms"],
            "total_filtered": r["total_filtered"],
            "embed_stats": r["embed_stats"],
            "rank_stats": r["rank_stats"],
            "hits": [(i["memory_id"], i["_sim"], i["summary"][:40]) for i in r["items"]],
        }, ensure_ascii=False, indent=2))

    if a.stats or not any([a.warm, a.clear, a.search]):
        print(json.dumps(embed_cache.stats(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
