# -*- coding: utf-8 -*-
r"""参赛插件自检：把 **AML 契约**变成可执行断言。

为什么要有它（而不是"手工 curl 一下"）：
AML 的失败模式里有三种**不报错但成绩作废**的：

  ① **跨用户泄漏** —— 查询 A 返回了 B 的记忆。契约说 `user_id` 是唯一隔离边界；
     漏掉任何一条召回路（Phase1 / FTS 预筛 / 路2 实体 / 路3 时间 / 路4 词法）都会漏。
     这条**必须每次改动后重跑**：本仓历史上 `session_ids` 就因为漏传被绕过一次。
  ② **Add 返回 200 但没落库** —— 契约要求"persists before returning 200"。
     写进内存没落盘、或 DelayedWrite，都表现为"评测前半段好好的、后面全错"。
  ③ **交付文本不带时间戳** —— 平台固定的回答提示词要靠记忆里的时间戳换算相对时间；
     丢了它，时间类题目会整片错，但**本地跑题时看不出来**。

用法::

    python selftest.py            # 起临时服务，跑全部断言，退出码 0/1
    python selftest.py -v         # 额外打印每条断言
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

PASS, FAIL = [], []


def chk(name: str, cond: bool, extra: str = "") -> None:
    (PASS if cond else FAIL).append(name)
    mark = "OK  " if cond else "FAIL"
    print(f"  [{mark}] {name}{('  ' + extra) if extra else ''}")


def post(base: str, path: str, body: dict, key: str = "") -> tuple:
    req = urllib.request.Request(
        base + path, data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json",
                 **({"X-Api-Key": key} if key else {})}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read().decode("utf-8") or "{}")


def get(base: str, path: str) -> tuple:
    try:
        with urllib.request.urlopen(base + path, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        return error.code, {}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=18080)
    ap.add_argument("--keep", action="store_true", help="保留临时数据目录（排障用）")
    args = ap.parse_args()

    from services.C2_aml_service.server import AmlService, Handler

    data_dir = tempfile.mkdtemp(prefix="aml_selftest_")
    Handler.svc = AmlService(data_dir)
    Handler.api_key = "test-key"                      # 刻意开鉴权：顺便验 401
    httpd = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{args.port}"
    time.sleep(0.3)
    print(f"服务已起 {base} · data_dir={data_dir}\n")

    try:
        print("① 契约基础")
        code, health = get(base, "/health")
        chk("/health 免鉴权返回 2xx", code == 200, f"status={code}")
        chk("/health 报告模型可达性",
            "embed_reachable" in health and "rerank_reachable" in health,
            f"embed={health.get('embed_reachable')} rerank={health.get('rerank_reachable')}")
        code, _ = get(base, "/nope")
        chk("未知路径 404", code == 404)
        code, _ = post(base, "/search", {"query": "x", "user_id": "u"})
        chk("无 key 被拒（401）", code == 401, f"status={code}")

        print("\n② Add 的持久化与回声")
        code, added = post(base, "/add", {
            "request_id": "eval:selftest:add-1",
            "user_id": "alice",
            "session_id": "sess-A",
            "messages": [
                {"role": "user", "content": "My sister gave me a stand mixer for my birthday.",
                 "timestamp": 1682000000000},
                {"role": "assistant", "content": "That is a lovely gift from your sister.",
                 "timestamp": 1682000060000},
            ],
        }, key="test-key")
        chk("POST /add 返回 200", code == 200, f"status={code}")
        chk("Add 回声 request_id/user_id/session_id",
            added.get("request_id") == "eval:selftest:add-1"
            and added.get("user_id") == "alice" and added.get("session_id") == "sess-A")
        chk("Add 有 success 字段", added.get("success") is True)

        print("\n③ ★ 隔离边界（最要紧的一组）")
        # ⚠️ 两处刻意设计，否则这组断言是**假的**：
        #   ① 两个用户放**同一句话**（My sister gave me a stand mixer…）——
        #      若 memory_id 不含 user_id，bob 的 Add 会 UPSERT 掉 alice 那一行，
        #      于是 alice 的查询会返回 session=sess-B → 泄漏断言立刻抓到。
        #   ② bob 的独有内容必须含**首字母大写的专名**（`Thunderbird`）——
        #      全大写（如 NIGHTHAWK）匹配不上 `[A-Z][a-z]{2,}`，
        #      实体路就抽不到它，这组断言会变成"永远通过"。
        post(base, "/add", {
            "request_id": "eval:selftest:add-2", "user_id": "bob", "session_id": "sess-B",
            "messages": [
                {"role": "user", "content": "My sister gave me a stand mixer for my birthday.",
                 "timestamp": 1682000000000},
                {"role": "user", "content": "Project Thunderbird is my private diary topic.",
                 "timestamp": 1682000100000},
            ],
        }, key="test-key")
        code, res = post(base, "/search",
                         {"query": "What did my sister give me? Project Thunderbird diary",
                          "user_id": "alice", "top_k": 20}, key="test-key")
        chk("POST /search 返回 200", code == 200, f"status={code}")
        data = res.get("data") or []
        chk("search 返回 {data:[...]}", isinstance(data, list) and len(data) > 0,
            f"n={len(data)}")
        leaked = [d for d in data
                  if "Thunderbird" in (d.get("content") or "")
                  or d.get("session_id") == "sess-B"]
        chk("★ alice 的查询拿不到 bob 的任何记忆", not leaked,
            f"泄漏 {len(leaked)} 条")
        # 反向再验一次（同一个内容在两人那里都有，靠 session 区分）
        code, res_b = post(base, "/search",
                           {"query": "What did my sister give me?", "user_id": "bob", "top_k": 20},
                           key="test-key")
        sess_b = {d.get("session_id") for d in (res_b.get("data") or [])}
        chk("★ bob 的结果只含 sess-B", sess_b <= {"sess-B"}, f"sessions={sess_b}")
        got_b = post(base, "/search", {"query": "Project Thunderbird", "user_id": "bob",
                                       "top_k": 20}, key="test-key")[1].get("data") or []
        chk("bob 能搜到自己的 Thunderbird", any("Thunderbird" in (d.get("content") or "")
                                                for d in got_b), f"n={len(got_b)}")
        # 直接查库：**同一句话必须在库里是两行**（各自归属），而不是一行被改写
        import sqlite3
        conn = sqlite3.connect(os.path.join(data_dir, "data", "index", "header.db"))
        rows = conn.execute(
            "SELECT user_id, COUNT(*) FROM memories WHERE summary LIKE '%stand mixer%' "
            "GROUP BY user_id").fetchall()
        conn.close()
        chk("★ 同一句话按 user_id 存成两行（memory_id 拌了 user_id）",
            len(rows) == 2, f"{rows}")

        print("\n④ 时间戳必须随记忆交付")
        hit = next((d for d in data if "stand mixer" in (d.get("content") or "")), None)
        chk("命中的记忆带 [YYYY-MM-DD] 前缀",
            bool(hit) and (hit.get("content") or "").startswith("[2023-04-2"),
            f"content={(hit or {}).get('content', '')[:60]!r}")
        chk("JSON 里给完整 created_at",
            bool(hit) and (hit.get("created_at") or "").startswith("2023-04-2"),
            f"created_at={(hit or {}).get('created_at')}")
        chk("role 原样保留", bool(hit) and hit.get("role") in ("user", "assistant"),
            f"role={(hit or {}).get('role')}")
        # ★ 字段名冗余（2026-10-03 读官方契约源码后加）：CL-Bench 的内部清单读 `text`，
        #   其余读 `content`。只给一个名字 ⇒ 某个数据集的编排器会把这条记忆**静默丢掉**，
        #   而分数上只表现为"答错"，极难定位。
        chk("content 带日期前缀 / text 不带（防 CL-Bench 双时间戳）",
            bool(hit) and (hit.get("content") or "").startswith("[")
            and not (hit.get("text") or "").startswith("["),
            f"content={(hit or {}).get('content', '')[:30]!r} text={(hit or {}).get('text', '')[:30]!r}")
        chk("user_id 回显（隔离审计用）",
            bool(hit) and hit.get("user_id") == "alice")

        print("\n⑤ top_k 上限与参数校验")
        code, big = post(base, "/search", {"query": "sister", "user_id": "alice",
                                           "top_k": 9999}, key="test-key")
        chk("top_k 被夹到 100 以内", code == 200 and len(big.get("data") or []) <= 100,
            f"n={len(big.get('data') or [])}")
        code, err = post(base, "/search", {"query": "x"}, key="test-key")
        chk("缺 user_id → 400（隔离边界是必填）", code == 400, f"status={code}")
        code, err = post(base, "/add", {"user_id": "u", "messages": "not-a-list"},
                         key="test-key")
        chk("messages 非数组 → 400", code == 400, f"status={code}")

        print("\n⑤b ★ 原文索引必须真有内容（路4 不许静默失效）")
        # 路4（词法）是留一法里**最值钱的一路**（去掉 −12.6）。
        # 它的数据来自 `memories_fts_raw`，而我们是靠 `content_path=""` 让原文索引
        # **回退到 summary** 的 —— 如果这条回退链断了，路4 会**整路变空且不报错**，
        # 分数上只表现为"召回变差"。所以必须直接查 `src` 分布。
        # （原版 build_turns_store.py 的注释里写着同一件事："必须核对 src 占比"。）
        import sqlite3 as _sq
        conn = _sq.connect(f"file:{os.path.join(data_dir, 'data', 'index', 'header.db')}"
                           "?mode=ro", uri=True)
        try:
            rows = conn.execute("SELECT src, COUNT(*) FROM memories_raw_meta "
                                "GROUP BY src").fetchall()
        except Exception as error:                       # noqa: BLE001
            rows = [("查不了", f"{type(error).__name__}: {error}")]
        conn.close()
        src = {str(r[0]): r[1] for r in rows}
        n_summary = src.get("summary", 0)
        n_none = src.get("none", 0)
        chk("★ 原文索引 src='summary'（回退链生效，路4 有数据）",
            n_summary > 0, f"src 分布={src}")
        chk("★ 没有 src='none' 的行（没有取不到文本的记忆）",
            n_none == 0, f"none={n_none}")

        print("\n⑤c 扩窗（参赛版 window=2，序列题要靠它）")
        # 5 个数据集里 3 个评测"顺序"（ScriptMem/CL-Bench 的 ordering、BEAM 的 event_ordering）
        chk("配置口径 expand_window=2", health.get("config", {}).get("expand_window") == 2,
            f"expand_window={health.get('config', {}).get('expand_window')}")
        # 先在 alice 的会话里补几轮**与查询无关**的邻居 ——
        # 否则"条数>1"分不清是"扩窗带出了邻居"还是"本来就有两条命中"。
        post(base, "/add", {
            "request_id": "eval:selftest:add-3", "user_id": "alice", "session_id": "sess-A",
            "messages": [
                {"role": "user", "content": "I also bought a new bookshelf last weekend.",
                 "timestamp": 1682001000000},
                {"role": "assistant", "content": "A bookshelf sounds practical.",
                 "timestamp": 1682001060000},
                {"role": "user", "content": "And I repotted the monstera into a bigger pot.",
                 "timestamp": 1682001120000},
            ]}, key="test-key")
        code, ex = post(base, "/search", {"query": "stand mixer", "user_id": "alice",
                                          "top_k": 10}, key="test-key")
        got_ex = ex.get("data") or []
        chk("命中轮仍是第 1 条（扩窗是加法，不挤掉命中）",
            bool(got_ex) and "stand mixer" in (got_ex[0].get("content") or ""),
            f"top1={(got_ex or [{}])[0].get('content', '')[:44]!r}")
        # ★ 这条才是"扩窗真的生效"的证据：出现了**不含查询词**的邻居。
        neighbours = [d for d in got_ex if "stand mixer" not in (d.get("content") or "")]
        chk("★ 带出了与查询无关的**会话邻居**（扩窗真生效）", len(neighbours) > 0,
            f"邻居 {len(neighbours)} 条 / 共 {len(got_ex)} 条")
        # 邻居必须是**同一会话**的（跨会话串味是扩窗的经典 bug）
        chk("★ 邻居同属该会话（没有跨会话串味）",
            all((d.get("session_id") or "") == "sess-A" for d in got_ex),
            f"sessions={sorted({d.get('session_id') for d in got_ex})}")

        print("\n⑥ 幂等（重放同一次 Add 不该产生重复记忆）")
        before = len((post(base, "/search", {"query": "stand mixer", "user_id": "alice",
                                             "top_k": 100}, key="test-key")[1]).get("data") or [])
        post(base, "/add", {
            "request_id": "eval:selftest:add-1", "user_id": "alice", "session_id": "sess-A",
            "messages": [
                {"role": "user", "content": "My sister gave me a stand mixer for my birthday.",
                 "timestamp": 1682000000000},
                {"role": "assistant", "content": "That is a lovely gift from your sister.",
                 "timestamp": 1682000060000},
            ]}, key="test-key")
        after = len((post(base, "/search", {"query": "stand mixer", "user_id": "alice",
                                            "top_k": 100}, key="test-key")[1]).get("data") or [])
        chk("重放后结果数不增（memory_id 由内容决定）", after <= before,
            f"{before} → {after}")

        print("\n⑦ 四路确实开着（配置口径）")
        chk("routes_enabled 生效", health.get("routes_enabled") is True,
              f"routes_enabled={health.get('routes_enabled')}")
        chk("rerank_doc = raw（读原文）", health.get("rerank_doc") == "raw",
            f"rerank_doc={health.get('rerank_doc')}")

        print("\n⑧ ★ 退化可用性（模型服务挂掉时不许返回空）")
        # 这一组是 2026-10-03 实测逼出来的：当时 18099/18098 **都拒绝连接**，
        # 嵌入落 hash 兜底 ⇒ 余弦≈0 ⇒ `min_sim_threshold=0.15` 把每条都切掉
        # ⇒ **每个查询返回 {"data": []}**，而只探 `/health` 的版本还报"可达"。
        # 现在有两道防线：① phase2 的阈值兜底（不许清零）② /health 真探活。
        code, h2 = get(base, "/health")
        chk("/health 用**真调用**判断模型健康（不是端口探活）",
            "embed_ok" in h2 and "embed_detail" in h2,
            f"embed_ok={h2.get('embed_ok')} detail={str(h2.get('embed_detail'))[:48]}")
        code, res2 = post(base, "/search",
                          {"query": "What did my sister give me?", "user_id": "alice",
                           "top_k": 10}, key="test-key")
        n2 = len(res2.get("data") or [])
        chk("★ 模型服务不可用时检索仍返回非空（退化可用）", n2 > 0,
            f"n={n2} · embed_ok={h2.get('embed_ok')} rerank_ok={h2.get('rerank_ok')}")
    finally:
        httpd.shutdown()
        if args.keep:
            print(f"\n数据目录保留：{data_dir}")
        else:
            shutil.rmtree(data_dir, ignore_errors=True)

    print("\n" + "=" * 66)
    print(f"通过 {len(PASS)} · 失败 {len(FAIL)}")
    if FAIL:
        print("失败项：")
        for f in FAIL:
            print(f"  - {f}")
    print("=" * 66)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
