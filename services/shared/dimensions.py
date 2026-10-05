# -*- coding: utf-8 -*-
"""v1.3.1「维度版」· 维度判定与交付策略（**唯一**的一份实现）。

存在的理由
----------
2026-09-29 实测：`chat.py` 和 `locomo-test/full_9b_eval.py` **各自有一份**完全相同的
`classify_question`（两份 `_INFER_PAT` 逐字一样）。这是"产品与评测走两条路"的典型 ——
评测量到的不是产品跑的东西。v1.3.1 把它合并成这一份：**两边 import 同一个模块**。

设计约束（作者 2026-09-29）
--------------------------
* **维度即数据**：定义在 `<BASE_DIR>/dimensions.yaml`，改文件就能加维度，不用动代码。
  找不到该文件时用本模块内置的 `_FALLBACK` 兜底（只含 A1），并往 stderr 告警 ——
  **绝不静默退化成"所有题都是一个维度"**。
* **热加载**：按 mtime 重读，改完 YAML 不用重启进程。
* **判定失败不许拖垮检索**：任何异常都退回 fallback 维度，只记 stderr。

对外 API
--------
    classify(text)                  -> str   子类 id，如 'B1'（永不抛异常）
    parent_of(child)                -> str   大项 id，如 'B'
    policy(child)                   -> dict  该维度生效的 policy（子类覆盖大项）
    topk(child, default)            -> int
    delivery(child)                 -> str   raw | summary | grouped
    dedupe(child)                   -> bool
    describe(child)                 -> str   人话描述（审计/README 用）
    reload()                        -> dict  强制重读 YAML
"""
from __future__ import annotations

import os
import re
import sys
from typing import Optional

try:                                    # 包内导入
    from services.shared.config.loader import get_base_dir
except Exception:                       # noqa: BLE001 - 独立跑时的兜底
    def get_base_dir() -> str:          # type: ignore
        return os.getcwd()


def _eprint(*a, **kw):
    kw.setdefault("file", sys.stderr)
    print(*a, **kw)


YAML_NAME = "dimensions.yaml"

# 找不到 YAML 时的最小兜底：全部归 A1。
# **故意只有一个维度** —— 宁可退化成"没有维度分流"，也不要凭空编出一套维度。
_FALLBACK = {
    "version": "v1.3.1-fallback",
    "fallback": "A1",
    "order": ["A1"],
    "parents": {"A": {"name": "查找", "policy": {"topk": 15, "delivery": "raw"}}},
    "children": {"A1": {"parent": "A", "name": "已知条目检索"}},
}

_DEF: Optional[dict] = None
_PAT: Optional[dict] = None
_MTIME: Optional[float] = None
_WARNED = False


def _yaml_path() -> str:
    # 允许显式覆盖（评测/测试用）
    env = os.environ.get("SHUFANG_DIMENSIONS")
    if env:
        return env
    return os.path.join(get_base_dir(), YAML_NAME)


def _compile(defn: dict) -> dict:
    """把 YAML 里的 zh/en 正则编译成 [(child_id, [compiled, ...]), ...]，按 order 排。"""
    children = defn.get("children") or {}
    order = defn.get("order") or list(children)
    # order 里出现的排前面；没列到的补在后面（用户新加维度不会因为忘了改 order 而失效）
    order = [c for c in order if c in children] + \
            [c for c in children if c not in order]
    pat = {}
    for cid in order:
        spec = children.get(cid) or {}
        regs = []
        for key in ("zh", "en"):
            raw = spec.get(key)
            if not raw:
                continue
            try:
                regs.append(re.compile(raw, re.IGNORECASE))
            except re.error as e:
                _eprint(f"[dimensions] {cid}.{key} 正则无效，已忽略: {e}")
        pat[cid] = regs
    return pat


def _load(force: bool = False) -> dict:
    """读 YAML；按 mtime 热加载。任何失败都不抛异常。"""
    global _DEF, _PAT, _MTIME, _WARNED
    path = _yaml_path()
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        mtime = None

    if not force and _DEF is not None and mtime == _MTIME:
        return _DEF

    defn = None
    if mtime is not None:
        try:
            import yaml
            with open(path, "r", encoding="utf-8") as f:
                defn = yaml.safe_load(f)
        except Exception as e:                                  # noqa: BLE001
            _eprint(f"[dimensions] 读取 {path} 失败，用兜底维度: {e}")
            defn = None
    elif not _WARNED:
        _WARNED = True
        _eprint(f"[dimensions] 未找到 {path} —— 退化为单一维度 A1"
                f"（不会静默编造维度）。产品侧该文件应随插件分发。")

    if not isinstance(defn, dict) or not defn.get("children"):
        defn = _FALLBACK

    _DEF, _PAT, _MTIME = defn, _compile(defn), mtime
    _eprint(f"[dimensions] v={defn.get('version')} 维度数={len(defn.get('children') or {})}"
            f" 来源={path if mtime is not None else '(内置兜底)'}")
    return _DEF


def reload() -> dict:
    return _load(force=True)


def override() -> str:
    """强制覆盖维度（调试/度量用）。环境变量优先于 YAML 的 `override` 键。

    为什么需要它（2026-09-29 实测）：cat1 多跳与 cat4 单跳**在问句形态上不可分**
    （B1 枚举形态净收益只有 16 个百分点）。要单独量"提高交付条数"这个变量本身，
    就必须绕开分类器、把全部题都按同一维度跑 —— 否则分不清是"条数变了"还是
    "被分类器挑中的那部分题恰好不同"。
    """
    env = (os.environ.get("SHUFANG_DIM_OVERRIDE") or "").strip()
    if env:
        return env
    return str(_load().get("override") or "").strip()


def classify(text: str) -> str:
    """返回子类 id（如 'B1'）。**永不抛异常**，最差返回 fallback。"""
    defn = _load()
    ov = override()
    if ov:
        return ov
    q = str(text or "").strip()
    if q:
        for cid, regs in (_PAT or {}).items():
            for r in regs:
                try:
                    if r.search(q):
                        return cid
                except Exception:                                # noqa: BLE001
                    continue
    return defn.get("fallback") or "A1"


def parent_of(child: str) -> str:
    defn = _load()
    return ((defn.get("children") or {}).get(child) or {}).get("parent") or "A"


def policy(child: str) -> dict:
    """子类 policy 覆盖大项 policy；两边都没有时给一份最小默认。"""
    defn = _load()
    cspec = (defn.get("children") or {}).get(child) or {}
    pspec = (defn.get("parents") or {}).get(cspec.get("parent")) or {}
    out = {"topk": 15, "delivery": "raw", "dedupe": False}
    out.update(pspec.get("policy") or {})
    out.update(cspec.get("policy") or {})     # 子类可覆盖
    return out


def topk(child: str, default: int = 15) -> int:
    try:
        return int(policy(child).get("topk", default))
    except Exception:                                            # noqa: BLE001
        return default


def delivery(child: str) -> str:
    return str(policy(child).get("delivery") or "raw")


def dedupe(child: str) -> bool:
    return bool(policy(child).get("dedupe"))


def describe(child: str) -> str:
    defn = _load()
    cspec = (defn.get("children") or {}).get(child) or {}
    pspec = (defn.get("parents") or {}).get(cspec.get("parent")) or {}
    return (f"{child} {cspec.get('name', '?')}"
            f"（{pspec.get('en', '?')}·{pspec.get('name', '?')}）"
            f" topk={topk(child)} delivery={delivery(child)}")


def dump() -> dict:
    """给审计 / 评测落盘用：把当前生效的维度定义原样交出去。"""
    defn = _load()
    return {"version": defn.get("version"), "order": defn.get("order"),
            "fallback": defn.get("fallback"), "override": override(),
            "children": list((defn.get("children") or {}).keys()),
            "source": _yaml_path()}


# ---------------------------------------------------------------- 兼容层
# `chat.py` 与 `full_9b_eval.py` 原来各有一份 `classify_question()`（2 类）。
# 保留同名函数让**老调用点不改也能跑**。
#
# ⚠️ 旧标签按**子类**映射，不按大项 —— 这一点很要紧：
#   V1.2 的 `inference` 语义只对应 **B2（探索式）**。
#   若按大项映射，B 大项下的 **B1（穷尽汇总）也会变成 inference**，
#   于是 `--ctx-format routed` 的历史基线会被静默改变（B1 从"给原文"变成"给实体"），
#   与之前几轮 `routed` 结果**不再可比**。2026-09-29 实测时差点踩到。
_LEGACY_INFER = {"B2"}


def classify_question(text: str) -> str:
    """旧接口：`'fact'` / `'inference'`。**新代码请直接用 `classify()`。**"""
    return "inference" if classify(text) in _LEGACY_INFER else "fact"


def classify_detail(text: str) -> dict:
    """判定 + 策略一次给全，方便审计落盘。"""
    cid = classify(text)
    pid = parent_of(cid)
    p = policy(cid)
    return {"child": cid, "parent": pid, "topk": p.get("topk"),
            "delivery": p.get("delivery"), "dedupe": bool(p.get("dedupe")),
            "legacy": classify_question(text)}
