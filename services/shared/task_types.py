# -*- coding: utf-8 -*-
"""v1.3.2 · task_type 封闭取值（**唯一**一份实现）。

它管什么
--------
**记忆侧**字段 `task_type` —— "这条记忆是一次什么性质的对话"。
（问题侧的「维度」在 `services/shared/dimensions.py`，两者不可混：
 一个是"这条记忆是什么"，一个是"用户这句话想干什么"。）

为什么需要它（2026-09-29 实测）
------------------------------
表头 `task_type` **380/482 = 78.8% 都是"闲聊"**，而它是 Phase1 的过滤维度 ——
**拿它过滤等于不过滤**。根因两层，详见 `task_types.yaml` 文件头。

对外 API
--------
    values()                 -> list[str]   封闭取值（不含兜底）
    judge_block()            -> str         拼好的「判据」prompt 片段
    prompt_for(text)         -> str         让模型标一条的完整 prompt
    normalize(raw)           -> str         把模型输出收进封闭取值
    validate(dist)           -> dict        判据检查（最大单值 / 其他占比）
"""
from __future__ import annotations

import os
import re
import sys
from typing import Optional

try:
    from services.shared.config.loader import get_base_dir
except Exception:                                   # noqa: BLE001
    def get_base_dir() -> str:                      # type: ignore
        return os.getcwd()


def _eprint(*a, **kw):
    kw.setdefault("file", sys.stderr)
    print(*a, **kw)


YAML_NAME = "task_types.yaml"

# 找不到 YAML 时的兜底：**只用"其他"一个值**。
# 宁可退化成"标不出来"，也不要凭空编一套词表。
_FALLBACK = {
    "version": "v1.3.2-fallback",
    "fallback": "其他",
    "order": [],
    "types": {},
}

_DEF: Optional[dict] = None
_MTIME: Optional[float] = None
_WARNED = False


def _yaml_path() -> str:
    env = os.environ.get("SHUFANG_TASK_TYPES")
    if env:
        return env
    return os.path.join(get_base_dir(), YAML_NAME)


def _load(force: bool = False) -> dict:
    global _DEF, _MTIME, _WARNED
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
        except Exception as e:                       # noqa: BLE001
            _eprint(f"[task_types] 读取 {path} 失败，用兜底取值: {e}")
    elif not _WARNED:
        _WARNED = True
        _eprint(f"[task_types] 未找到 {path} —— 退化为只含「其他」的单一取值"
                f"（不会静默编造词表）。产品侧该文件应随插件分发。")

    if not isinstance(defn, dict) or not defn.get("types"):
        defn = _FALLBACK
    _DEF, _MTIME = defn, mtime
    _eprint(f"[task_types] v={defn.get('version')} 取值数={len(defn.get('types') or {})}"
            f" 来源={path if mtime is not None else '(内置兜底)'}")
    return _DEF


def reload() -> dict:
    return _load(force=True)


def values(include_fallback: bool = False) -> list:
    defn = _load()
    vs = list(defn.get("types") or {})
    if include_fallback:
        fb = defn.get("fallback")
        if fb and fb not in vs:
            vs.append(fb)
    return vs


def ordered() -> list:
    """按 YAML 的 order 排（越具体越先）；没列到的补在后面。"""
    defn = _load()
    order = [t for t in (defn.get("order") or []) if t in (defn.get("types") or {})]
    return order + [t for t in values() if t not in order]


def judge_block() -> str:
    """拼「判据」片段。**给判据不给例子** —— 给例子会让小模型抄例子。"""
    defn = _load()
    types = defn.get("types") or {}
    lines = []
    for t in ordered():
        spec = types.get(t) or {}
        lines.append(f"【{t}】目标：{spec.get('goal', '')}")
        body = (spec.get("judge") or "").strip()
        if body:
            lines.append(body)
        nb = (spec.get("not") or "").strip()
        if nb:
            lines.append(f"⚠️ {nb}")
        lines.append("")
    return "\n".join(lines).strip()


PROMPT_TMPL = """给下面这条对话记忆标一个类型。

可选类型（只能用这些，不许自造）：{values}

判定判据：
{judges}

记忆摘要：
{summary}

已提取的实体：{entities}

只输出类型名本身，不要解释、不要引号、不要标点。
类型："""


def prompt_for(summary: str, entities: Optional[list] = None) -> str:
    return PROMPT_TMPL.format(
        values=" / ".join(values()),
        judges=judge_block(),
        summary=(summary or "").strip()[:500],
        entities=", ".join(str(e) for e in (entities or [])[:8]) or "（无）",
    )


def normalize(raw: str) -> str:
    """把模型输出收进封闭取值。

    实测小模型会带引号 / 冒号 / 句号 / 解释。这里做**确定性**收敛：
    先精确匹配，再在文本里找**唯一**出现的取值；找不到就返回兜底。
    ⚠️ 不做模糊匹配 —— 模糊匹配会把"决策"和"验证"这种相邻词搞混。
    """
    defn = _load()
    fb = defn.get("fallback") or "其他"
    s = (raw or "").strip().strip('"\'`“”‘’。，,：: \n\t')
    vs = values()
    if s in vs:
        return s
    hit = [v for v in vs if v in s]
    if len(hit) == 1:
        return hit[0]
    return fb


def validate(dist: dict) -> dict:
    """判据检查（离线可验，不用跑 LoCoMo）。

    Args:
        dist: {类型名: 条数}

    Returns: {'n':.., 'max_share':(值,占比), 'fallback_share':占比,
              'ok':bool, 'why':[不通过原因...]}
    """
    defn = _load()
    fb = defn.get("fallback") or "其他"
    total = sum(dist.values()) or 0
    why = []
    if not total:
        return {"n": 0, "ok": False, "why": ["没有数据"]}
    top_v, top_n = max(dist.items(), key=lambda kv: kv[1])
    max_share = top_n / total
    fb_share = dist.get(fb, 0) / total
    if max_share > 0.40:
        why.append(f"最大单值 {top_v} 占 {max_share:.1%} > 40%")
    if fb_share > 0.10:
        why.append(f"兜底「{fb}」占 {fb_share:.1%} > 10%（判据有漏，回去补判据）")
    return {"n": total, "top": (top_v, top_n), "max_share": max_share,
            "fallback_share": fb_share, "ok": not why, "why": why}


def dump() -> dict:
    defn = _load()
    return {"version": defn.get("version"), "values": values(True),
            "fallback": defn.get("fallback"), "source": _yaml_path()}
