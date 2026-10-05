# -*- coding: utf-8 -*-
"""V1.3 增量 1 · 四路召回中的「路2 实体路」+ RRF 融合。

为什么需要这一路
----------------
B4 原来是**单路语义**：Phase1（SQL ∪ FTS5 预筛）→ Phase2 向量余弦精排。
语义路的固有毛病是 **top-K 会被"同一天/同一话题的近似内容"占满** ——
而开放域问题（cat3）的答案**散在多条记忆里**，挤掉一条就少一个答案项。
2026-09-26 的 cat3 诊断量化过这一点：证据**已经进了候选池**（90.2%），
但 **@15 只捞出 37.0%**、@50 才有 58.7% —— 不是没找到，是**排在后面**。

实体路提供的是**结构上不同**的候选（"和这几条共享核心实体"），
而不是"语义上更像"，所以它补的是覆盖面，不是排序分。

★ 为什么必须 `max-IDF`，不能按"命中实体数"
------------------------------------------
底本原方案是"命中实体数越多越靠前"。实测（`locomo-test/_entity_path_round2.txt`，
库 5,871 条 / 1,982 题 / RRF k=60 / 向量候选 20 / 实体候选 20 / 交付 15）：

    方案            全量召回   vs 基线   多跳    时间   开放域   单跳   对抗
    向量 top15（基线）  57.7%            64.9%  78.2%  42.4%  58.3%  40.6%
    A_count         59.1%    +1.4      68.4%  76.6%  43.5%  59.0%  43.9%
    B_idf           61.9%    +4.1      71.3%  78.8%  44.6%  61.6%  47.8%
    C_maxidf        62.8%    +5.0      71.3%  79.1%  45.7%  62.5%  49.6%
    F_idf_q_w05     58.5%    +0.8      66.0%  78.5%  43.5%  59.0%  41.7%   ← 给查询实体降权反而有害

**根因**：最高频实体 `John` 出现在 **25.6%** 的库里（1,504/5,871）——
命中它毫无信息量，但"命中数"排序会把它当强证据。取**最稀有命中的 IDF** 才反映信息量。

★ 为什么必须按**名次**融合，不能按分数合并
------------------------------------------
实体命中的文档**余弦分天然很低**（它和问题不像，只是和人名像）。
按分数合并会让实体路的候选**沉底** —— 实测 PRF（同样是"分数式扩展"）cat3 净增益 **+0.0**。
RRF 只看名次，天然免疫两路的分数尺度不同。

实测数字来源
------------
* `locomo-test/_entity_path_round2.txt` —— 上表（本轮实现要复现它）
* `locomo-test/_entity_path_test.log` —— 第一轮：实体路单独 43.3%、融合只有 +1.4、随机实体对照 1.2%
  （证明**实体信号不是噪声，是排序器太差**）

用法
----
    from services.B4_dual_gate_retrieval.routes import EntityRoute, rrf_fuse

    route = EntityRoute.from_store()          # 从 B3 的 header.db 建倒排 + IDF
    vec   = [r["memory_id"] for r in retrieve(...)["items"]]   # 路1 语义的名次表
    ent   = route.rank(vec[:5], top_n=20)                       # 路2 实体路的名次表
    fused = rrf_fuse([vec, ent])                                # 融合
"""
from __future__ import annotations

import json
import math
import re
from typing import Iterable, Optional

# ---- 与 `_entity_path_round2.txt` 那次实测**完全一致**的参数，别随手改 ----
# 改了就等于换了一个方案，旧数字不再可比 —— 要改就连同实测一起重跑。
RRF_K = 60            # RRF 的 k。路线书定 60（等权）
VEC_N = 20            # 路1 交给融合的名次表长度
ENT_N = 20            # 路2 交给融合的名次表长度
FINAL_N = 15          # 交付条数
SEED_FROM_VEC = 5     # 种子实体取自向量路 top-N 条（实测用的就是 5）
WEIGHT_ENTITY = 1.0   # 等权（实测 F_idf_q_w05 表明降权有害，别再试）

# ---- 路4 词法路（2026-09-30 步骤 2 新增）。上面那批常量没动，旧数字仍可比。----
LEX_N = 20            # 路4 交给融合的名次表长度（与 VEC_N / ENT_N 对齐）
# 诊断专用：把 BM25 能看到的候选一次取全，用来算 reach / oracle。
# 它是"上限"不是"预算" —— FTS5 的 OR 并集不可能超过库行数（5,871），设大只是不截断。
LEX_REACH_CAP = 20000

# ---- 路3 时间路（2026-09-30 步骤 3 新增）。上面那批常量同样没动。----
TIME_N = 20           # 路3 交给融合的名次表长度（与 VEC_N / ENT_N / LEX_N 对齐）


class EntityRoute:
    """基于 `memories.core_entities` 的倒排 + IDF 实体召回。

    只依赖 B3 的表头字段，**不新建任何索引**（架构图 §六：「四个维度的数据全都有了」）。
    """

    def __init__(self, entities_of: dict, inverted: dict, idf: dict):
        """
        Args:
            entities_of: `{memory_id: set[str]}` 每条记忆的核心实体。
            inverted:    `{entity: set[memory_id]}` 倒排表。
            idf:         `{entity: float}`，`log(N / df)`。
        """
        self.entities_of = entities_of
        self.inverted = inverted
        self.idf = idf
        self.size = len(entities_of)

    # ---------------------------------------------------------------- 构建
    @classmethod
    def from_rows(cls, rows: Iterable) -> "EntityRoute":
        """从 B3 的行构建。`rows` 元素需支持 `["memory_id"]` / `["core_entities"]`。"""
        entities_of = {}
        for r in rows:
            raw = r["core_entities"]
            try:
                entities_of[r["memory_id"]] = set(json.loads(raw)) if raw else set()
            except (TypeError, ValueError):
                entities_of[r["memory_id"]] = set()
        inverted: dict = {}
        for mid, ents in entities_of.items():
            for e in ents:
                inverted.setdefault(e, set()).add(mid)
        n = len(entities_of) or 1
        idf = {e: math.log(n / max(1, len(ids))) for e, ids in inverted.items()}
        return cls(entities_of, inverted, idf)

    @classmethod
    def from_store(cls, user_ids: Optional[list] = None) -> "EntityRoute":
        """从当前 SHUFANG_DATA_DIR 指向的 B3 库构建（评测里用 stage2 的库）。

        ★★ 参赛版（AML）：必须按 `user_ids` 建 —— 原版是**全表**建倒排，
        因为生产是单用户、评测靠 `session_ids` 限定 haystack；
        但 AML 的 `user_id` 是**唯一隔离边界**，全表建出来的倒排会让
        `rank()` 返回**别的用户**的记忆（跨用户泄漏 = 成绩作废）。

        顺带一个语义上的正确性：IDF 只有在**用户自己的记忆空间**上算才有意义
        （某实体在 A 用户那里很常见，在 B 用户那里可能是唯一一次提到）。
        """
        from services.B3_header_engine import get_conn
        conn = get_conn()
        where, params = "", []
        if user_ids:
            ph = ",".join("?" * len(user_ids))
            where = f" WHERE user_id IN ({ph})"
            params = list(user_ids)
        try:
            rows = conn.execute(
                "SELECT memory_id, core_entities FROM memories" + where,
                params).fetchall()
        finally:
            conn.close()
        return cls.from_rows(rows)

    # ---------------------------------------------------------------- 召回
    def seed_from(self, memory_ids: Iterable[str]) -> set:
        """种子实体 = 给定记忆（向量路 top-N）的核心实体并集。

        为什么用"向量路 top-N 的实体"而不是"查询里的实体"：
        实测把查询实体并进来（D/E/F 三档）**没有更好**，降权那档还明显更差。
        查询里出现的实体名往往过于笼统，而 top-N 命中的实体带着**已经在同一话题上**
        这个额外信号。
        """
        seed = set()
        for mid in memory_ids:
            seed |= self.entities_of.get(mid, set())
        return seed

    def reach(self, seed: set) -> set:
        """种子实体覆盖到的全部 memory_id（倒排并集）。"""
        ids = set()
        for e in seed:
            ids |= self.inverted.get(e, set())
        return ids

    def entities_in_text(self, text: str) -> set:
        """从查询文本里抽出**倒排词表中确实存在**的实体（最长匹配优先）。

        ★ 为什么必须补这个（2026-10-01 专项诊断的结论）
        ----------------------------------------------
        原先 `rank()` 只吃 `memory_ids` —— **查询文本从不进入这一路**。于是它在结构上
        只是「从路1 的邻域再走一跳」，**不是一条独立通道**。实测（`ab_entity_only.py`，
        400 题）它 76% 的"独有"命中其实已被路1 覆盖：
        **独占 vs 路4 = 9.0%，但独占 vs 路1∪路4 只有 2.2%** ——
        这就是留一法只有 −0.2 的机制性原因。

        ★ 为什么用"查词表"而不是跑实体抽取器
        -----------------------------------
        * 抽取器会引入**新的模型依赖**与新的不可复现性（本仓已经为"不确定的中间层"
          吃过亏：cross-encoder 同输入两次差 0.03）；
        * 倒排表**本身就是词表**，最长匹配足以覆盖"实体名原样出现在查询里"这一情形；
        * 它**没有可调阈值** —— 不会因为阈值调不准而时好时坏。

        ★ 语言说明（这是"测试端用不上"的另一半原因）
        -------------------------------------------
        本语料（LoCoMo）的实体词表 **74.6% 是中文**（3914 个里 2921 个），而查询是英文 ——
        所以本函数在**这个**语料上只能捞到那 25.4% 的英文名。
        它的价值在**中文查询**上：那时查询能直接命中中文实体表。
        这正是"测试端英文 / 生产端中文"的差别所在。

        ⚠️ 最长匹配：`('New York','York')` + 文本 `'New York'` 只应命中 `'New York'`。
        不加这条，短实体会在长实体内部再命中一次，等于给同一段文字投两票。
        """
        t = (text or '').lower()
        if not t:
            return set()
        by_len = getattr(self, '_by_len', None)
        if by_len is None:
            by_len = sorted(self.inverted, key=len, reverse=True)
            self._by_len = by_len
        hits: set = set()
        taken = [False] * len(t)
        for e in by_len:
            el = e.lower()
            if len(el) < 2:
                continue
            start = t.find(el)
            while start != -1:
                if not any(taken[start:start + len(el)]):
                    hits.add(e)
                    for k in range(start, start + len(el)):
                        taken[k] = True
                    break
                start = t.find(el, start + 1)
        return hits

    def rank(self, memory_ids: Iterable[str], top_n: int = ENT_N,
             query_text: Optional[str] = None, min_hits: int = 1) -> list:
        """排序并取 top_n。

        ★ 排序键 = **命中的最大 IDF**（`maxidf`）。理由见模块头：最高频实体
        可能占库 1/4，按命中数排序会被它支配。

        Args:
            memory_ids: 作为种子的记忆（通常传向量路 top-5）。
            top_n: 返回条数。
            query_text: **查询原文**。给了就把"查询里出现的实体"也并进种子
                （见 `entities_in_text` 的说明：不给的话这一路不看查询）。
                默认 `None` 是为了**向后兼容** —— 老调用方行为一字不变。
            min_hits: 至少要命中几个种子实体才算候选（**共现约束**）。
                ★ 为什么需要它（2026-10-01）：实测这一路的信号**按构造就是词法索引的子集** ——
                `memories_fts` 的索引内容正是 `" ".join(jieba.cut(summary + core_entities))`，
                而这里排序用的就是 `core_entities`。所以它若只做"命中任一实体"，
                与词法路重叠是必然的（英文语料独占 2.2%、中文 4.0%，并入查询实体也改不动）。
                `min_hits>=2` 要求一条记忆**同时**含多个种子实体 —— 这是
                **BM25 的词项求和表达不出来的结构信号**，也正是多跳题要的
                （"Jon 和 Gina 有什么共同点"需要一条同时提到两人的记忆）。

        Returns: memory_id 列表，按 maxidf 降序，同分按 id 升序（保证可复现）。
        """
        seed = self.seed_from(memory_ids)
        if query_text:
            seed = seed | self.entities_in_text(query_text)
        if not seed:
            return []
        scored = []
        for mid in self.reach(seed):
            hit = self.entities_of.get(mid, set()) & seed
            if len(hit) < max(1, int(min_hits)):
                continue
            scored.append((max(self.idf[e] for e in hit), mid))
        # 同分按 id 升序：不加这条，同分顺序依赖 set 迭代顺序 → 结果不可复现
        scored.sort(key=lambda t: (-t[0], t[1]))
        return [mid for _, mid in scored[:top_n]]


class LexicalRoute:
    """路4 词法路：FTS5 BM25 的**名次表**。

    为什么需要这一路（它不是"再加一个索引"）
    ----------------------------------------
    FTS5 从 2026-09-26 起就在用了，但**只当 Phase1 的并集预筛**：
    `fts_search(k=fts_k)` 的结果被并进候选池，随后由 Phase2 的向量余弦**重排**。
    也就是 BM25 的**名次信息被丢掉了** —— 它只换来"候选更多"（recall@15 8.3%→51.4%），
    没换来"排序更好"。这一路要做的就是把那个名次当成**独立一张表**交给 `rrf_fuse`。

    ★ 关键约束：分词必须与索引侧是**同一套**（这是这一步唯一的实现风险）
    ------------------------------------------------------------------
    `memories_fts` 是**独立表 + Python 侧 jieba 分词**（`sqlite_store.py` 里
    `_FTS_TOKENIZER` 的注释记着踩坑史：原先用 trigram，而中文实词大多是 **2 个字**，
    短于 trigram 的 3 字符窗口 → **中文查询几乎永远 0 命中**，且静默返回 `[]`）。
    索引侧写进去的是 `" ".join(jieba.cut(summary + core_entities))`，
    查询侧必须走同一个 `_fts_tokens`。

    所以这里**不自己实现分词**，一律转发给 `fts_search` / `_fts_tokens`：
    两套分词器只要差一点，就表现为"明明有这条却搜不到"，而且**不报错**。

    ⚠️ 这一路在 LoCoMo 上有一个**语料层**的障碍（不是实现问题，也不是分词问题）
    ------------------------------------------------------------------------
    问题是**英文**（`"When did Caroline go to the LGBTQ support group?"`），
    而 `summary` 是**中文**（`"用户与助手久别重逢，互相问候…"`）、
    `core_entities` 中英混杂（`["Joanna", "Nate", "团队射击游戏"]`）。
    BM25 只能匹配到**英文专名那一部分**，中文实词永远对不上英文问句。

    于是 LoCoMo 上量出的 BM25 路 recall **天然偏低**，而它**不能**直接推广成
    "词法路没用"——真实中文库里查询与摘要同语言，前提完全不同。
    读数必须连这个前提一起读，见 `locomo-test/_route2_lexical_verify.txt`。
    """

    @classmethod
    def from_store(cls) -> "LexicalRoute":
        """接口与 `EntityRoute.from_store()` 对齐，好让步骤 4 四路统一调用。

        词法路不需要预建任何结构（FTS 表是 B3 维护的），所以这里不查库 ——
        真正的库访问发生在 `rank()` / `reach()` 里，走 `fts_search` 的退化保护。
        """
        return cls()

    # ---------------------------------------------------------------- 分词
    def tokens(self, query_text: str) -> list:
        """查询侧分词（**就是索引侧那一套**，为了诊断和自检才暴露出来）。"""
        from services.B3_header_engine.sqlite_store import _fts_tokens
        return _fts_tokens(query_text)

    # ---------------------------------------------------------------- 召回
    def rank(self, query_text: str, top_n: int = LEX_N) -> list:
        """BM25 名次表：`top_n` 条 memory_id，按 BM25 降序。

        注意这里**不接 `memory_ids` 种子**（不像 `EntityRoute.rank`）——
        词法路只用问题本身，与语义路的 top-N 无关。这是它与路2 的本质区别：
        路2 靠"向量路已经找对了人"起步，路4 独立于语义路，所以能补**语义路整段漏掉**的候选。
        """
        from services.B3_header_engine import fts_search
        if not query_text or not str(query_text).strip():
            return []
        return fts_search(query_text, k=max(1, int(top_n)))

    def reach(self, query_text: str) -> list:
        """BM25 能看到的**全部**候选（OR 并集），仍按 BM25 降序。

        诊断用：`rank()` 只给前 N 条，看不出"证据到底有没有被词法路看见"。
        分开量 **reach（覆盖面）** 与 **@15（排序）** 是这一步的核心
        —— 计划 §六 ② 明确要求不只看总分。
        """
        from services.B3_header_engine import fts_search
        if not query_text or not str(query_text).strip():
            return []
        return fts_search(query_text, k=LEX_REACH_CAP)


# ==================================================================== 路3 时间路
_MONTHS = {
    'january': 1, 'jan': 1, 'february': 2, 'feb': 2, 'march': 3, 'mar': 3,
    'april': 4, 'apr': 4, 'may': 5, 'june': 6, 'jun': 6, 'july': 7, 'jul': 7,
    'august': 8, 'aug': 8, 'september': 9, 'sep': 9, 'sept': 9, 'october': 10,
    'oct': 10, 'november': 11, 'nov': 11, 'december': 12, 'dec': 12,
}
_MONTH_RE = '|'.join(sorted(_MONTHS, key=len, reverse=True))
# 时间词：**分档记录**，因为它们的可解析程度完全不同（见 `parse_time`）。
_RE_ISO = re.compile(r'\b(\d{4})-(\d{1,2})(?:-(\d{1,2}))?\b')
_RE_DMY = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)?\s+(' + _MONTH_RE + r')\.?,?\s+(\d{4})\b',
                     re.I)
_RE_MDY = re.compile(r'\b(' + _MONTH_RE + r')\.?\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})\b',
                     re.I)
_RE_MY = re.compile(r'\b(' + _MONTH_RE + r')\.?,?\s+(\d{4})\b', re.I)
_RE_YEAR = re.compile(r'\b(1[6-9]\d{2}|20\d{2})\b')
# 相对表达式：**解析得出来，但没有锚点就落不到具体日期上**。
_RE_REL = re.compile(
    r'\b(yesterday|today|tomorrow|tonight|last\s+(?:night|week|month|year)|'
    r'this\s+(?:week|month|year)|next\s+(?:week|month|year)|'
    r'\d+\s+(?:day|week|month|year)s?\s+ago)\b', re.I)
# 中文（真实库里查询是中文，这一支在生产上才是主力）
_RE_CN_YMD = re.compile(r'(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*[日号]')
_RE_CN_YM = re.compile(r'(\d{4})\s*年\s*(\d{1,2})\s*月')
_RE_CN_Y = re.compile(r'(\d{4})\s*年')
_RE_CN_MD = re.compile(r'(\d{1,2})\s*月\s*(\d{1,2})\s*[日号]')
_RE_CN_REL = re.compile(r'(昨天|今天|明天|前天|后天|上周|上个月|上月|去年|今年|明年|最近)')
# 裸月名（没有年份）：`in June` / `6月`。年份**只能靠锚点**定 —— 见 parse_time。
# ⚠️ 英文月份名有**假阳性**风险：`May` 既是五月也是人名。所以这一条必须放在
#    **所有带年份/带日的具体模式之后**，且在相对表达式之后，尽量减少误抓。
_RE_BARE_MONTH = re.compile(r'\b(' + _MONTH_RE + r')\b', re.I)
_RE_CN_BARE_MONTH = re.compile(r'(?<!\d)(\d{1,2})\s*月(?!\s*\d)')


class TimeTarget:
    """查询里的时间表达式解析结果。

    区间语义：`lo..hi` 都是 `YYYY-MM-DD`，**闭区间**。
    为什么是区间不是单日：查询里的"2022"、"May 2023"本来就不是一天，
    硬压成一天等于凭空造精度；而排序要的正是"落在这个窗口里就算命中"。
    """

    __slots__ = ('lo', 'hi', 'kind', 'raw', 'rel')

    def __init__(self, lo, hi, kind, raw, rel=None):
        self.lo, self.hi, self.kind, self.raw, self.rel = lo, hi, kind, raw, rel

    def __repr__(self):
        return f'TimeTarget({self.lo}..{self.hi}, {self.kind}, {self.raw!r})'


def _today_iso() -> str:
    """今天的 ISO 日期。相对时间表达式的**默认锚点**。"""
    import datetime as _dt
    return _dt.date.today().isoformat()


def _year_of(anchor: Optional[str]):
    """锚点里的年份（裸月名/裸月日要靠它定年）。取不到返回 `None`。"""
    import datetime as _dt
    if not anchor:
        return None
    try:
        return _dt.date.fromisoformat(str(anchor)[:10]).year
    except (TypeError, ValueError):
        return None


def parse_time(text: str, anchor: Optional[str] = None,
               allow_bare_month: bool = True) -> Optional[TimeTarget]:
    """从查询里抽时间表达式 → 日期区间。

    ★ 锚点语义（2026-09-30 改 —— 这一改是为了让**路3 在生产上真的能用**）
    ------------------------------------------------------------------
    `anchor` 决定相对表达式（"last week" / "去年" / "4 years ago"）落到哪一天：

    * `anchor=None`（默认）→ **锚在"今天"**。这才是"上周/去年/n 天前"的正确语义：
      用户问"我上个月做了什么"，指的就是**今天往前一个月**。
      ⚠️ 早先的实现是"**没锚点就不解析**"，理由是"不能在评测里偷用'对话什么时候结束'"。
      那条顾虑本身对，但**代价是这一路在生产上也瘫了**：中文查询里
      "上周 / 上个月 / 最近 / 去年"极常见，却全部交空表 ——
      实测这一路非空率只有 **12.9%**，而它**不是坏在实现，是坏在没给锚点**。
    * `anchor=''`（显式空串）→ **明确弃权**：相对表达式保持不可解析。
      给诊断/对照用（例如"假装我不知道今天几号"）。
    * `anchor='YYYY-MM-DD'` → 用给定锚点（评测里可传"对话那天"，见 `ab_time_route.py`）。

    绝对表达式（2023-05-08 / 7 May 2023 / May 2023 / 2022）**不受锚点影响**。

    匹配顺序 = 由具体到宽泛，避免"7 May 2023"被 `_RE_YEAR` 先吃掉变成"2023 年"。

    Returns: `TimeTarget` 或 `None`（查询里没有时间表达式）。
    """
    t = text or ''
    _anchor = _today_iso() if anchor is None else anchor
    m = _RE_CN_YMD.search(t)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        lo = hi = _iso(y, mo, d)
        if lo:
            return TimeTarget(lo, hi, 'day', m.group(0))
    m = _RE_ISO.search(t)
    if m:
        y, mo = int(m.group(1)), int(m.group(2))
        if m.group(3):
            lo = hi = _iso(y, mo, int(m.group(3)))
            if lo:
                return TimeTarget(lo, hi, 'day', m.group(0))
        lo, hi = _month_span(y, mo)
        if lo:
            return TimeTarget(lo, hi, 'month', m.group(0))
    m = _RE_DMY.search(t)
    if m:
        lo = hi = _iso(int(m.group(3)), _MONTHS[m.group(2).lower()], int(m.group(1)))
        if lo:
            return TimeTarget(lo, hi, 'day', m.group(0))
    m = _RE_MDY.search(t)
    if m:
        lo = hi = _iso(int(m.group(3)), _MONTHS[m.group(1).lower()], int(m.group(2)))
        if lo:
            return TimeTarget(lo, hi, 'day', m.group(0))
    m = _RE_CN_YM.search(t)
    if m:
        lo, hi = _month_span(int(m.group(1)), int(m.group(2)))
        if lo:
            return TimeTarget(lo, hi, 'month', m.group(0))
    m = _RE_CN_MD.search(t)
    if m:
        # 中文"5月8日"没写年份 → 用**锚点所在那年**（"5月8日"就是当年 5 月 8 日）。
        # 早先这里返回"不可解析的 relative"，等于白白丢掉一个可用的窗口。
        y = _year_of(_anchor)
        if y:
            lo = hi = _iso(y, int(m.group(1)), int(m.group(2)))
            if lo:
                return TimeTarget(lo, hi, 'day', m.group(0))
        return TimeTarget(None, None, 'relative', m.group(0),
                          ('md', int(m.group(1)), int(m.group(2)), 0))
    m = _RE_MY.search(t)
    if m:
        lo, hi = _month_span(int(m.group(2)), _MONTHS[m.group(1).lower()])
        if lo:
            return TimeTarget(lo, hi, 'month', m.group(0))
    m = _RE_REL.search(t)
    if m:
        tgt = TimeTarget(None, None, 'relative', m.group(0), _rel_of(m.group(0)))
        return _resolve_rel(tgt, _anchor)
    m = _RE_CN_REL.search(t)
    if m:
        tgt = TimeTarget(None, None, 'relative', m.group(0), _rel_of(m.group(0)))
        return _resolve_rel(tgt, _anchor)
    # 裸月名（没有年份）→ 用**锚点所在的那一年**。
    # 这是"in June / 6月"的正确语义：说话人指的是"（当年/今年）六月"。
    # ⚠️ 放在最后：`May`（人名）这类假阳性只会在**其它模式全不匹配**时才触发。
    # ⚠️ `allow_bare_month=False` 可关掉它 —— 它是**唯一带已知假阳性**的模式
    #    （英文月份名与人名重叠：`May`），所以必须能单独 A/B 它的净效果。
    if allow_bare_month:
        m = _RE_CN_BARE_MONTH.search(t)
        if m:
            y = _year_of(_anchor)
            if y:
                lo, hi = _month_span(y, int(m.group(1)))
                if lo:
                    return TimeTarget(lo, hi, 'month', m.group(0))
        m = _RE_BARE_MONTH.search(t)
        if m:
            y = _year_of(_anchor)
            if y:
                lo, hi = _month_span(y, _MONTHS[m.group(1).lower()])
                if lo:
                    return TimeTarget(lo, hi, 'month', m.group(0))
    m = _RE_CN_Y.search(t)
    if m:
        y = int(m.group(1))
        return TimeTarget(f'{y:04d}-01-01', f'{y:04d}-12-31', 'year', m.group(0))
    m = _RE_YEAR.search(t)
    if m:
        y = int(m.group(1))
        return TimeTarget(f'{y:04d}-01-01', f'{y:04d}-12-31', 'year', m.group(0))
    return None


def _iso(y: int, mo: int, d: int) -> Optional[str]:
    """宽松构造 ISO 日期：越界的月/日返回 None（而不是抛异常）。

    为什么要宽松：查询里会出现 `2023-13-45` 这种脏东西（OCR、手误），
    解析器**不能因为一条脏查询就让整次检索挂掉**。
    """
    if not (1 <= mo <= 12) or not (1 <= d <= 31):
        return None
    try:
        import datetime as _dt
        return _dt.date(y, mo, d).isoformat()
    except ValueError:
        return None


def _month_span(y: int, mo: int):
    if not (1 <= mo <= 12):
        return None, None
    import calendar
    import datetime as _dt
    last = calendar.monthrange(y, mo)[1]
    return _dt.date(y, mo, 1).isoformat(), _dt.date(y, mo, last).isoformat()


def _rel_of(raw: str):
    """把相对表达式压成 `(单位, 数量, 方向)`；方向 -1=过去 / 0=现在 / +1=未来。"""
    r = raw.lower().strip()
    cn = {'昨天': ('day', 1, -1), '今天': ('day', 0, 0), '明天': ('day', 1, 1),
          '前天': ('day', 2, -1), '后天': ('day', 2, 1), '上周': ('week', 1, -1),
          '上个月': ('month', 1, -1), '上月': ('month', 1, -1), '去年': ('year', 1, -1),
          '今年': ('year', 0, 0), '明年': ('year', 1, 1), '最近': ('week', 1, -1)}
    if r in cn:
        return cn[r]
    m = re.match(r'(\d+)\s+(day|week|month|year)s?\s+ago', r)
    if m:
        return (m.group(2), int(m.group(1)), -1)
    if r.startswith('yesterday') or r == 'last night':
        return ('day', 1, -1)
    if r == 'today' or r == 'tonight':
        return ('day', 0, 0)
    if r == 'tomorrow':
        return ('day', 1, 1)
    m = re.match(r'(last|this|next)\s+(week|month|year)', r)
    if m:
        return (m.group(2), 1, {'last': -1, 'this': 0, 'next': 1}[m.group(1)])
    return ('day', 0, 0)


def _resolve_rel(tgt: TimeTarget, anchor: Optional[str]) -> TimeTarget:
    """把相对表达式落到区间上（**必须有锚点**，否则保持不可解析）。"""
    if not anchor or not tgt.rel:
        return tgt
    import datetime as _dt
    try:
        a = _dt.date.fromisoformat(anchor)
    except (TypeError, ValueError):
        return tgt
    unit, n, direction = tgt.rel
    if unit == 'day':
        d = a + _dt.timedelta(days=n * direction)
        return TimeTarget(d.isoformat(), d.isoformat(), 'day', tgt.raw, tgt.rel)
    if unit == 'week':
        start = a - _dt.timedelta(days=a.weekday()) + _dt.timedelta(weeks=n * direction)
        return TimeTarget(start.isoformat(),
                          (start + _dt.timedelta(days=6)).isoformat(), 'week',
                          tgt.raw, tgt.rel)
    if unit == 'month':
        y, mo = a.year, a.month + n * direction
        y += (mo - 1) // 12
        mo = (mo - 1) % 12 + 1
        lo, hi = _month_span(y, mo)
        return TimeTarget(lo, hi, 'month', tgt.raw, tgt.rel)
    y = a.year + n * direction
    return TimeTarget(f'{y:04d}-01-01', f'{y:04d}-12-31', 'year', tgt.raw, tgt.rel)


class TimeRoute:
    """路3 时间路：按 `source_date` 与查询时间窗口的**邻近度**排序。

    它解决的是哪一类问题（别搞错，否则会得出"时间路没用"的错误结论）
    ------------------------------------------------------------------
    时间路能帮的是"**查询里带时间**"的问法："我上个月做了什么？"、
    "2023 年 5 月那次聊了什么？" —— 这时时间是一个**过滤/排序维度**。

    时间路**结构上帮不了**"**When did X happen?**"这类问题：
    查询里一个时间词都没有（答案才是时间），所以 `parse_time` 返回 `None`、
    这一路交出一张**空名次表**。这不是实现缺陷，是这类问题的信息不在查询里。

    ⚠️ 所以看验收数字时必须**分题型**读：cat2（时间类）大量是"When...?"，
    时间路对它天然无候选；而路线书构想的收益场景主要是前者。

    另外：目标窗口越宽，路越弱 —— "2022 年"命中的记忆可能上千条，
    窗口内只能按 memory_id 兜底排序（**确定但无信息量**）。
    验收脚本按 `kind`（day/month/year）分开量，正是为了看见这一点。
    """

    def __init__(self, dates_of: dict):
        """Args: dates_of: `{memory_id: 'YYYY-MM-DD' or None}`。

        内部**预先把日期转成 ordinal（整数天）**，不存字符串。
        理由：`rank()` 要对全库每条记忆算一次距离，而它会被每道题调用一次 ——
        5,871 条 × 1,974 题 ≈ **1,160 万次**。存字符串就得每次跑
        `date.fromisoformat`，那一步实测是整步的主要耗时；存整数只剩减法。
        """
        import datetime as _dt
        self.dates_of = {m: d for m, d in dates_of.items() if d}
        self._ord: dict = {}
        for mid, d in self.dates_of.items():
            try:
                self._ord[mid] = _dt.date.fromisoformat(str(d)[:10]).toordinal()
            except (TypeError, ValueError):
                continue          # 脏日期：跳过这一条，不能让整条路挂掉

    @classmethod
    def from_store(cls, user_ids: Optional[list] = None) -> "TimeRoute":
        """★ 参赛版：同 `EntityRoute.from_store` —— 按 `user_ids` 限定，
        否则时间路会把**别的用户**在相近日期的记忆排进来（隔离边界泄漏）。"""
        from services.B3_header_engine import get_conn
        conn = get_conn()
        where, params = "", []
        if user_ids:
            ph = ",".join("?" * len(user_ids))
            where = f" WHERE user_id IN ({ph})"
            params = list(user_ids)
        try:
            rows = conn.execute(
                'SELECT memory_id, source_date FROM memories' + where,
                params).fetchall()
        finally:
            conn.close()
        return cls({r['memory_id']: (r['source_date'] or '').strip() for r in rows})

    @property
    def size(self) -> int:
        return len(self.dates_of)

    def rank(self, query_text: str, top_n: int = TIME_N,
             anchor: Optional[str] = None,
             order_with: Optional[dict] = None) -> list:
        """按邻近度排序取 top_n。

        距离 = 记忆日期到目标区间的**天数**（在区间内为 0）。

        ★ 破平键（2026-10-01 改，V1.3.4 步骤 2）
        --------------------------------------
        原先同距离一律按 `memory_id` 升序 —— **那一点信息量都没有**，而窗口内
        所有记忆距离都是 0，所以"没信息量"的部分**占了整张名单的绝大部分**
        （实测窗口大小中位 **24 条/天**）。

        后果实测（200 题探针）：`路2·中文` 那格**召回 98.0% → 98.0% 完全相同**，
        而 **F1 差 −0.030** —— 召回一条没变，**只是交付顺序变了，分数就掉了**。

        现在：`order_with` 给一个 `{memory_id: 排序键}`（越小越靠前），
        同距离时按它排 —— 也就是**窗口内的顺序继承"相关性"**，
        时间路不再自己发明顺序，它只负责**划定范围**。
        （时间在记忆里是**架位**，不是**相关度分**。）

        Args:
            order_with: 同距离时的破平依据；`None` = 退回 `memory_id`（**旧行为**，
                供"破平键值多少分"的同运行内配对使用）。
        """
        t = parse_time(query_text, anchor=anchor)
        if t is None or not t.lo:
            return []
        import datetime as _dt
        lo = _dt.date.fromisoformat(t.lo).toordinal()
        hi = _dt.date.fromisoformat(t.hi).toordinal()
        BIG = float('inf')
        scored = []
        for mid, o in self._ord.items():
            dist = lo - o if o < lo else (o - hi if o > hi else 0)
            tie = order_with.get(mid, BIG) if order_with else 0
            scored.append((dist, tie, mid))
        # 距离优先 → 同距离按 `order_with` → 最后才用 memory_id 保证可复现
        scored.sort(key=lambda x: (x[0], x[1], x[2]))
        return [mid for _, _, mid in scored[:top_n]]

    def reach(self, query_text: str, anchor: Optional[str] = None) -> list:
        """全部有日期的记忆按邻近度排序（诊断 reach / 证据名次用）。"""
        return self.rank(query_text, top_n=len(self.dates_of) or 1, anchor=anchor)


def rrf_fuse(ranked_lists: list, k: int = RRF_K,
             weights: Optional[list] = None) -> list:
    """Reciprocal Rank Fusion：按**名次**融合多路。

        score(d) = Σ_i  w_i / (k + rank_i(d))

    Args:
        ranked_lists: 每路一个**有序** memory_id 列表。
        k: RRF 常数（60）。
        weights: 每路权重，默认等权。

    Returns: 融合后的 memory_id 列表，按 score 降序，同分按 id 升序。

    为什么是名次不是分数：实体路命中的文档余弦分天然很低（和问题不像，只是和人名像），
    按分数合并会让它沉底。RRF 只看名次，免疫两路分数尺度不同。
    """
    if weights is None:
        weights = [1.0] * len(ranked_lists)
    if len(weights) != len(ranked_lists):
        raise ValueError("weights 与 ranked_lists 长度不一致")
    score: dict = {}
    for w, lst in zip(weights, ranked_lists):
        for rank, mid in enumerate(lst, 1):
            score[mid] = score.get(mid, 0.0) + w / (k + rank)
    return [mid for mid, _ in sorted(score.items(), key=lambda t: (-t[1], t[0]))]


def quick_selfcheck() -> dict:
    """不需要数据库的自检：倒排、IDF、maxidf 排序、RRF 是否符合预期。

    给验证套件用。返回 `{检查项: bool}`，全 True 才算过。

    ★ 关键那条是 `maxidf_not_sum`：构造一个**两种排序器结论相反**的库，
    否则"测试通过"只证明它能跑，不证明它用的是 max-IDF。

        10 条文档：
          dA 含 C1..C5（5 个**常见**实体，各出现在 5/10 条里，idf=log2=0.693）
          dB 含 R      （1 个**稀有**实体，只出现在 1/10 条里，idf=log10=2.303）
          f1..f4 也含 C1..C5  （把 C 的 df 撑到 5）
          f5..f8 什么都不含    （把 N 撑到 10）

        种子 = {C1..C5, R} 时：
          sum 排序 → dA = 5×0.693 = 3.47  >  dB = 2.303   → **dA 在前**
          max 排序 → dA = 0.693          <  dB = 2.303   → **dB 在前**  ← 我们要的
    """
    def ent(*names):
        return json.dumps(list(names))

    commons = ["C1", "C2", "C3", "C4", "C5"]
    rows = [{"memory_id": "dA", "core_entities": ent(*commons)},
            {"memory_id": "dB", "core_entities": ent("R")}]
    for i in range(1, 5):
        rows.append({"memory_id": f"f{i}", "core_entities": ent(*commons)})
    for i in range(5, 9):
        rows.append({"memory_id": f"f{i}", "core_entities": json.dumps([])})

    route = EntityRoute.from_rows(rows)
    out = {}
    out["size_is_10"] = route.size == 10
    # df(R)=1、df(C1)=5 → 稀有实体 IDF 必须更大
    out["idf_rare_higher"] = route.idf["R"] > route.idf["C1"]
    # ★ 换排序器会反转结论的那一条
    ranked = route.rank(["dA", "dB"])
    out["maxidf_not_sum"] = bool(ranked) and ranked[0] == "dB"
    # 同分按 id 升序 → 可复现（不依赖 set 迭代顺序）
    out["deterministic_tiebreak"] = ranked[1:4] == ["dA", "f1", "f2"]
    # 无种子实体时返回空，而不是抛异常
    out["empty_seed_safe"] = route.rank(["f5"]) == []
    # ★ 查询侧实体（2026-10-01 补）：这一路原先**从不看查询**
    #   ① 最长匹配：词表里有 `New York` 与 `York` 时，`New York` 只应命中长的那条
    q = EntityRoute.from_rows([
        {"memory_id": "m1", "core_entities": ent("New York")},
        {"memory_id": "m2", "core_entities": ent("York")},
        {"memory_id": "m3", "core_entities": ent("Joanna")}])
    out["query_entity_longest_match"] = q.entities_in_text("I love New York") == {"New York"}
    #   ② 大小写不敏感（英文人名常首字母大写，查询里可能小写）
    out["query_entity_casefold"] = "Joanna" in q.entities_in_text("what did joanna say")
    #   ③ ★ 真的改变结果：查询里点名的实体，能把**种子里没有**的那条捞上来
    #      （m3 只含 Joanna；种子只有 m1 时必然捞不到 m3）
    out["query_entity_changes_result"] = (
        "m3" not in q.rank(["m1"])
        and "m3" in q.rank(["m1"], query_text="What did Joanna say?"))
    #   ④ 不给 `query_text` 时**行为与改动前逐字一致**（向后兼容）
    out["query_entity_backward_compatible"] = q.rank(["m1"]) == q.rank(["m1"], query_text=None)
    #   ⑤ 共现约束：`min_hits=2` 必须**真的排除**只命中一个实体的记忆
    #      （dA 含 C1..C5、dB 只含 R；种子 {C1..C5, R} 时 dA 命中 5 个、dB 命中 1 个）
    out["min_hits_excludes_single"] = (
        "dA" in route.rank(["dA", "dB"], min_hits=2)
        and "dB" not in route.rank(["dA", "dB"], min_hits=2))
    #   ⑥ `min_hits=1`（默认之外的显式传值）必须与不传时一致
    out["min_hits_1_is_default"] = (route.rank(["dA", "dB"], min_hits=1)
                                   == route.rank(["dA", "dB"]))

    # RRF：两路都排第一的，必须赢过只被一路排第一的
    out["rrf_agreement_wins"] = rrf_fuse([["a", "b"], ["a", "c"]])[0] == "a"
    # 名次融合免疫分数尺度：整体加偏移不改变顺序
    out["rrf_rank_only"] = rrf_fuse([["x", "y"], ["y", "x"]]) == ["x", "y"]
    # 权重必须和路数对齐，否则是静默用错权重
    try:
        rrf_fuse([["a"], ["b"]], weights=[1.0])
        out["weights_length_checked"] = False
    except ValueError:
        out["weights_length_checked"] = True
    return out


def lexical_selfcheck() -> dict:
    """路4 词法路的自检：**只测分词语义**，不碰数据库、不碰语料。

    为什么单独立一个函数，不并进 `quick_selfcheck()`
    ------------------------------------------------
    `quick_selfcheck()` 是纯函数的、连 `B3` 都不 import；
    这个函数要 import `sqlite_store._fts_tokens`（会读一次配置），
    重量级不同，混在一起会让"纯自检"这条退路失效。

    ★ 关键那条是 `two_char_cjk_survives`
    -----------------------------------
    索引侧不是 FTS5 内置分词器，而是 **Python 侧 jieba 切好再喂 unicode61**。
    历史上这里用过 trigram，而**中文实词绝大多数是 2 个字**（记忆 / 检索 / 插件…），
    短于 trigram 的 3 字符窗口 → 中文查询**几乎永远 0 命中**，
    而且 `fts_search` 按设计**静默返回 `[]`** —— 整条路"看起来在跑、实际从没生效"。

    没有这一条，本文件所有测试都过、而中文检索全废。
    """
    from services.B3_header_engine.sqlite_store import _fts_tokens, _FTS_MAX_TOKENS

    out = {}
    # ★ 2 字中文实词必须活下来（trigram 时代就是死在这里）
    out["two_char_cjk_survives"] = _fts_tokens("记忆") == ["记忆"]
    # 词与词之间要能分开，否则 BM25 拿到的是一个长 token，什么也匹配不上
    out["cjk_words_split"] = _fts_tokens("记忆 检索 插件 并行") == ["记忆", "检索", "插件", "并行"]
    # 单字虚词是噪声（"的""了""是"）
    out["single_char_dropped"] = _fts_tokens("的 了 是") == []
    # 疑问词/停用词不该主导匹配：英文问句只留下实词
    out["en_stopwords_dropped"] = _fts_tokens("What did Caroline research?") == ["Caroline", "research"]
    # 英文专名必须留下 —— LoCoMo 上词法路**只**能靠这一部分活着
    out["en_proper_noun_kept"] = "Caroline" in _fts_tokens(
        "When did Caroline go to the LGBTQ support group?")
    # 去重（同一 token 重复出现不该重复计数、白占 24 个名额）
    out["dedup"] = _fts_tokens("记忆记忆记忆") == ["记忆"]
    # 上限生效（约束 MATCH 的查询代价）
    out["token_cap"] = len(_fts_tokens(" ".join(f"w{i}" for i in range(60)))) == _FTS_MAX_TOKENS
    # 空查询返回空，而不是抛异常（调用方按"这一路没候选"处理）
    out["empty_safe"] = _fts_tokens("") == [] and _fts_tokens("   ") == []
    return out


def time_selfcheck() -> dict:
    """路3 时间路的自检：**只测解析器语义**，不碰数据库、不碰语料。

    这个函数的重点不是"能不能解析"，而是三条**容易做错、错了也不报错**的边界：

    ★ `rel_needs_anchor`
        "last week" 没有锚点就**必须**不可解析。给它编一个锚点（比如全库最新日期）
        会让覆盖率好看，但那是在评测里偷用"对话什么时候结束"这个信息。
    ★ `specific_beats_general`
        "7 May 2023" 必须解析成**一天**，不能被年份规则先吃掉变成"2023 年"
        —— 匹配顺序错了会让精度静默下降（区间从 1 天膨胀到 365 天）。
    ★ `dirty_date_safe`
        `2023-13-45` 这种脏值返回 None，不能抛异常
        —— 解析器不能因为一条脏查询就把整次检索弄挂。
    """
    out = {}
    # 英文绝对日期：日 / 月 / 年 三档精度各自对
    t = parse_time('When did Caroline go on 7 May 2023?')
    out['en_dmy_is_one_day'] = bool(t) and t.kind == 'day' and t.lo == t.hi == '2023-05-07'
    t = parse_time('What happened in May 2023?')
    out['en_month_span'] = bool(t) and t.kind == 'month' and t.lo == '2023-05-01' \
        and t.hi == '2023-05-31'
    t = parse_time('What did we discuss in 2022?')
    out['en_year_span'] = bool(t) and t.kind == 'year' and t.lo == '2022-01-01' \
        and t.hi == '2022-12-31'
    # ★ 具体优先于宽泛：不能被年份规则吞掉
    t = parse_time('the sunday before 25 May 2023')
    out['specific_beats_general'] = bool(t) and t.kind == 'day' and t.lo == '2023-05-25'
    # 中文三档
    out['cn_ymd'] = (parse_time('2023年5月8日聊了什么') or TimeTarget('', '', '', '')).lo \
        == '2023-05-08'
    out['cn_month'] = (parse_time('2023年5月的事') or TimeTarget('', '', '', '')).hi \
        == '2023-05-31'
    out['cn_year'] = (parse_time('2022年聊过什么') or TimeTarget('', '', '', '')).lo \
        == '2022-01-01'
    # ★ 相对表达式：**默认锚在"今天"**（2026-09-30 改，见 parse_time 的文档）
    t = parse_time('What did I do last week?')
    out['rel_defaults_to_today'] = bool(t) and t.kind == 'week' and t.lo is not None
    # 显式传空串 = 明确弃权，仍要能"不解析"（诊断/对照用）
    t2 = parse_time('What did I do last week?', anchor='')
    out['rel_abstain_on_empty_anchor'] = bool(t2) and t2.lo is None
    # 给了锚点就落到那个锚点，且区间是"那一整周"
    t = parse_time('What did I do last week?', anchor='2023-05-10')
    out['rel_with_anchor'] = bool(t) and t.kind == 'week' and t.lo == '2023-05-01' \
        and t.hi == '2023-05-07'
    out['cn_rel_with_anchor'] = (parse_time('去年做了什么', anchor='2023-05-10')
                                 or TimeTarget('', '', '', '')).lo == '2022-01-01'
    # ★ 裸月名（没年份）→ 用**锚点那年**（"in June / 6月"的正确语义）
    t = parse_time('When did Melanie go camping in June?', anchor='2023-05-10')
    out['en_bare_month_uses_anchor_year'] = (bool(t) and t.kind == 'month'
                                            and t.lo == '2023-06-01'
                                            and t.hi == '2023-06-30')
    t = parse_time('6月做了什么', anchor='2023-05-10')
    out['cn_bare_month_uses_anchor_year'] = (bool(t) and t.lo == '2023-06-01')
    # ★ 裸"月日"也一样（"5月8日"= 锚点那年的 5 月 8 日）
    t = parse_time('5月8日聊了什么', anchor='2023-05-10')
    out['cn_md_uses_anchor_year'] = (bool(t) and t.kind == 'day'
                                     and t.lo == '2023-05-08')
    # 裸月名不能盖过带年份的具体写法（顺序守卫）
    out['bare_month_loses_to_specific'] = (
        (parse_time('What happened in May 2023?', anchor='2026-10-01')
         or TimeTarget('', '', '', '')).lo == '2023-05-01')
    # 没有时间词的查询要返回 None（否则"这一路有候选"是假的）
    out['no_time_is_none'] = parse_time('What is Caroline\'s identity?') is None
    out['empty_is_none'] = parse_time('') is None and parse_time(None) is None
    # ★ 脏日期不能抛
    try:
        parse_time('meeting on 2023-13-45')
        out['dirty_date_safe'] = True
    except Exception:                          # noqa: BLE001
        out['dirty_date_safe'] = False

    # 路由行为：没有时间词 → 空名次表（而不是"按 id 返回一堆"）
    route = TimeRoute({'m1': '2023-05-07', 'm2': '2022-01-01', 'm3': None})
    out['route_empty_without_time'] = route.rank('What is Caroline\'s identity?') == []
    # 窗口内的排在窗口外的前面
    out['route_nearest_first'] = route.rank('What happened in May 2023?')[:1] == ['m1']
    # 没有日期的记忆不能进候选（不能拿它冒充"时间命中"）
    out['route_skips_dateless'] = 'm3' not in route.reach('What happened in May 2023?')

    # ★ V1.3.4 步骤 2：破平键必须**有信息量**
    #   ① 不传 `order_with` → 与改动前**逐字一致**（按 memory_id，向后兼容）
    _tr = TimeRoute({'a': '2023-05-01', 'b': '2023-05-01', 'c': '2023-05-01'})
    out['time_tiebreak_legacy_stable'] = (_tr.rank('What happened in May 2023?')
                                          == ['a', 'b', 'c'])
    #   ② 传了 `order_with` → **同距离内按它排**（这里刻意设成与 id 序相反）
    out['time_tiebreak_uses_order_with'] = (
        _tr.rank('What happened in May 2023?', order_with={'a': 2, 'b': 0, 'c': 1})
        == ['b', 'c', 'a'])
    #   ③ **距离仍然优先**：窗口外更近的也不能越过窗口内的（破平键不得越权）
    _tr2 = TimeRoute({'in1': '2023-05-01', 'in2': '2023-05-01', 'out': '2023-06-10'})
    _got2 = _tr2.rank('What happened in May 2023?',
                      order_with={'out': 0, 'in1': 1, 'in2': 2})
    out['time_distance_still_dominates'] = (_got2[:2] == ['in1', 'in2']
                                            and _got2[-1] == 'out')
    return out

