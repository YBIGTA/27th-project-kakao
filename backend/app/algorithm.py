"""
GPU의 'per_sentence' 원천 스코어를 받아
- 의도/감정/최근성 가중,
- 카테고리별 합산 후 정렬,
- 최소 1 ~ 최대 3개 sub_category 선정,
- 카테고리별 근거문장 3개 추출.
"""

import os, datetime
from typing import Dict, Any, List, Tuple, DefaultDict
from collections import defaultdict
from math import log, exp

# ── 정책 파라미터(.env로 조정 가능) ───────────────────────────────────────────────
INTENT_MAP = {
    "필요": "need", "구매": "purchase", "관심": "interest", "고민": "consider",
    "부정": "negative", "단순 언급": "informative",
    "need": "need", "purchase": "purchase", "interest": "interest", "consider": "consider",
    "negative": "negative", "informative": "informative",
}

INTENT_WEIGHT = {
    "need":       float(os.getenv("W_INTENT_NEED", "1.30")),
    "purchase":   float(os.getenv("W_INTENT_PURCHASE", "1.25")),
    "interest":   float(os.getenv("W_INTENT_INTEREST", "1.10")),
    "consider":   float(os.getenv("W_INTENT_CONSIDER", "1.15")),
    "negative":   float(os.getenv("W_INTENT_NEGATIVE", "0.00")),  # 제외
    "informative":float(os.getenv("W_INTENT_INFO", "0.80")),
    "_default":   1.0,
}

SENT_POS = float(os.getenv("W_SENT_POS", "0.30"))
SENT_NEG = float(os.getenv("W_SENT_NEG", "0.50"))
HALF_LIFE_DAYS = float(os.getenv("RECENCY_HALF_LIFE_DAYS", "30"))

MIN_CATEGORIES = int(os.getenv("MIN_CATEGORIES", "1"))
MAX_CATEGORIES = int(os.getenv("MAX_CATEGORIES", "3"))
MIN_CAT_SCORE  = float(os.getenv("MIN_CAT_SCORE", "0.10"))

def _parse_ts(ts: str):
    try: return datetime.datetime.fromisoformat(ts.replace("Z","+00:00"))
    except: return None

def _recency_decay(ts: str) -> float:
    if not ts or HALF_LIFE_DAYS <= 0: return 1.0
    dt = _parse_ts(ts)
    if not dt: return 1.0
    now = datetime.datetime.now(datetime.timezone.utc)
    days = max((now - dt).total_seconds()/86400.0, 0.0)
    return exp(-log(2) * days / HALF_LIFE_DAYS)

def _intent_mul(lbl: str | None) -> float:
    key = INTENT_MAP.get((lbl or "").lower(), "_missing")
    return INTENT_WEIGHT.get(key, INTENT_WEIGHT["_default"])

def _sent_mul(s: float | None) -> float:
    if s is None: return 1.0
    s = max(min(float(s), 1.0), -1.0)
    return 1.0 + (s * SENT_POS if s >= 0 else s * (-SENT_NEG))

def decide_categories_and_evidence(gpu_out: Dict[str, Any]) -> Dict[str, Any]:
    per = gpu_out.get("per_sentence") or []
    if not per:
        return {"subcats": [], "evidence_by_cat": {}}

    cat_score: DefaultDict[str, float] = defaultdict(float)
    contrib: DefaultDict[str, List[Tuple[str, float]]] = defaultdict(list)

    for row in per:
        text = row.get("text", "")
        ts = row.get("ts")
        sent = row.get("sentiment")
        intent = row.get("intent")
        intent_key = INTENT_MAP.get((intent or "").lower(), None)

        mul = _intent_mul(intent) * _sent_mul(sent) * _recency_decay(ts)

        for cat, sim in (row.get("cat_scores") or {}).items():
            try:
                sim = float(sim)
            except Exception:
                continue

            # '부정'은 제외
            score = 0.0 if intent_key == "negative" else max(sim, 0.0) * mul
            if score > 0:
                cat_score[cat] += score
                if text:
                    contrib[cat].append((text, score))

    ranked = sorted(cat_score.items(), key=lambda x: x[1], reverse=True)
    filtered = [c for c, s in ranked if s >= MIN_CAT_SCORE]
    if len(filtered) < MIN_CATEGORIES:
        filtered = [c for c, _ in ranked[:MIN_CATEGORIES]]

    subcats = filtered[:MAX_CATEGORIES]

    # 카테고리별 근거문장 상위 3개
    evidence_by_cat: Dict[str, List[str]] = {}
    for cat in subcats:
        pairs = sorted(contrib.get(cat, []), key=lambda x: x[1], reverse=True)[:3]
        evidence_by_cat[cat] = [t for t, _ in pairs]

    return {"subcats": subcats, "evidence_by_cat": evidence_by_cat}

