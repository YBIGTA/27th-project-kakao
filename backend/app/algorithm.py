"""
GPU의 'sentences' 원천 스코어를 받아
- 가중치(weight)와 최근성 가중,
- 카테고리별 합산 후 정렬,
- 최대 3개 sub_category 선정,
- 카테고리별 근거문장 3개 추출.
"""

import os, datetime
from typing import Dict, Any, List, Tuple, DefaultDict
from collections import defaultdict
from math import log, exp

# ── 정책 파라미터(.env로 조정 가능) ───────────────────────────────────────────────
HALF_LIFE_DAYS = float(os.getenv("RECENCY_HALF_LIFE_DAYS", "30"))

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

def decide_categories_and_evidence(gpu_out: Dict[str, Any]) -> Dict[str, Any]:
    # sentences 필드 사용 (하위 호환성을 위해 per_sentence도 지원)
    sentences = gpu_out.get("sentences") or gpu_out.get("per_sentence") or []
    if not sentences:
        return {"subcats": [], "evidence_by_cat": {}}

    cat_score: DefaultDict[str, float] = defaultdict(float)
    contrib: DefaultDict[str, List[Tuple[str, float]]] = defaultdict(list)

    for row in sentences:
        text = row.get("text", "")
        weight = row.get("weight", 1.0)  # GPU에서 받은 종합 가중치
        
        # 가중치 계산 (GPU 가중치 + 최근성 가중치)
        try:
            weight = float(weight)
        except (ValueError, TypeError):
            weight = 1.0

        # 카테고리별 점수 계산
        for cat, sim in (row.get("cat_scores") or {}).items():
            try:
                sim = float(sim)
            except Exception:
                continue

            # 최종 점수 계산 (GPU 가중치 * 유사도)
            score = max(sim, 0.0) * weight
            if score > 0:
                cat_score[cat] += score
                if text:
                    contrib[cat].append((text, score))

    # 카테고리 순위 결정
    ranked = sorted(cat_score.items(), key=lambda x: x[1], reverse=True)
    filtered = [c for c, s in ranked if s >= MIN_CAT_SCORE]
    
    # 최대 카테고리 수 제한
    subcats = filtered[:MAX_CATEGORIES]

    # 카테고리별 근거문장 상위 3개 추출
    evidence_by_cat: Dict[str, List[str]] = {}
    for cat in subcats:
        pairs = sorted(contrib.get(cat, []), key=lambda x: x[1], reverse=True)[:3]
        evidence_by_cat[cat] = [t for t, _ in pairs]

    return {"subcats": subcats, "evidence_by_cat": evidence_by_cat}

