"""ChildScoreNode — INTEGRATION READY
- Calls LLM per sentence for child labels.
- Outputs global child probabilities (conditionalization later).
"""
from typing import Dict, List, Any
from core.state import GraphState
from services.llm.scorer import score_children_for_sentences
from utils.softmax import softmax_with_temp, auto_temperature
from core import config
import json

async def child_score_node(state: GraphState) -> GraphState:
    # 문장을 새로운 형식으로 변환
    sentences = [
        {"idx": r.idx, "date": r.date, "text": r.text}
        for r in state.rows
    ]
    
    all_children = [s for kids in state.child_labels_map.values() for s in kids]

    outs = await score_children_for_sentences(sentences, all_children)

    raw_sum: Dict[str, float] = {s: 0.0 for s in all_children}
    evidence_idx: Dict[str, List[int]] = {s: [] for s in all_children}
    reasons: Dict[str, List[str]] = {s: [] for s in all_children}
    
    # 새로운 신호들 초기화
    specificity: Dict[str, List[Dict[str, Any]]] = {s: [] for s in all_children}
    ownership_hint: Dict[str, List[str]] = {s: [] for s in all_children}
    explicit_rejection: Dict[str, List[bool]] = {s: [] for s in all_children}

    for o in outs:
        # 새로운 응답 구조 파싱
        if "subcategories" in o:
            for subcat_info in o["subcategories"]:
                subcat_path = subcat_info["path"]
                # 풀패스에서 하위 카테고리만 추출
                subcat_name = subcat_path.split("/")[-1] if "/" in subcat_path else subcat_path
                
                if subcat_name not in all_children:
                    continue
                    
                # 기본 점수 계산 (0-100 스케일을 0-1로 정규화)
                relevance = float(subcat_info.get("relevance_raw", 0)) / 100.0
                intent = float(subcat_info.get("intent_raw", 0)) / 100.0
                
                # LLM 점수만 사용 (임베딩 prior 제거)
                raw = relevance + intent
                raw_sum[subcat_name] += raw
                
                # 새로운 신호들 수집
                if "specificity" in subcat_info:
                    specificity[subcat_name].append(subcat_info["specificity"])
                if "ownership_hint" in subcat_info:
                    ownership_hint[subcat_name].append(subcat_info["ownership_hint"])
                if "explicit_rejection" in subcat_info:
                    explicit_rejection[subcat_name].append(subcat_info["explicit_rejection"])
                
                # evidence 인덱스 수집 (프롬프트와 동일하게 2개로 제한)
                for idx in subcat_info.get("evidence_idx", [])[:2]:
                    if idx not in evidence_idx[subcat_name]:
                        evidence_idx[subcat_name].append(idx)
                        if len(evidence_idx[subcat_name]) >= 2:  # 3개 → 2개로 수정
                            break
        else:
            # 기존 응답 구조 호환성 유지
            sc = o.get("scores", {})
            for s in all_children:
                e = sc.get(s, {"relevance_raw":0,"specificity_raw":0,"intent_raw":0})
                # 0-100 스케일을 0-1로 정규화
                relevance = float(e.get("relevance_raw",0)) / 100.0
                specificity_score = float(e.get("specificity_raw",0)) / 100.0
                intent = float(e.get("intent_raw",0)) / 100.0
                raw_llm = relevance + specificity_score + intent
                raw_sum[s] += raw_llm  # 임베딩 prior 제거
            
            # evidence/reason caps
            for s in all_children:
                if len(evidence_idx[s]) < 2:  # 3개 → 2개로 수정
                    for idx in o.get("evidence_idx", []):
                        if idx not in evidence_idx[s]:
                            evidence_idx[s].append(idx)
                            if len(evidence_idx[s]) >= 2:  # 3개 → 2개로 수정
                                break
                if len(reasons[s]) < 3 and o.get("mini_reason"):
                    reasons[s].append(o["mini_reason"])

    # softmax 적용
    R_vals = [raw_sum[s] for s in all_children]
    T = auto_temperature(R_vals, config.ENTROPY_TARGET_CHILD)
    P = softmax_with_temp(R_vals, T=T)
    
    # 상태 업데이트
    state.child_scores = {s: float(v) for s, v in zip(all_children, P)}
    state.child_reasoning = reasons
    state.child_evidence_idx = evidence_idx
    
    # 새로운 신호들 업데이트
    state.child_specificity = {s: _most_common(specificity[s]) if specificity[s] else {} for s in all_children}
    state.child_ownership_hint = {s: _most_common(ownership_hint[s]) if ownership_hint[s] else "none" for s in all_children}
    state.child_explicit_rejection = {s: _most_common(explicit_rejection[s]) if explicit_rejection[s] else False for s in all_children}

    state.debug["child_raw_sum"] = raw_sum
    state.debug["child_T"] = T
    state.debug["child_scores_global"] = state.child_scores
    return state

def _most_common(lst):
    """리스트에서 가장 빈번한 값 반환"""
    if not lst:
        return None
    return max(set(lst), key=lst.count)
