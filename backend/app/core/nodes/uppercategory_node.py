"""
상위 카테고리 노드 (parent_score_node)

Input: state (GraphState with rows and parent_labels)
Process:
    - Calls LLM per sentence using services/llm/scorer.py
    - Expects rows already preprocessed
    - Returns state with parent_scores, reasoning, evidence
Output: GraphState with parent_scores, parent_reasoning, parent_evidence_idx
"""

from typing import Dict, List
from ..state import GraphState
from ..llm.scorer import score_parents_for_sentences
from ..utils.math import softmax_with_temp, auto_temperature
from ...config.settings import ENTROPY_TARGET_PARENT
import json

async def parent_score_node(state: GraphState) -> GraphState:
    print(f"🔍 parent_score_node 시작: {len(state.rows)}개 행")
    print(f"🔍 첫 번째 행: {state.rows[0] if state.rows else 'None'}")
    
    # 문장을 새로운 형식으로 변환
    sentences = []
    for i, r in enumerate(state.rows):
        try:
            sentence = {"idx": r.idx, "date": r.date, "text": r.text}
            sentences.append(sentence)
            if i == 0:
                print(f"🔍 첫 번째 문장: {sentence}")
        except Exception as e:
            print(f"❌ 문장 변환 실패 (행 {i}): {e}")
            print(f"   행 객체: {r}")
            print(f"   행 타입: {type(r)}")
            print(f"   행 속성: {dir(r)}")
            raise
    
    outs = await score_parents_for_sentences(sentences, state.parent_labels)

    raw_sum: Dict[str, float] = {p: 0.0 for p in state.parent_labels}
    evidence_idx: Dict[str, List[int]] = {p: [] for p in state.parent_labels}
    reasons: Dict[str, List[str]] = {p: [] for p in state.parent_labels}
    
    # 새로운 신호들 초기화
    polarity: Dict[str, List[str]] = {p: [] for p in state.parent_labels}
    exclusivity: Dict[str, List[str]] = {p: [] for p in state.parent_labels}
    form_signals: Dict[str, List[Dict[str, int]]] = {p: [] for p in state.parent_labels}

    for o in outs:
        # 새로운 응답 구조 파싱
        if "categories" in o:
            for cat_info in o["categories"]:
                cat_name = cat_info["name"]
                if cat_name not in state.parent_labels:
                    continue
                    
                # 기본 점수 계산 (0-100 스케일을 0-1로 정규화)
                relevance = float(cat_info.get("relevance_raw", 0)) / 100.0
                intent = float(cat_info.get("intent_raw", 0)) / 100.0
                raw_sum[cat_name] += (relevance + intent)  # 가중치 제거
                
                # 새로운 신호들 수집
                if "polarity" in cat_info:
                    polarity[cat_name].append(cat_info["polarity"])
                if "exclusivity_hint" in cat_info:
                    exclusivity[cat_name].append(cat_info["exclusivity_hint"])
                if "form_signals" in cat_info:
                    form_signals[cat_name].append(cat_info["form_signals"])
                
                # evidence 인덱스 수집 (프롬프트와 동일하게 2개로 제한)
                for idx in cat_info.get("evidence_idx", [])[:2]:
                    if idx not in evidence_idx[cat_name]:
                        evidence_idx[cat_name].append(idx)
                        if len(evidence_idx[cat_name]) >= 2:  # 3개 → 2개로 수정
                            break
        else:
            # 기존 응답 구조 호환성 유지
            sc = o.get("scores", {})
            for p in state.parent_labels:
                e = sc.get(p, {"relevance_raw":0,"intent_raw":0})
                # 0-100 스케일을 0-1로 정규화
                relevance = float(e.get("relevance_raw",0)) / 100.0
                intent = float(e.get("intent_raw",0)) / 100.0
                raw_sum[p] += (relevance + intent)  # 가중치 제거
            
            # evidence/reason (capped)
            for p in state.parent_labels:
                if len(evidence_idx[p]) < 2:  # 3개 → 2개로 수정
                    for idx in o.get("evidence_idx", []):
                        if idx not in evidence_idx[p]:
                            evidence_idx[p].append(idx)
                            if len(evidence_idx[p]) >= 2:  # 3개 → 2개로 수정
                                break
                if len(reasons[p]) < 3 and o.get("mini_reason"):
                    reasons[p].append(o["mini_reason"])

    # softmax 적용
    R_vals = [raw_sum[p] for p in state.parent_labels]
    T = auto_temperature(R_vals, ENTROPY_TARGET_PARENT)
    P = softmax_with_temp(R_vals, T=T)
    
    # 상태 업데이트
    state.parent_scores = {p: float(v) for p, v in zip(state.parent_labels, P)}
    state.parent_reasoning = reasons
    state.parent_evidence_idx = evidence_idx
    
    # 새로운 신호들 업데이트
    state.parent_polarity = {p: _most_common(polarity[p]) if polarity[p] else "neutral" for p in state.parent_labels}
    state.parent_exclusivity = {p: _most_common(exclusivity[p]) if exclusivity[p] else "mid" for p in state.parent_labels}
    state.parent_form_signals = {p: _aggregate_form_signals(form_signals[p]) if form_signals[p] else {} for p in state.parent_labels}

    state.debug["parent_raw_sum"] = raw_sum
    state.debug["parent_T"] = T
    state.debug["parent_scores"] = state.parent_scores
    return state

def _most_common(lst):
    """리스트에서 가장 빈번한 값 반환"""
    if not lst:
        return None
    return max(set(lst), key=lst.count)

def _aggregate_form_signals(signals_list):
    """폼 신호들을 집계"""
    if not signals_list:
        return {}
    
    aggregated = {}
    for signals in signals_list:
        for key, value in signals.items():
            if key not in aggregated:
                aggregated[key] = 0
            aggregated[key] += value
    
    return aggregated
