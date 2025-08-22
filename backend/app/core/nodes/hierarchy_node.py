"""HierarchyNode — INTEGRATION READY
- Combines parent and child scores using conditional probability
- Applies hierarchy constraints and penalties
- Returns final child scores for top-3 selection
"""
from typing import Dict, List
from core.state import GraphState
from core.config import BETA, GAMMA, SINGLE_CHILD_PENALTY

def hierarchy_node(state: GraphState) -> GraphState:
    """
    계층 결합 노드: 상위 카테고리와 하위 카테고리 점수를 결합
    
    Args:
        state: GraphState 객체
        
    Returns:
        GraphState: final_child_scores가 업데이트된 상태
    """
    if not state.parent_scores or not state.child_scores:
        state.final_child_scores = {}
        return state
    
    final_scores = {}
    
    for parent, parent_score in state.parent_scores.items():
        if parent not in state.child_labels_map:
            continue
            
        children = state.child_labels_map[parent]
        if not children:
            continue
            
        # 해당 상위 카테고리의 하위 카테고리들만 처리
        for child in children:
            if child not in state.child_scores:
                continue
                
            child_score = state.child_scores[child]
            
            # 계층 결합 공식: P(child|parent) * P(parent)
            # BETA와 GAMMA는 가중치 조정 파라미터
            combined_score = (BETA * child_score + GAMMA * parent_score) / (BETA + GAMMA)
            
            # 싱글 차일드 페널티 적용
            if len(children) == 1:
                combined_score *= (1.0 - SINGLE_CHILD_PENALTY)
            
            final_scores[child] = combined_score
    
    state.final_child_scores = final_scores
    
    # 디버그 정보 추가
    state.debug["hierarchy_combination"] = {
        "parent_scores": state.parent_scores,
        "child_scores": state.child_scores,
        "final_child_scores": final_scores,
        "beta": BETA,
        "gamma": GAMMA,
        "single_child_penalty": SINGLE_CHILD_PENALTY
    }
    
    return state
