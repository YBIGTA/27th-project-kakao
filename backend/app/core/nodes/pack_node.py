"""PackNode — INTEGRATION READY
- Final result packaging and formatting
- Creates the final JSON payload for the API response
"""
from typing import Dict, Any
from ..state import GraphState

def pack_node(state: GraphState) -> GraphState:
    """
    최종 결과 패키징 노드
    
    Args:
        state: GraphState 객체
        
    Returns:
        GraphState: final_payload가 업데이트된 상태
    """
    # JSON 직렬화 가능한 형태로 debug 정보 정리
    safe_debug = {}
    for key, value in state.debug.items():
        if key == "final_payload":
            continue  # 순환 참조 방지
        try:
            # 간단한 값들만 포함
            if isinstance(value, (str, int, float, bool, list, dict)):
                safe_debug[key] = value
        except:
            safe_debug[key] = str(value)
    
    # 최종 JSON 페이로드 생성
    final_payload = {
        "success": True,
        "message": "선물 추천이 완료되었습니다.",
        "data": {
            "user_context": {
                "age": state.ctx.age,
                "gender": state.ctx.gender,
                "relation": state.ctx.relation,
                "budget_min": state.ctx.budget_min,
                "budget_max": state.ctx.budget_max
            },
            "analysis": {
                "parent_categories": {
                    "scores": state.parent_scores,
                    "evidence": state.parent_evidence_idx,
                    "reasoning": state.parent_reasoning
                },
                "child_categories": {
                    "scores": state.final_child_scores,
                    "evidence": state.child_evidence_idx,
                    "reasoning": state.child_reasoning
                },
                "top3_selection": state.top3_children
            },
            "products": {
                "candidates_count": len(state.candidate_products),
                "selected_count": len(state.selected_products),
                "selected_products": state.selected_products,
                "rationales": state.rationales
            }
        },
        "debug": safe_debug
    }
    
    state.debug["final_payload"] = final_payload
    
    return state