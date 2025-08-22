from core.state import GraphState

def select_top3_node(state: GraphState) -> GraphState:
    """Top 3 하위 카테고리 선택 노드"""
    if not state.final_child_scores:
        state.top3_children = []
        return state
    
    # 점수 기준으로 정렬하여 상위 3개 선택
    sorted_children = sorted(
        state.final_child_scores.items(), 
        key=lambda x: x[1], 
        reverse=True
    )
    
    state.top3_children = [child for child, _ in sorted_children[:3]]
    
    # 디버그 정보 추가
    state.debug["top3_selection"] = {
        "all_scores": state.final_child_scores,
        "top3_children": state.top3_children,
        "top3_scores": {child: state.final_child_scores[child] for child in state.top3_children}
    }
    
    return state
