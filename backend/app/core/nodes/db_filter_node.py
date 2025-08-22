from typing import Optional, Dict, Any, List
from core.state import GraphState
from services.repo.product_repo import new_repo

async def db_filter_node(state: GraphState, products_csv: Optional[str] = None) -> GraphState:
    """
    DB에서 상품을 필터링하는 노드
    
    Args:
        state: GraphState 객체
        products_csv: CSV 파일 경로 (선택사항)
        
    Returns:
        GraphState: 후보 상품이 추가된 상태
    """
    if not state.top3_children:
        state.candidate_products = []
        state.debug["candidate_count"] = 0
        return state
    
    try:
        # DB에서 상품 검색 (예산 범위로 필터링)
        repo = new_repo(products_csv)
        candidates = await repo.fetch(
            state.top3_children, 
            state.ctx.budget_min, 
            state.ctx.budget_max
        )
        
        state.candidate_products = candidates
        state.debug["candidate_count"] = len(candidates)
        state.debug["db_filter"] = {
            "categories": state.top3_children,
            "budget_range": f"{state.ctx.budget_min}~{state.ctx.budget_max}",
            "filtered_count": len(candidates)
        }
        
        return state
        
    except Exception as e:
        print(f"DB 필터링 노드 오류: {e}")
        state.candidate_products = []
        state.debug["candidate_count"] = 0
        state.debug["db_filter_error"] = str(e)
        return state
