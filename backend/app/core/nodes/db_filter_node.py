from typing import Optional, Dict, Any, List
from ..database.db import fetch_products, get_product_count

async def db_filter_node(state):
    """
    DB에서 상품을 필터링합니다.
    
    Args:
        state: GraphState 객체
        
    Returns:
        GraphState: candidate_products가 업데이트된 상태
    """
    if not state.top3_children:
        state.candidate_products = []
        state.debug["candidate_count"] = 0
        return state
    
    try:
        # 상품 수 미리 확인
        total_count = await get_product_count(
            state.top3_children, 
            state.ctx.budget_min, 
            state.ctx.budget_max
        )
        
        print(f"🔍 조건에 맞는 상품 수: {total_count}개")
        
        # DB에서 상품 검색
        candidates = await fetch_products(
            state.top3_children, 
            state.ctx.budget_min, 
            state.ctx.budget_max,
            limit=400  # 최대 400개까지 조회
        )
        
        state.candidate_products = candidates
        state.debug["candidate_count"] = len(candidates)
        state.debug["db_filter"] = {
            "categories": state.top3_children,
            "budget_range": f"{state.ctx.budget_min}~{state.ctx.budget_max}",
            "total_available": total_count,
            "filtered_count": len(candidates)
        }
        
        print(f"✅ DB 필터링 완료: {len(candidates)}개 상품 선택")
        return state
        
    except Exception as e:
        print(f"❌ DB 필터링 노드 오류: {e}")
        state.candidate_products = []
        state.debug["candidate_count"] = 0
        state.debug["db_filter_error"] = str(e)
        return state