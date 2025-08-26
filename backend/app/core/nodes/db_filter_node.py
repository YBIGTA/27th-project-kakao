
from typing import List, Dict, Any
import logging
from core.state import PipelineState
from services.repo.product_repo import ProductRepo

logger = logging.getLogger(__name__)

def db_filter_node(state: PipelineState) -> PipelineState:
    """데이터베이스 필터링 노드: 예산 범위 내 상품 필터링 및 조회"""
    try:
        profile = state.get("profile", {})
        top3 = state.get("top3_children", [])
        
        if not top3:
            logger.warning("No top-3 children categories available for filtering")
            state["candidate_products"] = []
            return state
            
        budget_min = int(profile.get("budget_min", 0))
        budget_max = int(profile.get("budget_max", 10**9))
        
        logger.info(f"Filtering products for categories: {top3}")
        logger.info(f"Budget range: {budget_min:,}원 ~ {budget_max:,}원")
        
        # ProductRepo 초기화 및 쿼리
        repo = ProductRepo()
        logger.info(f"Initializing ProductRepo with DSN: {repo.dsn if hasattr(repo, 'dsn') else 'Not set'}")
        try:
            candidates = repo.query_candidates(top3, budget_min, budget_max)
            logger.info(f"Successfully queried {len(candidates)} candidates from database")
        except Exception as e:
            logger.error(f"Failed to query candidates: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            candidates = []
        
        if not candidates:
            logger.error(f"No products found for categories {top3} within budget range")
            logger.error("This indicates a database connection or data issue")
            state["candidate_products"] = []
            return state
            
        logger.info(f"Found {len(candidates)} candidate products")
        
        # 하드 캡 적용 (성능 및 메모리 고려)
        limited_candidates = candidates[:300]
        if len(candidates) > 300:
            logger.info(f"Limited candidates from {len(candidates)} to 300 for performance")
            
        state["candidate_products"] = limited_candidates
        return state
        
    except Exception as e:
        logger.error(f"Error in database filtering: {e}")
        state["candidate_products"] = []
        return state
