
from typing import Dict, List
import logging
from app.core.state import PipelineState
from app.services.llm.scorer import run_child_scoring

logger = logging.getLogger(__name__)

def run_child_score(state: PipelineState) -> PipelineState:
    """
    하위 카테고리별 점수 계산
    """
    try:
        sentences = state.get("sentences", [])
        child_list = state.get("child_list", [])
        # ✅ 부모 카테고리 비어있으면 parent_scores_prob의 키로 보충
        parent_categories = state.get("parent_categories") \
            or list((state.get("parent_scores_prob") or {}).keys())
        
        # 타입 체크
        if not isinstance(parent_categories, list):
            parent_categories = []
        
        if not sentences or not child_list:
            logger.warning("Missing sentences or child_list for child scoring")
            return state
        
        logger.info(f"Starting child scoring for {len(child_list)} child categories")
        logger.info(f"Using parent categories: {parent_categories}")
        
        # 부모 카테고리 정보와 함께 하위 카테고리 점수 계산
        logger.info(f"Calling run_child_scoring with {len(sentences)} sentences, {len(child_list)} child categories, {len(parent_categories)} parent categories")
        scores, scores_info = run_child_scoring(sentences, child_list, parent_categories)
        
        logger.info(f"run_child_scoring returned: scores={len(scores) if scores else 0}, scores_info={len(scores_info) if scores_info else 0}")
        
        if scores and len(scores) > 0:
            state["final_child_scores"] = scores
            state["child_scores_prob"] = scores 
            state["child_scores_info"] = scores_info
            logger.info(f"Child scoring completed: {len(scores)} categories scored")
        else:
            logger.warning("No child scores generated")
            state["final_child_scores"] = {}
            state["child_scores_prob"] = {}  # combine_node에서 필요
            state["child_scores_info"] = []
        
        return state
        
    except Exception as e:
        logger.error(f"Error in child score node: {e}")
        state["final_child_scores"] = {}
        state["child_scores_prob"] = {}
        state["child_scores_info"] = []
        return state
