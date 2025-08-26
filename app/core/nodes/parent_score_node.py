
from typing import Dict, List
import logging
from app.core.state import PipelineState
from app.services.llm.scorer import run_parent_scoring

logger = logging.getLogger(__name__)

def parent_score_node(state: PipelineState) -> PipelineState:
    """상위 카테고리 점수 노드: LLM을 사용하여 상위 카테고리별 점수 계산"""
    try:
        sentences = state.get("sentences", [])
        parent_list = state.get("parent_list", [])
        
        if not sentences:
            logger.warning("No sentences available for parent scoring")
            state["parent_scores_prob"] = {}
            state["parent_scores_raw"] = []
            state["parent_reasoning"] = {}
            state["parent_evidence_idx"] = {}
            return state
            
        if not parent_list:
            logger.warning("No parent categories available for scoring")
            state["parent_scores_prob"] = {}
            state["parent_scores_raw"] = []
            state["parent_reasoning"] = {}
            state["parent_evidence_idx"] = {}
            return state
            
        logger.info(f"Starting parent scoring for {len(sentences)} sentences against {len(parent_list)} parent categories")
        
        # LLM 점수 계산
        probs, raw = run_parent_scoring(sentences, parent_list)
        
        if not probs or not raw:
            logger.warning("Parent scoring returned no results")
            state["parent_scores_prob"] = {}
            state["parent_scores_raw"] = []
            state["parent_reasoning"] = {}
            state["parent_evidence_idx"] = {}
            return state
            
        # 결과 처리
        state["parent_scores_prob"] = probs
        state["parent_scores_raw"] = raw
        
        # reasoning과 evidence 추출
        reasoning = {}
        evidence_idx = {}
        for item in raw:
            if isinstance(item, dict):
                name = item.get("name", "")
                if name:
                    reasoning[name] = item.get("reasoning", "")
                    evidence_idx[name] = item.get("evidence_idx", [])
                
        state["parent_reasoning"] = reasoning
        state["parent_evidence_idx"] = evidence_idx
        state["parent_scores_raw"] = raw  
        
        logger.info(f"Parent scoring completed: {len(probs)} probabilities, {len(raw)} raw scores")
        logger.info(f"Extracted reasoning for {len(reasoning)} categories")
        return state
        
    except Exception as e:
        logger.error(f"Error in parent scoring: {e}")
        # 에러 시 빈 결과 반환
        state["parent_scores_prob"] = {}
        state["parent_scores_raw"] = []
        state["parent_reasoning"] = {}
        state["parent_evidence_idx"] = {}
        return state
