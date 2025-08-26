
from typing import Dict, List, Tuple, Optional, Any
import logging
from .client import LLMClient
from app.utils.math import softmax_with_temp
from app.core.config import SOFTMAX

logger = logging.getLogger(__name__)

def aggregate_scores(items: List[Dict[str, Any]]) -> Dict[str, float]:
    # score := weighted multiplication of (relevance_raw, interest_raw)
    # relevance에 더 높은 가중치 부여 (0.6), interest에 낮은 가중치 (0.4)
    scores: Dict[str, float] = {}
    for item in items:
        try:
            r = max(SOFTMAX.clamp_min, min(SOFTMAX.clamp_max, float(item.get("relevance_raw", 0.0))))
            i = max(SOFTMAX.clamp_min, min(SOFTMAX.clamp_max, float(item.get("interest_raw", 0.0))))
            # 가중 곱하기: relevance^0.6 * interest^0.4
            s = (r ** 0.6) * (i ** 0.4)
            scores[item["name"]] = s
        except (ValueError, TypeError, KeyError) as e:
            logger.warning(f"Invalid score data for item {item}: {e}")
            scores[item["name"]] = 0
    return scores

def to_probs(scores: Dict[str, float], temperature: float, entropy_target: Optional[float] = None) -> Dict[str, float]:
    if not scores:
        return {}
    
    # 엔트로피 타깃이 설정된 경우 자동 온도 튜닝
    if entropy_target is not None:
        temperature = _tune_temperature_for_entropy(scores, entropy_target, temperature)
    
    probs = softmax_with_temp(scores, temperature=temperature, clamp_min=SOFTMAX.clamp_min, clamp_max=SOFTMAX.clamp_max)
    if isinstance(probs, dict):
        return probs
    else:
        # List[float]인 경우 Dict로 변환
        return {f"item_{i}": p for i, p in enumerate(probs)}

def _tune_temperature_for_entropy(scores: Dict[str, float], target_entropy: float, base_temperature: float) -> float:
    """엔트로피 타깃에 맞는 온도 자동 튜닝"""
    try:
        if len(scores) <= 1:
            return base_temperature
            
        # 현재 점수로 엔트로피 계산
        current_entropy = _calculate_entropy(scores)
        
        # 엔트로피 차이에 따라 온도 조정
        if abs(current_entropy - target_entropy) < 0.1:
            return base_temperature
            
        # 엔트로피가 낮으면 온도를 높여서 다양성 증가
        if current_entropy < target_entropy:
            temperature = base_temperature * 1.5
        else:
            # 엔트로피가 높으면 온도를 낮춰서 확실성 증가
            temperature = base_temperature * 0.7
            
        # 온도 범위 제한
        temperature = max(0.1, min(2.0, temperature))
        
        logger.info(f"Temperature tuned: {base_temperature:.3f} -> {temperature:.3f} (entropy: {current_entropy:.3f} -> target: {target_entropy:.3f})")
        return temperature
        
    except Exception as e:
        logger.warning(f"Temperature tuning failed: {e}")
        return base_temperature

def _calculate_entropy(scores: Dict[str, float]) -> float:
    """점수 분포의 엔트로피 계산"""
    try:
        if not scores:
            return 0.0
            
        # 점수를 확률로 정규화
        total = sum(scores.values())
        if total <= 0:
            return 0.0
            
        probs = [score / total for score in scores.values()]
        
        # 엔트로피 계산: -Σ(p * log(p))
        entropy = 0.0
        for p in probs:
            if p > 0:
                entropy -= p * (p ** 0.5)  # 제곱근을 사용하여 극단값 완화
                
        return entropy
        
    except Exception as e:
        logger.warning(f"Entropy calculation failed: {e}")
        return 0.0

def run_parent_scoring(sentences: List[str], parent_list: List[str]) -> Tuple[Dict[str, float], List[dict]]:
    try:
        llm = LLMClient()
        data = llm.score_parents(sentences, parent_list)
        categories = data.get("categories", [])
        if not categories:
            logger.warning("No categories returned from LLM for parent scoring")
            return {}, []
        
        scores = aggregate_scores(categories)
        probs = to_probs(scores, temperature=SOFTMAX.temperature, entropy_target=SOFTMAX.entropy_target_parent)
        logger.info(f"Parent scoring completed: {len(scores)} categories, {len(probs)} probabilities")
        return probs, categories
    except Exception as e:
        logger.error(f"Error in parent scoring: {e}")
        return {}, []

def run_child_scoring(sentences: List[str], child_list: List[str], parent_categories: List[str]) -> Tuple[Dict[str, float], List[Dict[str, Any]]]:
    try:
        logger.info(f"Starting child scoring: {len(sentences)} sentences, {len(child_list)} children, {len(parent_categories)} parents")
        
        llm = LLMClient()
        
        # 부모 카테고리 정보와 함께 하위 카테고리 점수 계산
        logger.info("Calling LLM score_children...")
        data = llm.score_children(sentences, child_list, parent_categories)
        
        logger.info(f"LLM score_children returned: {type(data)}")
        
        categories: List[Dict[str, Any]] = data.get("categories", [])
        if not categories:
            logger.warning("No categories returned from LLM for child scoring")
            return {}, []
        
        scores = aggregate_scores(categories)
        probs = to_probs(scores, temperature=SOFTMAX.temperature, entropy_target=SOFTMAX.entropy_target_child)
        logger.info(f"Child scoring completed: {len(scores)} categories, {len(probs)} probabilities")
        return probs, categories
    except Exception as e:
        logger.error(f"Error in child scoring: {e}")
        return {}, []
