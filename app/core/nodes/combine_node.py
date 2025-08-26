
from typing import Dict, List
import logging
import json
from app.core.state import PipelineState
from app.core.config import WEIGHTS
from app.services.llm.client import LLMClient

logger = logging.getLogger(__name__)

def hierarchy_combine(state: PipelineState) -> PipelineState:
    """
    계층 결합 노드 (Hierarchy Combination Node)
    LLM을 사용한 실제 추론으로 최종 카테고리 선정
    
    입력:
    - parent_scores_prob: 상위 카테고리 확률
    - child_scores_prob: 하위 카테고리 확률
    - child_to_parent: 하위-상위 매핑
    - parent_to_children: 상위-하위 매핑
    - child_reasoning: 하위 카테고리 추론 이유
    - parent_reasoning: 상위 카테고리 추론 이유
    
    출력:
    - final_child_scores: 최종 점수
    - top3_children: Top-3 하위 카테고리
    - top3_children_reasoning: Top-3 추론 이유
    """
    try:
        parent_probs = state.get("parent_scores_prob", {})
        child_probs = state.get("child_scores_prob", {})
        child_to_parent = state.get("child_to_parent", {})
        parent_to_children = state.get("parent_to_children", {})
        # child_reasoning이 없으면 child_scores_info에서 추출
        child_reasoning = state.get("child_reasoning", {})
        if not child_reasoning:
            child_scores_info = state.get("child_scores_info", [])
            child_reasoning = {}
            for item in child_scores_info:
                if isinstance(item, dict):
                    name = item.get("name", "")
                    reasoning = item.get("reasoning", "")
                    if name and reasoning:
                        child_reasoning[name] = reasoning
        
        parent_reasoning = state.get("parent_reasoning", {})
        
        if not parent_probs or not child_probs:
            logger.warning("Missing probability scores for hierarchy combination")
            state["final_child_scores"] = {}
            state["top3_children"] = []
            state["top3_children_reasoning"] = {}
            return state

        logger.info(f"Combining {len(child_probs)} child categories with {len(parent_probs)} parent categories using LLM")
        
        # LLM을 사용한 계층 결합 추론
        try:
            llm_client = LLMClient()
            
            # LLM 입력 데이터 구성
            hierarchy_data = {
                "parent_categories": [
                    {
                        "name": name,
                        "probability": prob,
                        "reasoning": parent_reasoning.get(name, ""),
                        "children": list(parent_to_children.get(name, []))
                    }
                    for name, prob in parent_probs.items()
                ],
                "child_categories": [
                    {
                        "name": name,
                        "probability": prob,
                        "reasoning": child_reasoning.get(name, ""),
                        "parent": child_to_parent.get(name, ""),
                        "parent_probability": parent_probs.get(child_to_parent.get(name, ""), 0.0)
                    }
                    for name, prob in child_probs.items()
                ],
                "weights": {
                    "beta_child": WEIGHTS.beta_child,
                    "gamma_parent": WEIGHTS.gamma_parent,
                    "single_child_penalty_lambda": WEIGHTS.single_child_penalty_lambda
                }
            }
            
            # LLM 프롬프트 구성
            prompt = f"""
당신은 선물 카테고리 분석 전문가입니다. 상위 카테고리와 하위 카테고리의 점수를 종합하여 최종적으로 가장 적합한 하위 카테고리 Top-3를 선정해주세요.

계층 구조 데이터:
{chr(10).join([f"- {cat['name']}: {cat['probability']:.4f} (이유: {cat['reasoning']})" for cat in hierarchy_data['parent_categories']])}

하위 카테고리 데이터:
{chr(10).join([f"- {cat['name']} (상위: {cat['parent']}): {cat['probability']:.4f} (이유: {cat['reasoning']})" for cat in hierarchy_data['child_categories']])}

가중치 설정:
- 하위 카테고리 가중치: {WEIGHTS.beta_child}
- 상위 카테고리 가중치: {WEIGHTS.gamma_parent}
- 단일 자식 패널티: {WEIGHTS.single_child_penalty_lambda}

다음 JSON 형식으로 응답해주세요:
{{
    "top3_categories": [
        {{
            "name": "카테고리명",
            "final_score": 0.1234,
            "reasoning": "이 카테고리가 선정된 이유",
            "parent_influence": "상위 카테고리의 영향도",
            "child_strength": "하위 카테고리의 강점"
        }}
    ],
    "analysis_summary": "전체적인 분석 요약"
}}
"""
            
            # LLM 호출
            try:
                response = llm_client._chat_json(prompt)
            except Exception as e:
                logger.warning(f"LLM response parsing failed: {e}")
                raise ValueError("LLM response parsing failed")
            
            if "top3_categories" in response and len(response["top3_categories"]) >= 3:
                # LLM 결과 처리
                top3_data = response["top3_categories"][:3]
                
                final_scores: Dict[str, float] = {}
                top3_names = []
                top3_reasoning = {}
                
                for item in top3_data:
                    name = item.get("name", "")
                    score = float(item.get("final_score", 0.0))
                    reasoning = item.get("reasoning", "")
                    
                    if name and name in child_probs:
                        final_scores[name] = score
                        top3_names.append(name)
                        top3_reasoning[name] = reasoning
                
                logger.info(f"LLM hierarchy combination successful: {len(top3_names)} categories")
                
            else:
                logger.warning("LLM response format invalid, falling back to mathematical combination")
                raise ValueError("Invalid LLM response format")
                
        except Exception as e:
            logger.warning(f"LLM hierarchy combination failed: {e}, using mathematical fallback")
            
            # Fallback: 수학적 계산
            fallback_final_scores: Dict[str, float] = {}
            for child, cp in child_probs.items():
                parent = child_to_parent.get(child)
                if not parent:
                    logger.warning(f"Child category '{child}' has no parent mapping")
                    continue
                    
                pp = parent_probs.get(parent, 0.0)
                
                # Weighted blend: (BETA × child + GAMMA × parent) / (BETA + GAMMA)
                s = (WEIGHTS.beta_child * cp + WEIGHTS.gamma_parent * pp) / (WEIGHTS.beta_child + WEIGHTS.gamma_parent)

                # Single-child penalty based on number of siblings
                siblings = parent_to_children.get(parent, [])
                sib_count = len(siblings) if siblings else 1
                penalty = WEIGHTS.single_child_penalty_lambda / (sib_count ** 0.5)
                s = max(0.0, s - penalty)
                
                fallback_final_scores[child] = s
                logger.debug(f"Fallback calculation - Child '{child}' (parent: '{parent}'): child_score={cp:.3f}, parent_score={pp:.3f}, final={s:.3f}, penalty={penalty:.3f}")

            # Top-3 selection
            top3 = sorted(fallback_final_scores.items(), key=lambda x: x[1], reverse=True)[:3]
            top3_names = [name for name, score in top3]
            
            # Top-3에 대한 reasoning 수집
            top3_reasoning = {}
            for child_name in top3_names:
                reasoning = child_reasoning.get(child_name, "")
                if reasoning:
                    top3_reasoning[child_name] = reasoning
                else:
                    # reasoning이 없는 경우 기본 메시지
                    top3_reasoning[child_name] = f"상위 카테고리와 하위 카테고리 점수를 종합하여 선정됨 (최종 점수: {fallback_final_scores[child_name]:.3f})"  # type: ignore
        
        # 최종 결과를 state에 저장
        # final_scores가 정의되지 않은 경우 fallback 결과 사용
        if 'final_scores' not in locals():
            final_scores = fallback_final_scores
        state["final_child_scores"] = final_scores
        state["top3_children"] = top3_names
        state["top3_children_reasoning"] = top3_reasoning
        
        logger.info(f"Hierarchy combination completed: {len(top3_names)} top categories selected")
        return state
        
    except Exception as e:
        logger.error(f"Error in hierarchy combination: {e}")
        # 에러 시 기본 결과 반환
        state["final_child_scores"] = {}
        state["top3_children"] = []
        state["top3_children_reasoning"] = {}
        return state
