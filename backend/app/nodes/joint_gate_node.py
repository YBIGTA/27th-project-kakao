"""
결합+게이트 연산 노드 (joint_gate_node.py)

Input: probs_upper, probs_lower_by_parent, upper_reasoning, lower_reasoning
Process:
    - 로그 가중합 결합: joint(p,c) = α·log P(p|x) + β·log P(c|p,x)
    - joint score로 정렬 → Top-K 하위 카테고리
Output: leaf=[{parent,child,score}] (Top-K), merged_reasoning
"""

from typing import Dict, Any, List, Tuple
import numpy as np
import os

class JointGateNode:
    def __init__(self, alpha: float = 0.5, beta: float = 0.5, top_k: int = 10):
        """
        Args:
            alpha: 상위 카테고리 가중치 (기본값: 0.5)
            beta: 하위 카테고리 가중치 (기본값: 0.5)
            top_k: 선택할 하위 카테고리 수 (기본값: 10)
        """
        self.alpha = alpha
        self.beta = beta
        self.top_k = top_k
        
        # 환경변수에서 설정 가능
        self.alpha = float(os.getenv("JOINT_ALPHA", str(alpha)))
        self.beta = float(os.getenv("JOINT_BETA", str(beta)))
        self.top_k = int(os.getenv("JOINT_TOP_K", str(top_k)))
    
    def process(
        self,
        probs_upper: Dict[str, float],
        probs_lower_by_parent: Dict[str, Dict[str, float]],
        upper_reasoning: str,
        lower_reasoning: str
    ) -> Tuple[List[Dict[str, Any]], str]:
        """
        로그 가중합 결합을 통해 최종 하위 카테고리를 선택합니다.
        
        Args:
            probs_upper: 상위 카테고리 확률 {parent: prob}
            probs_lower_by_parent: 부모별 하위 카테고리 확률 {p: {c: prob}}
            upper_reasoning: 상위 카테고리 추론 과정
            lower_reasoning: 하위 카테고리 추론 과정
            
        Returns:
            Tuple[List[Dict], str]: (선택된 하위 카테고리 목록, 통합 추론 과정)
        """
        # 1. 로그 가중합 결합 계산
        joint_scores = self._calculate_joint_scores(probs_upper, probs_lower_by_parent)
        
        # 2. joint score로 정렬하여 Top-K 선택
        sorted_candidates = sorted(
            joint_scores, 
            key=lambda x: x['score'], 
            reverse=True
        )
        
        leaf = sorted_candidates[:self.top_k]
        
        # 3. 추론 과정 통합
        merged_reasoning = self._merge_reasoning(upper_reasoning, lower_reasoning, leaf)
        
        return leaf, merged_reasoning
    
    def _calculate_joint_scores(
        self, 
        probs_upper: Dict[str, float], 
        probs_lower_by_parent: Dict[str, Dict[str, float]]
    ) -> List[Dict[str, Any]]:
        """
        로그 가중합 결합 계산
        
        joint(p, c) = α · log P(p | x) + β · log P(c | p, x)
        """
        joint_scores = []
        
        for parent, parent_prob in probs_upper.items():
            if parent_prob <= 0:
                continue
                
            child_probs = probs_lower_by_parent.get(parent, {})
            
            for child, child_prob in child_probs.items():
                if child_prob <= 0:
                    continue
                
                # 로그 가중합 결합 계산
                log_parent = np.log(parent_prob)
                log_child = np.log(child_prob)
                joint_score = self.alpha * log_parent + self.beta * log_child
                
                joint_scores.append({
                    'parent': parent,
                    'child': child,
                    'score': joint_score,
                    'parent_prob': parent_prob,
                    'child_prob': child_prob
                })
        
        return joint_scores
    
    def _merge_reasoning(
        self, 
        upper_reasoning: str, 
        lower_reasoning: str, 
        leaf: List[Dict[str, Any]]
    ) -> str:
        """
        상위/하위 카테고리 추론 과정을 통합합니다.
        """
        merged = f"""
상위 카테고리 분석: {upper_reasoning}

하위 카테고리 분석: {lower_reasoning}

최종 선택된 하위 카테고리 (Top-{len(leaf)}):
"""
        
        for i, item in enumerate(leaf, 1):
            merged += f"{i}. {item['parent']} > {item['child']} (점수: {item['score']:.4f})\n"
        
        return merged.strip()
