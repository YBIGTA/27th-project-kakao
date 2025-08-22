"""
파이프라인 엔진 (pipeline.py)

구조:
1. uppercategory_node.py - 상위 카테고리 확률 계산
2. lowercategory_node.py - 하위 카테고리 확률 계산  
3. joint_gate_node.py - 로그 가중합 결합 및 Top-K 선택
4. DB 필터링 - 선택된 카테고리로 상품 필터링
5. product_node.py - 최종 상품 랭킹/가드레일
"""

from typing import Dict, Any, List
from .preprocess.main_processor import main
from .nodes.uppercategory_node import UpperCategoryNode
from .nodes.lowercategory_node import LowerCategoryNode
from .nodes.joint_gate_node import JointGateNode
from .core.search import search_products_for_leaf
from .nodes.product_node import ProductNode

class PipelineEngine:
    def __init__(self):
        """
        파이프라인 엔진을 초기화합니다.
        """
        self.upper_node = UpperCategoryNode()
        self.lower_node = LowerCategoryNode()
        self.joint_gate_node = JointGateNode()
        self.product_node = ProductNode()
    
    async def run(
        self,
        file_bytes: bytes,
        filename: str,
        age: int,
        gender: str,
        relation: str,
        budget_min: int,
        budget_max: int,
    ) -> Dict[str, Any]:
        """
        파이프라인을 실행합니다.
        
        Args:
            file_bytes: 파일 bytes
            filename: 파일명
            age: 사용자 연령
            gender: 사용자 성별
            relation: 관계
            budget_min: 최소 예산
            budget_max: 최대 예산
            
        Returns:
            Dict[str, Any]: 파이프라인 실행 결과
        """
        try:
            # 1) 전처리: main_processor.py로 CSV 파일 생성
            csv_file_path: str = main(file_bytes=file_bytes, filename=filename)
            if not csv_file_path:
                raise ValueError("전처리된 CSV 파일을 생성하지 못했습니다.")
            
            # 사용자 프로필 구성
            user_profile = {
                "age": age,
                "gender": gender,
                "relation": relation,
                "budget_min": budget_min,
                "budget_max": budget_max,
            }
            
            # 전처리된 데이터 구성
            preprocessed_data = {
                "csv_file_path": csv_file_path,
                "user_profile": user_profile,
            }
            
            # 2) 상위 카테고리 노드
            upper_result = self.upper_node.process(
                preprocessed_data
            )
            probs_upper, upper_reasoning, upper_confidence_data = upper_result
            
            if not probs_upper:
                return {
                    "analysis": {
                        "message": "상위 카테고리를 분석할 수 없습니다.",
                        "upper_reasoning": upper_reasoning
                    },
                    "selections": []
                }
            
            # 3) 하위 카테고리 노드
            lower_result = self.lower_node.process(
                preprocessed_data
            )
            probs_lower_by_parent, lower_reasoning, lower_confidence_data = lower_result
            
            if not probs_lower_by_parent:
                return {
                    "analysis": {
                        "message": "하위 카테고리를 분석할 수 없습니다.",
                        "upper_reasoning": upper_reasoning,
                        "lower_reasoning": lower_reasoning
                    },
                    "selections": []
                }
            
            # 4) 결합+게이트 연산 노드
            leaf, merged_reasoning = self.joint_gate_node.process(
                probs_upper, probs_lower_by_parent, upper_reasoning, lower_reasoning
            )
            
            if not leaf:
                return {
                    "analysis": {
                        "message": "최종 카테고리를 선택할 수 없습니다.",
                        "merged_reasoning": merged_reasoning
                    },
                    "selections": []
                }
            
            # 5) DB 필터링
            candidates = await search_products_for_leaf(
                leaf, budget_min, budget_max
            )
            
            if not candidates:
                return {
                    "analysis": {
                        "message": "조건에 맞는 상품을 찾을 수 없습니다.",
                        "merged_reasoning": merged_reasoning,
                        "selected_categories": [item['child'] for item in leaf]
                    },
                    "selections": []
                }
            
            # 6) 상품 랭킹/가드레일 노드
            final_products = await self.product_node.select_final_products(
                candidates, user_profile, 
                sentence_context=self._extract_sentence_context(
                    upper_confidence_data, lower_confidence_data
                )
            )
            
            # 결과 구성
            analysis = {
                "upper_categories": probs_upper,
                "lower_categories_by_parent": probs_lower_by_parent,
                "selected_categories": [item['child'] for item in leaf],
                "category_scores": [item['score'] for item in leaf],
                "reasoning": merged_reasoning,
                "candidate_count": len(candidates)
            }
            
            return {
                "analysis": analysis,
                "selections": final_products
            }
            
        except Exception as e:
            print(f"파이프라인 실행 중 오류 발생: {e}")
            return {
                "analysis": {
                    "message": f"파이프라인 실행 중 오류가 발생했습니다: {str(e)}"
                },
                "selections": []
            }
    
    def _extract_sentence_context(
        self,
        upper_confidence_data: Dict[str, Any] = None,
        lower_confidence_data: Dict[str, Any] = None
    ) -> List[str]:
        """
        문장 라우팅 정보를 추출합니다.
        LLM 응답의 evidence와 reason 필드를 활용합니다.
        
        Args:
            upper_confidence_data: 상위 카테고리 confidence 데이터
            lower_confidence_data: 하위 카테고리 confidence 데이터
            
        Returns:
            List[str]: 관련 문장들
        """
        sentence_context = []
        
        # 1. 상위 카테고리 evidence 추출
        if upper_confidence_data:
            for cat_name, cat_data in upper_confidence_data.items():
                if isinstance(cat_data, dict):
                    # evidence 필드에서 문장 추출
                    evidence = cat_data.get("evidence", [])
                    for ev in evidence:
                        if isinstance(ev, dict) and "text" in ev:
                            sentence_context.append(f"[{cat_name}] {ev['text']}")
                    
                    # reason 필드 추가
                    reason = cat_data.get("reason", "")
                    if reason:
                        sentence_context.append(f"[{cat_name} 근거] {reason}")
        
        # 2. 하위 카테고리 evidence 추출
        if lower_confidence_data:
            for cat_path, cat_data in lower_confidence_data.items():
                if isinstance(cat_data, dict):
                    # evidence 필드에서 문장 추출
                    evidence = cat_data.get("evidence", [])
                    for ev in evidence:
                        if isinstance(ev, dict) and "text" in ev:
                            sentence_context.append(f"[{cat_path}] {ev['text']}")
                    
                    # reason 필드 추가
                    reason = cat_data.get("reason", "")
                    if reason:
                        sentence_context.append(f"[{cat_path} 근거] {reason}")
        
        return sentence_context
