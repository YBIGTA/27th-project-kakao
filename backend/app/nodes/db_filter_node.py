"""
DB 필터링 노드 (db_filter_node.py)

Input: leaf (선택된 카테고리), budget_min, budget_max
Process:
    - 선택된 하위 카테고리로 상품 검색
    - 가격 범위 필터링
    - 카테고리별 상품 수 제한
Output: candidates (필터링된 상품 목록)
"""

from typing import Dict, Any, List
from ..core.search import search_products_for_leaf

class DBFilterNode:
    def __init__(self, max_products_per_category: int = 50):
        """
        Args:
            max_products_per_category: 카테고리당 최대 상품 수 (기본값: 50)
        """
        self.max_products_per_category = max_products_per_category
    
    async def process(
        self,
        leaf: List[Dict[str, Any]],
        budget_min: int,
        budget_max: int
    ) -> List[Dict[str, Any]]:
        """
        DB에서 상품을 필터링합니다.
        
        Args:
            leaf: 선택된 하위 카테고리 목록 [{parent, child, score, ...}]
            budget_min: 최소 예산
            budget_max: 최대 예산
            
        Returns:
            List[Dict]: 필터링된 상품 목록
        """
        if not leaf:
            return []
        
        try:
            # DB에서 상품 검색
            candidates = await search_products_for_leaf(
                leaf, 
                budget_min, 
                budget_max, 
                self.max_products_per_category
            )
            
            return candidates
            
        except Exception as e:
            print(f"DB 필터링 노드 오류: {e}")
            return []
