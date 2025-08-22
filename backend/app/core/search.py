from typing import Any, Optional, Dict, List
from .db import fetch

# sub_category + price(INT) 필터, 메타 정렬 (기본)
SEARCH_ALL_SQL = """
SELECT
    p.url_hash,
    p.product_name,
    p.brand,
    p.sub_category,
    p.price,
    p.product_url,
    p.satisfaction_pct,
    p.review_count,
    p.wish_count
FROM products p
WHERE ($1::text[] IS NULL OR p.sub_category = ANY($1))
  AND p.price BETWEEN $2 AND $3
ORDER BY
  p.review_count DESC NULLS LAST,
  p.wish_count  DESC NULLS LAST,
  p.satisfaction_pct DESC NULLS LAST;
"""

# 확장된 필드 포함 (새로운 파이프라인용)
SEARCH_EXTENDED_SQL = """
SELECT
    p.url_hash,
    p.product_name,
    p.brand,
    p.sub_category,
    p.price,
    p.product_url,
    p.satisfaction_pct,
    p.review_count,
    p.wish_count,
    p.category,
    p.description,
    p.image_url
FROM products p
WHERE p.sub_category = ANY($1)
  AND p.price BETWEEN $2 AND $3
ORDER BY
    p.review_count DESC NULLS LAST,
    p.wish_count DESC NULLS LAST,
    p.satisfaction_pct DESC NULLS LAST;
"""

# 카테고리별 상품 수 조회
COUNT_BY_CATEGORY_SQL = """
SELECT 
    p.sub_category,
    COUNT(*) as product_count
FROM products p
WHERE p.sub_category = ANY($1)
  AND p.price BETWEEN $2 AND $3
GROUP BY p.sub_category;
"""

def _to_text_array(arg: Any) -> Optional[list[str]]:
    if arg is None:
        return None
    if isinstance(arg, str):
        s = arg.strip()
        return [s] if s else None
    if isinstance(arg, (list, tuple)):
        out = [x.strip() for x in arg if isinstance(x, str) and x.strip()]
        return out or None
    return None

async def metadata_search_grouped(
    sub_categories: Any,
    budget_min: int,
    budget_max: int,
) -> Dict[str, List[dict]]:
    """
    기존 함수: 카테고리별로 그룹화된 상품 목록 반환
    """
    cat_array = _to_text_array(sub_categories)
    rows = await fetch(
        SEARCH_ALL_SQL,
        cat_array,
        int(budget_min),
        int(budget_max),
    )
    grouped: Dict[str, List[dict]] = {}
    for r in rows:
        d = dict(r)
        grouped.setdefault(d["sub_category"], []).append(d)
    return grouped

async def search_products_extended(
    sub_categories: List[str],
    budget_min: int,
    budget_max: int,
    max_products_per_category: int = 50
) -> List[Dict[str, Any]]:
    """
    새로운 파이프라인용: 확장된 필드를 포함한 상품 목록 반환
    
    Args:
        sub_categories: 하위 카테고리 목록
        budget_min: 최소 예산
        budget_max: 최대 예산
        max_products_per_category: 카테고리당 최대 상품 수
        
    Returns:
        List[Dict]: 필터링된 상품 목록 (평면화)
    """
    if not sub_categories:
        return []
    
    # DB에서 상품 조회
    rows = await fetch(
        SEARCH_EXTENDED_SQL,
        sub_categories,
        int(budget_min),
        int(budget_max)
    )
    
    # 결과를 카테고리별로 그룹화
    grouped_products = {}
    for row in rows:
        product = dict(row)
        sub_category = product['sub_category']
        if sub_category not in grouped_products:
            grouped_products[sub_category] = []
        grouped_products[sub_category].append(product)
    
    # 카테고리별로 상품 수 제한하고 평면화
    filtered_products = []
    for sub_category, products in grouped_products.items():
        # 상위 상품만 선택
        limited_products = products[:max_products_per_category]
        filtered_products.extend(limited_products)
    
    return filtered_products

async def get_product_count_by_category(
    sub_categories: List[str],
    budget_min: int,
    budget_max: int
) -> Dict[str, int]:
    """
    카테고리별 상품 수를 조회합니다.
    
    Args:
        sub_categories: 하위 카테고리 목록
        budget_min: 최소 예산
        budget_max: 최대 예산
        
    Returns:
        Dict[str, int]: 카테고리별 상품 수
    """
    if not sub_categories:
        return {}
    
    rows = await fetch(
        COUNT_BY_CATEGORY_SQL,
        sub_categories,
        int(budget_min),
        int(budget_max)
    )
    
    return {row['sub_category']: row['product_count'] for row in rows}

async def search_products_for_leaf(
    leaf: List[Dict[str, Any]],
    budget_min: int,
    budget_max: int,
    max_products_per_category: int = 50
) -> List[Dict[str, Any]]:
    """
    leaf 형태의 입력을 받아서 상품을 검색합니다.
    
    Args:
        leaf: 선택된 하위 카테고리 목록 [{parent, child, score, ...}]
        budget_min: 최소 예산
        budget_max: 최대 예산
        max_products_per_category: 카테고리당 최대 상품 수
        
    Returns:
        List[Dict]: 필터링된 상품 목록
    """
    if not leaf:
        return []
    
    # 하위 카테고리 목록 추출
    sub_categories = [item['child'] for item in leaf]
    
    return await search_products_extended(
        sub_categories, budget_min, budget_max, max_products_per_category
    )
