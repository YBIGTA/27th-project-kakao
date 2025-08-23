"""
PostgreSQL 연결 관리 모듈
실무에서 사용하는 안전하고 효율적인 DB 연결 관리
"""

import os
import asyncpg
from typing import Any, Iterable, List, Dict, Optional

# 환경변수 설정
_DB_URL = os.getenv("DB_URL")
_POOL_MIN = int(os.getenv("DB_POOL_MIN", "1"))
_POOL_MAX = int(os.getenv("DB_POOL_MAX", "10"))
_STMT_TIMEOUT = int(os.getenv("PG_STMT_TIMEOUT_MS", "5000"))

_pool: Optional[asyncpg.Pool] = None

async def get_pool() -> asyncpg.Pool:
    """DB 연결 풀을 가져옵니다."""
    global _pool
    if _pool is None:
        if not _DB_URL:
            raise RuntimeError("DB_URL이 설정되지 않았습니다.")
        
        _pool = await asyncpg.create_pool(
            dsn=_DB_URL,
            min_size=_POOL_MIN,
            max_size=_POOL_MAX,
            statement_cache_size=0,
        )
        print(f"✅ PostgreSQL 연결 풀 생성 완료 (min: {_POOL_MIN}, max: {_POOL_MAX})")
    return _pool

async def fetch(sql: str, *args: Iterable[Any]) -> List[asyncpg.Record]:
    """
    일반적인 쿼리 실행 함수
    
    Args:
        sql: SQL 쿼리
        *args: 쿼리 파라미터
        
    Returns:
        List[asyncpg.Record]: 쿼리 결과
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(sql, *args, timeout=_STMT_TIMEOUT/1000.0)

async def fetchrow(sql: str, *args: Iterable[Any]) -> Optional[asyncpg.Record]:
    """
    단일 행 조회 함수
    
    Args:
        sql: SQL 쿼리
        *args: 쿼리 파라미터
        
    Returns:
        Optional[asyncpg.Record]: 단일 행 결과 또는 None
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(sql, *args, timeout=_STMT_TIMEOUT/1000.0)

async def execute(sql: str, *args: Iterable[Any]) -> str:
    """
    INSERT/UPDATE/DELETE 실행 함수
    
    Args:
        sql: SQL 쿼리
        *args: 쿼리 파라미터
        
    Returns:
        str: 실행 결과 메시지
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.execute(sql, *args, timeout=_STMT_TIMEOUT/1000.0)

async def close_pool():
    """연결 풀을 종료합니다."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        print("✅ PostgreSQL 연결 풀 종료 완료")

# 상품 검색 전용 함수들
def _calculate_popularity_score(row: Dict[str, Any]) -> float:
    """인기도 점수 계산: 리뷰수 + 만족도 + 위시리스트"""
    review_count = float(row.get("review_count") or 0)
    satisfaction_pct = float(row.get("satisfaction_pct") or 0)
    wish_count = float(row.get("wish_count") or 0)
    
    # 가중 평균으로 인기도 점수 계산
    popularity_score = (
        review_count * 0.5 +      # 리뷰수 50%
        satisfaction_pct * 0.3 +  # 만족도 30%
        wish_count * 0.2          # 위시리스트 20%
    )
    
    return popularity_score

async def fetch_products(
    sub_categories: List[str], 
    budget_min: int, 
    budget_max: int,
    limit: int = 400
) -> List[Dict[str, Any]]:
    """
    DB에서 상품을 가져옵니다.
    
    Args:
        sub_categories: 하위 카테고리 목록
        budget_min: 최소 예산
        budget_max: 최대 예산
        limit: 조회할 상품 수 제한
        
    Returns:
        List[Dict[str, Any]]: 상품 목록
    """
    sql = """
        SELECT 
            top_category, sub_category, brand, product_name, 
            price, 
            COALESCE(satisfaction_pct, 0) as satisfaction_pct, 
            COALESCE(review_count, 0) as review_count, 
            COALESCE(wish_count, 0) as wish_count, 
            tags, product_url, updated_at
        FROM products
        WHERE sub_category = ANY($1) AND price BETWEEN $2 AND $3
        ORDER BY review_count DESC, price ASC
        LIMIT $4
    """
    
    try:
        rows = await fetch(sql, sub_categories, budget_min, budget_max, limit)
        
        # 결과를 표준 형식으로 변환
        result = []
        for row in rows:
            row_dict = dict(row)
            product = {
                "id": str(row_dict.get("product_url", "")),
                "title": row_dict["product_name"],
                "brand": str(row_dict.get("brand", "")),
                "price": int(row_dict["price"]),
                "category_parent": row_dict["top_category"],
                "category_child": row_dict["sub_category"],
                "url": row_dict["product_url"],
                "satisfaction_pct": float(row_dict.get("satisfaction_pct") or 0),
                "review_count": int(row_dict.get("review_count") or 0),
                "wish_count": int(row_dict.get("wish_count") or 0),
                "tags": row_dict.get("tags", ""),
                "popularity_score": _calculate_popularity_score(row_dict),
                "updated_at": row_dict.get("updated_at", None)
            }
            result.append(product)
        
        print(f"✅ 상품 조회 완료: {len(result)}개 (카테고리: {sub_categories}, 예산: {budget_min}-{budget_max})")
        return result
        
    except Exception as e:
        print(f"❌ 상품 조회 실패: {e}")
        return []

async def get_product_count(sub_categories: List[str], budget_min: int, budget_max: int) -> int:
    """
    조건에 맞는 상품 수를 조회합니다.
    
    Args:
        sub_categories: 하위 카테고리 목록
        budget_min: 최소 예산
        budget_max: 최대 예산
        
    Returns:
        int: 상품 수
    """
    sql = """
        SELECT COUNT(*) as count
        FROM products
        WHERE sub_category = ANY($1) AND price BETWEEN $2 AND $3
    """
    
    try:
        row = await fetchrow(sql, sub_categories, budget_min, budget_max)
        return int(row["count"]) if row else 0
    except Exception as e:
        print(f"❌ 상품 수 조회 실패: {e}")
        return 0
