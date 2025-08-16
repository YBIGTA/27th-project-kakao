# backend/app/search.py
"""
SQL(카테고리·예산) + pgvector TopM 검색 모듈 
- price는 '원' 단위 정수 저장/비교 가정 (표시 포맷은 프론트/응답에서 처리 권장)
- 카테고리 필터는 단일 문자열 또는 복수 리스트 모두 지원
- 벡터 리터럴은 pgvector 규격: [0.123,0.456,...]
"""

from typing import Sequence, Any, Iterable
import os
from .db import fetch

# ---- 튜닝 파라미터 (환경변수로 덮어쓰기 가능) ----
RAG_TOP_M = int(os.getenv("RAG_TOP_M", "200"))
# pgvector 연산자 선택(코사인 유사도 권장): "cosine" | "inner" | "l2"
VECTOR_OP = os.getenv("VECTOR_OP", "cosine").lower()

# 연산자에 따른 ORDER BY/유사도 계산식 구성
if VECTOR_OP == "cosine":
    # pgvector: cosine_ops에서 <=> 는 cosine distance (0=유사, 2=반대)
    ORDER_EXPR = "pv.embedding <=> $1::vector"
    SIM_EXPR = f"1 - ({ORDER_EXPR})"
elif VECTOR_OP == "inner":
    # inner product는 값이 클수록 유사. 정렬은 DESC, sim은 직접 inner product 사용
    ORDER_EXPR = f"- (pv.embedding <#> $1::vector)"  # 더 큰 inner product가 먼저 오도록 음수화
    SIM_EXPR = f"- ({ORDER_EXPR})"                   # 다시 부호 되돌려 sim=inner product
elif VECTOR_OP == "l2":
    # euclidean distance(작을수록 유사)
    ORDER_EXPR = "pv.embedding <-> $1::vector"
    SIM_EXPR = f"1 / (1 + ({ORDER_EXPR}))"          # 간단 스케일링(0~1 근사)
else:
    raise ValueError("Unsupported VECTOR_OP. Use one of: cosine | inner | l2")

# 단일/복수 카테고리 모두 처리 가능한 SQL
SEARCH_SQL = f"""
WITH candidates AS (
  SELECT
    p.url_hash,
    p.product_name,
    p.brand,
    p.sub_category,
    p.price,
    p.image_url,
    p.product_url,
    {SIM_EXPR} AS sim
  FROM product_vectors pv
  JOIN products p USING (url_hash)
  WHERE (
      $2::text[] IS NULL
      OR p.sub_category = ANY($2::text[])
    )
    AND (p.price BETWEEN $3 AND $4)              -- 예산(원, 정수)
  ORDER BY {ORDER_EXPR}
  LIMIT $5
)
SELECT *
FROM candidates
ORDER BY sim DESC;
"""

def _to_pg_vector_literal(vec: Iterable[float]) -> str:
    """pgvector가 받는 문자열 리터럴로 변환: [0.12,0.34,...]"""
    return "[" + ",".join(f"{float(x):.6f}" for x in vec) + "]"

def _to_text_array(arg: Any) -> list[str] | None:
    """단일 문자열 -> [str], 리스트/튜플 -> list[str], None->None"""
    if arg is None:
        return None
    if isinstance(arg, (list, tuple)):
        return [str(x) for x in arg] if arg else None
    return [str(arg)]

async def vector_search(
    query_vec: list[float],
    sub_categories: str | list[str] | None,
    budget_min: int,
    budget_max: int,
    top_m: int | None = None,
) -> Sequence[Any]:
    """
    pgvector TopM 검색
    - query_vec: 쿼리 벡터(list[float])
    - sub_categories: 단일/복수 하위카테고리(없으면 None)
    - budget_min/max: 예산(원, 정수)
    - top_m: 상위 검색 건수(기본 RAG_TOP_M)
    """
    top_m = int(top_m or RAG_TOP_M)
    vec_str = _to_pg_vector_literal(query_vec)
    cat_array = _to_text_array(sub_categories)
    return await fetch(
        SEARCH_SQL,
        vec_str,
        cat_array,
        int(budget_min),
        int(budget_max),
        top_m,
    )
