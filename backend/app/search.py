# backend/app/search.py
"""
SQL(카테고리·예산) + pgvector TopM 검색 모듈 
- price는 '원' 단위 정수 저장/비교 가정 (표시 포맷은 프론트/응답에서 처리 권장)
- 카테고리 필터는 단일 문자열 또는 복수 리스트 모두 지원
- 벡터 리터럴은 pgvector 규격: [0.123,0.456,...]
- pgvector 연산자: cosine(<=>), inner(<#>), l2(<->)
"""

import os
from .db import fetch
from typing import Iterable, Any, Sequence, Optional

# ---- 튜닝 파라미터 (환경변수로 덮어쓰기 가능) ----
RAG_TOP_M = int(os.getenv("RAG_TOP_M", "30"))
# pgvector 연산자 선택(코사인 유사도 권장): "cosine" | "inner" | "l2"
VECTOR_OP = os.getenv("VECTOR_OP", "cosine").lower()

# 연산자에 따른 ORDER BY/유사도 계산식 구성
if VECTOR_OP == "cosine":
    # cosine distance d in [0,2]; similarity = 1 - d  (벡터 정규화 가정 시 [0,1])
    SIM_EXPR = "1 - (v.embedding <=> $1::vector)"
elif VECTOR_OP == "inner":
    # inner product distance는 -u·v (작을수록 유사) → similarity = -(distance)
    SIM_EXPR = "- (v.embedding <#> $1::vector)"
elif VECTOR_OP == "l2":
    # L2 distance도 작을수록 유사 → similarity = -(distance)
    SIM_EXPR = "- (v.embedding <-> $1::vector)"
else:
    raise ValueError(f"Unsupported VECTOR_OP: {VECTOR_OP}")

# 후보 생성 + 정렬
SEARCH_SQL = f"""
WITH candidates AS (
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
      {SIM_EXPR} AS sim
  FROM product_vectors v
  JOIN products p USING (url_hash)
  WHERE ($2::text[] IS NULL OR p.sub_category = ANY($2))
    AND p.price BETWEEN $3 AND $4
  ORDER BY sim DESC
  LIMIT $5
)
SELECT *
FROM candidates
ORDER BY sim DESC;
"""

def _to_pg_vector_literal(vec: Iterable[float]) -> str:
    """pgvector 문자열 리터럴: [0.12,0.34,...]"""
    return "[" + ",".join(f"{float(x):.6f}" for x in vec) + "]"

def _to_text_array(arg: Any) -> Optional[list[str]]:
    """단일 문자열 -> [str], 리스트/튜플 -> list[str], None->None"""
    if arg is None:
        return None
    if isinstance(arg, str):
        s = arg.strip()
        return [s] if s else None
    if isinstance(arg, (list, tuple)):
        out = []
        for x in arg:
            if x is None:
                continue
            if isinstance(x, str) and x.strip():
                out.append(x.strip())
        return out or None
    return None

async def vector_search(
    query_vec: Sequence[float],
    sub_categories: Any,       # str | list[str] | None
    budget_min: int,
    budget_max: int,
    top_m: Optional[int] = None,
):
    """
    - query_vec: 쿼리 임베딩(정규화 권장)
    - sub_categories: 하위 카테고리(단일/복수), None이면 전체
    - budget_min/max: 예산(원, 정수)
    - top_m: 상위 검색 건수(기본 RAG_TOP_M)
    """
    top_m = int(top_m or RAG_TOP_M)
    vec_str = _to_pg_vector_literal(query_vec)
    cat_array = _to_text_array(sub_categories)
    return await fetch(
        SEARCH_SQL,
        vec_str,           # $1
        cat_array,         # $2
        int(budget_min),   # $3
        int(budget_max),   # $4
        top_m,             # $5
    )