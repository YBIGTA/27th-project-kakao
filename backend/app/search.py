from typing import Any, Optional, Dict, List
from .db import fetch

# sub_category + price(INT) 필터, 메타 정렬
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
