"""Product Repositories — INTEGRATION POINT
- CSVProductRepo: CSV 파일에서 후보를 읽는다. 컬럼은 아래와 같다.
  [top_category,sub_category,sub_tab,brand,product_name,price,satisfaction_pct,review_count,wish_count,tags,product_url]
- SQLProductRepo(Postgres): 같은 컬럼을 가진 `products` 테이블에서 읽는다.
- 선택 로직: --products가 주어지면 CSV, 아니면 .env의 DB_URL 사용.
"""
from typing import List, Dict, Any, Optional
import pandas as pd
from sqlalchemy import create_engine, text
from core.config import POPULARITY_WEIGHTS, NORMALIZATION_THRESHOLDS

class ProductRepo:
    async def fetch(self, top3_children: List[str], budget_min: int, budget_max: int) -> List[Dict[str,Any]]:
        raise NotImplementedError

    def _calculate_popularity_score(self, row: Dict[str, Any]) -> float:
        """인기도 점수 계산: 만족도 + 리뷰수 + 위시수 가중 평균"""
        satisfaction = float(row.get("satisfaction_pct") or 0) / 100.0  # 0-1로 정규화
        review_count = float(row.get("review_count") or 0)
        wish_count = float(row.get("wish_count") or 0)
        
        # 가중치: 환경변수에서 가져오기
        weights = POPULARITY_WEIGHTS
        thresholds = NORMALIZATION_THRESHOLDS
        
        score = (weights["satisfaction"] * satisfaction + 
                weights["review_count"] * min(review_count / thresholds["review_count"], 1.0) + 
                weights["wish_count"] * min(wish_count / thresholds["wish_count"], 1.0))
        
        return score

class CSVProductRepo(ProductRepo):
    def __init__(self, csv_path: str):
        self.csv_path = csv_path

    async def fetch(self, top3_children: List[str], budget_min: int, budget_max: int) -> List[Dict[str,Any]]:
        df = pd.read_csv(self.csv_path)
        
        # 하위 카테고리 필터링
        df = df[df["sub_category"].isin(top3_children)]
        
        # 예산 필터링
        df = df[(df["price"] >= budget_min) & (df["price"] <= budget_max)]
        
        # 인기도 점수 계산 및 정렬
        df["popularity_score"] = df.apply(self._calculate_popularity_score, axis=1)
        df = df.sort_values(["popularity_score", "price"], ascending=[False, True])
        
        # 결과를 표준 형식으로 변환
        result = []
        for _, row in df.iterrows():
            product = {
                "id": str(row.get("product_url", "")),  # product_url을 ID로 사용
                "title": row["product_name"],
                "brand": str(row.get("brand", "")),  # 이미 정규화된 상태
                "price": int(row["price"]),
                "category_parent": row["top_category"],
                "category_child": row["sub_category"],
                "url": row["product_url"],
                "satisfaction_pct": float(row.get("satisfaction_pct", 0)),
                "review_count": int(row.get("review_count", 0)),
                "wish_count": int(row.get("wish_count", 0)),
                "tags": row.get("tags", ""),
                "popularity_score": float(row["popularity_score"]),
                "updated_at": row.get("updated_at", None)
            }
            result.append(product)
        
        return result

class SQLProductRepo(ProductRepo):
    def __init__(self, db_url: str):
        if not db_url:
            raise RuntimeError("DB_URL is empty. Set .env DB_URL or pass --products CSV.")
        self.engine = create_engine(db_url, pool_pre_ping=True)

    async def fetch(self, top3_children: List[str], budget_min: int, budget_max: int) -> List[Dict[str,Any]]:
        # 실제 데이터 구조에 맞는 SQL 쿼리
        weights = POPULARITY_WEIGHTS
        thresholds = NORMALIZATION_THRESHOLDS
        
        q = text(f"""
            SELECT 
                top_category, sub_category, brand, product_name, 
                price, 
                COALESCE(satisfaction_pct, 0) as satisfaction_pct, 
                COALESCE(review_count, 0) as review_count, 
                COALESCE(wish_count, 0) as wish_count, 
                tags, product_url, updated_at
            FROM products
            WHERE sub_category IN :cats AND price BETWEEN :minp AND :maxp
            ORDER BY 
                ({weights["satisfaction"]} * (COALESCE(satisfaction_pct, 0)/100.0) + 
                 {weights["review_count"]} * LEAST(COALESCE(review_count, 0)/{thresholds["review_count"]}.0, 1.0) + 
                 {weights["wish_count"]} * LEAST(COALESCE(wish_count, 0)/{thresholds["wish_count"]}.0, 1.0)) DESC,
                price ASC
            LIMIT 400
        """)
        
        cats = tuple(top3_children)
        with self.engine.connect() as conn:
            rows = conn.execute(q, {"cats": cats, "minp": budget_min, "maxp": budget_max}).mappings().all()
        
        # 결과를 표준 형식으로 변환
        result = []
        for row in rows:
            row_dict = dict(row)
            product = {
                "id": str(row_dict.get("product_url", "")),  # product_url을 ID로 사용
                "title": row_dict["product_name"],
                "brand": str(row_dict.get("brand", "")),  # 이미 정규화된 상태
                "price": int(row_dict["price"]),
                "category_parent": row_dict["top_category"],
                "category_child": row_dict["sub_category"],
                "url": row_dict["product_url"],
                "satisfaction_pct": float(row_dict.get("satisfaction_pct") or 0),
                "review_count": int(row_dict.get("review_count") or 0),
                "wish_count": int(row_dict.get("wish_count") or 0),
                "tags": row_dict.get("tags", ""),
                "popularity_score": self._calculate_popularity_score(row_dict),
                "updated_at": row_dict.get("updated_at", None)
            }
            result.append(product)
        
        return result

def new_repo(products_csv: Optional[str]) -> ProductRepo:
    if products_csv:
        return CSVProductRepo(products_csv)
    # DB_URL은 환경변수에서 가져와야 함
    from core.config import DB_URL
    if DB_URL:
        return SQLProductRepo(DB_URL)
    raise RuntimeError("No product source configured. Provide --products CSV or set DB_URL in .env.")
