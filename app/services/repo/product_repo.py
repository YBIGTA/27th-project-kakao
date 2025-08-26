
import os
from typing import List, Dict, Any, Optional
import pandas as pd
from app.core.config import REPO
import logging

logger = logging.getLogger(__name__)

# PostgreSQL 의존성 처리
try:
    import psycopg2
    try:
        from psycopg2.extras import RealDictCursor
        PSYCOPG2_AVAILABLE = True
        logger.info("PostgreSQL support available with RealDictCursor")
    except ImportError:
        # RealDictCursor가 없는 경우 기본 cursor 사용
        PSYCOPG2_AVAILABLE = True
        logger.info("PostgreSQL support available with basic cursor")
        RealDictCursor = None
except ImportError:
    PSYCOPG2_AVAILABLE = False
    RealDictCursor = None
    logger.warning("psycopg2 not available. Install with: pip install psycopg2-binary")

# 타입 어노테이션
RealDictCursor: Any  # type: ignore

class ProductRepo:
    def __init__(self, dsn: Optional[str] = None, csv_path: Optional[str] = None):
        self.dsn = dsn or REPO.postgres_dsn
        self.csv_path = csv_path or REPO.csv_path
        self._df: Optional[pd.DataFrame] = None

        # PostgreSQL 연결 테스트
        self._pg_available = self._test_postgres_connection()
        if self._pg_available:
            logger.info("PostgreSQL connection successful - using database as primary source")
        else:
            logger.warning("PostgreSQL connection failed - falling back to CSV")
            self._ensure_csv_fallback()

    def _test_postgres_connection(self) -> bool:
        """PostgreSQL 연결 테스트"""
        if not PSYCOPG2_AVAILABLE or not self.dsn:
            return False
            
        try:
            conn = psycopg2.connect(self.dsn)
            cur = conn.cursor()
            
            # 테이블 존재 여부 확인 (products_meta 테이블 사용)
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'products_meta'
                );
            """)
            
            result = cur.fetchone()
            if result is None:
                logger.warning("No result from table existence query")
                return False
                
            table_exists = result[0]
            cur.close()
            conn.close()
            
            if not table_exists:
                logger.warning("products_meta table does not exist in PostgreSQL")
                return False
                
            return True
            
        except Exception as e:
            logger.warning(f"PostgreSQL connection test failed: {e}")
            return False

    def _ensure_csv_fallback(self):
        """CSV fallback 준비"""
        if not os.path.exists(self.csv_path):
            logger.error(f"CSV fallback file not found: {self.csv_path}")
            raise FileNotFoundError(f"Neither PostgreSQL nor CSV available: {self.csv_path}")
        
        logger.info(f"Using CSV fallback: {self.csv_path}")

    def _ensure_df(self):
        """CSV DataFrame 로드"""
        if self._df is None:
            try:
                self._df = pd.read_csv(self.csv_path)
                logger.info(f"CSV loaded: {len(self._df)} rows, {len(self._df.columns)} columns")
            except Exception as e:
                logger.error(f"Failed to load CSV: {e}")
                raise RuntimeError(f"CSV loading failed: {e}")

    # --- PostgreSQL Primary Path ---
    def query_candidates_pg(self, child_categories: List[str], budget_min: int, budget_max: int) -> List[Dict[str, Any]]:
        """PostgreSQL에서 상품 조회 (주요 경로) - products_meta 테이블 사용"""
        if not self._pg_available:
            raise RuntimeError("PostgreSQL not available")
            
        try:
            conn = psycopg2.connect(self.dsn)
            
            # RealDictCursor 사용 가능 여부에 따라 cursor 선택
            if RealDictCursor is not None:
                cur = conn.cursor(cursor_factory=RealDictCursor)
            else:
                cur = conn.cursor()
            
            # products_meta 테이블 구조에 맞는 쿼리
            query = """
            SELECT 
                p.product_name,
                p.brand,
                p.price,
                p.product_url,
                p.sub_category as child_category,
                p.top_category as parent_category,
                p.satisfaction_pct as rating,
                p.review_count,
                p.wish_count,
                p.tags
            FROM products_meta p
            WHERE p.sub_category = ANY(%s) 
            AND p.price BETWEEN %s AND %s
            ORDER BY 
                CASE 
                    WHEN p.sub_category = ANY(%s) THEN 1
                    ELSE 2
                END,
                p.price ASC,
                p.satisfaction_pct DESC NULLS LAST
            LIMIT 500
            """
            
            cur.execute(query, (child_categories, budget_min, budget_max, child_categories))
            rows = cur.fetchall()
            
            # 결과를 딕셔너리로 변환
            candidates = []
            for row in rows:
                if RealDictCursor is not None:
                    # RealDictCursor 사용 시
                    candidate = {
                        "product_name": row['product_name'],
                        "brand": row['brand'],
                        "price": int(row['price']) if row['price'] is not None else None,
                        "product_url": row['product_url'],
                        "child_category": row['child_category'],
                        "parent_category": row['parent_category'],
                        "description": row.get('tags', ''),  # tags를 description으로 사용
                        "image_url": "",  # 이미지 URL은 없음
                        "rating": float(row['rating']) if row['rating'] is not None else None,
                        "review_count": int(row['review_count']) if row['review_count'] is not None else None,
                        "wish_count": int(row['wish_count']) if row['wish_count'] is not None else None
                    }
                else:
                    # 기본 cursor 사용 시 (인덱스 기반)
                    candidate = {
                        "product_name": row[0],  # product_name
                        "brand": row[1],         # brand
                        "price": int(row[2]) if row[2] is not None else None,  # price
                        "product_url": row[3],   # product_url
                        "child_category": row[4], # sub_category
                        "parent_category": row[5], # top_category
                        "description": row[9] if len(row) > 9 and row[9] else "",  # tags
                        "image_url": "",  # 이미지 URL은 없음
                        "rating": float(row[6]) if len(row) > 6 and row[6] is not None else None,  # satisfaction_pct
                        "review_count": int(row[7]) if len(row) > 7 and row[7] is not None else None,  # review_count
                        "wish_count": int(row[8]) if len(row) > 8 and row[8] is not None else None   # wish_count
                    }
                candidates.append(candidate)
            
            cur.close()
            conn.close()
            
            logger.info(f"PostgreSQL query successful: {len(candidates)} products found")
            return candidates
            
        except Exception as e:
            logger.error(f"PostgreSQL query failed: {e}")
            raise RuntimeError(f"Database query failed: {e}")

    # --- CSV Fallback Path ---
    def query_candidates_csv(self, child_categories: List[str], budget_min: int, budget_max: int) -> List[Dict[str, Any]]:
        """CSV에서 상품 조회 (fallback 경로)"""
        self._ensure_df()
        if self._df is None:
            raise RuntimeError("CSV DataFrame not loaded")
        df = self._df.copy()
        
        # 컬럼명 정규화 및 매핑
        column_mapping = self._normalize_csv_columns(df)
        
        # 디버깅을 위한 로그
        logger.info(f"Filtering for child categories: {child_categories}")
        logger.info(f"Budget range: {budget_min:,}원 ~ {budget_max:,}원")
        logger.info(f"Available child categories in CSV: {df[column_mapping['child_category']].unique()}")
        
        # 카테고리 필터링
        df_filtered = df[df[column_mapping['child_category']].isin(child_categories)]
        logger.info(f"After category filtering: {len(df_filtered)} products")
        
        # 가격 필터링 (숫자 변환 및 범위 필터링)
        try:
            # 가격 컬럼을 숫자로 변환
            price_col = column_mapping['price']
            df_filtered[price_col] = pd.to_numeric(df_filtered[price_col], errors='coerce')
            
            # NaN 값 제거
            df_filtered = df_filtered.dropna(subset=[price_col])
            logger.info(f"After price conversion: {len(df_filtered)} products")
            
            # 예산 범위 필터링
            df_filtered = df_filtered[
                (df_filtered[price_col] >= budget_min) & 
                (df_filtered[price_col] <= budget_max)
            ]
            logger.info(f"After budget filtering: {len(df_filtered)} products")
            
        except Exception as e:
            logger.error(f"Price filtering failed: {e}")
            return []
        
        # 결과 변환
        candidates = []
        for _, row in df_filtered.iterrows():
            try:
                candidate = {
                    "product_name": str(row[column_mapping['name']]),
                    "brand": str(row[column_mapping['brand']]) if column_mapping['brand'] is not None and column_mapping['brand'] in row else None,  # type: ignore
                    "price": int(row[column_mapping['price']]) if pd.notna(row[column_mapping['price']]) else None,
                    "product_url": str(row[column_mapping['url']]) if column_mapping['url'] is not None and column_mapping['url'] in row else None,  # type: ignore
                    "child_category": str(row[column_mapping['child_category']]),
                    "parent_category": str(row[column_mapping['parent_category']]) if column_mapping['parent_category'] in row else None,
                    "description": "",
                    "image_url": ""
                }
                candidates.append(candidate)
            except Exception as e:
                logger.warning(f"Failed to process row: {e}")
                continue
        
        logger.info(f"CSV fallback query successful: {len(candidates)} products found")
        return candidates

    def _normalize_csv_columns(self, df: pd.DataFrame) -> Dict[str, Optional[str]]:
        """CSV 컬럼명 정규화"""
        df.columns = [str(c).strip().lower() for c in df.columns]
        
        # 컬럼 매핑 규칙
        column_mapping: Dict[str, Optional[str]] = {}
        
        # 필수 컬럼들
        required_columns = {
            'name': ['product_name', 'productname', 'title'],
            'price': ['price', 'sale_price', 'amount', 'price_krw', 'price_won'],
            'child_category': ['sub_category', 'subcategory', 'child', 'category_child', 'category2'],
            'parent_category': ['top_category', 'category', 'parent_category', 'main_category']
        }
        
        # URL 컬럼 (선택사항)
        url_variants = ['product_url', 'url', 'link', 'gift_link']
        url_col: Optional[str] = None
        for variant in url_variants:
            if variant in df.columns:
                url_col = variant
                break
        
        # 브랜드 컬럼 (선택사항)
        brand_col: Optional[str] = 'brand' if 'brand' in df.columns else None
        
        # 필수 컬럼 검증
        for required_name, variants in required_columns.items():
            found = False
            for variant in variants:
                if variant in df.columns:
                    column_mapping[required_name] = variant
                    found = True
                    break

            if not found:
                logger.error(f"Required column '{required_name}' not found. Available columns: {list(df.columns)}")
                raise RuntimeError(f"Required column '{required_name}' not found. Available columns: {list(df.columns)}")
        
        # 선택사항 컬럼들
        column_mapping['url'] = url_col
        column_mapping['brand'] = brand_col
        
        logger.info(f"Column mapping successful: {column_mapping}")
        return column_mapping

    def query_candidates(self, child_categories: List[str], budget_min: int, budget_max: int) -> List[Dict[str, Any]]:
        """상품 후보 조회 - PostgreSQL 우선, CSV fallback"""
        try:
            if self._pg_available:
                return self.query_candidates_pg(child_categories, budget_min, budget_max)
            else:
                return self.query_candidates_csv(child_categories, budget_min, budget_max)
        except Exception as e:
            logger.error(f"All query methods failed: {e}")
            # 최종 fallback: 빈 결과 반환
            return []
