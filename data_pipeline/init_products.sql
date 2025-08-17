-- 상품 메타 테이블 

CREATE TABLE IF NOT EXISTS products (
  product_url      TEXT NOT NULL,                              -- 상품 상세 링크
  url_hash         TEXT PRIMARY KEY,                           -- sha256(product_url)
  top_category     VARCHAR(100),                               -- 상위 카테고리 (원본 보존)
  sub_category     VARCHAR(100),                               -- 표준 하위 카테고리
  brand            VARCHAR(128),                               -- '브랜드명 :' 제거한 값
  product_name     VARCHAR(255) NOT NULL,                      -- 따옴표 제거한 상품명
  price            INT NOT NULL,                               -- 원화 정수
  satisfaction_pct NUMERIC(5,2),                               -- 만족도(%)
  review_count     INT,                                        -- 리뷰 수
  wish_count       INT,                                        -- 위시 수
  tags             TEXT,                                       -- 태그 원문(쉼표/슬래시 구분 등)
  updated_at       TIMESTAMPTZ DEFAULT now()                   -- 갱신 시각
);

-- 인덱스 
CREATE INDEX IF NOT EXISTS idx_topcat
  ON products(top_category);

CREATE INDEX IF NOT EXISTS idx_subcat_price
  ON products(sub_category, price);

-- 품질 점검용 제약/체크
ALTER TABLE products
  ADD CONSTRAINT chk_price_positive CHECK (price >= 0) NOT VALID;