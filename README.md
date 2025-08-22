
# 🎁 **카카오톡 대화 기반 맞춤형 선물 추천 시스템**

카카오톡 대화내역을 분석하여 개인 맞춤형 선물을 추천하는 서비스입니다.  


## 🚀 **작동 방법**

### **1. 환경 설정**

```bash
# 프로젝트 클론
git clone <repository-url>
cd 27th-project-kakao/backend

# 의존성 설치
pip install -r requirements.txt


### **2. 실행**

```bash
cd app

# 기본 실행 
python run_preprocess_pipeline.py \
  --input 파일명.csv \
  --user "상대방 이름" \
  --age 나이 \
  --gender F/M \
  --relation "관계" \
  --budget-min 예산 하한 \
  --budget-max 예산 상한
```

## 📁 **프로젝트 구조**

```
27th-project-kakao/
├── backend/                          # 백엔드 애플리케이션
│   ├── app/                          # 메인 애플리케이션
│   │   ├── core/                     # 핵심 비즈니스 로직
│   │   │   ├── config.py             # 환경변수, 하이퍼파라미터
│   │   │   ├── state.py              # 파이프라인 상태 관리
│   │   │   ├── pipeline.py           # 메인 파이프라인 실행
│   │   │   └── nodes/                # LangGraph 노드들
│   │   │       ├── init_node.py      # 초기화 (카테고리 레이블)
│   │   │       ├── parent_score_node.py    # 상위 카테고리 LLM 점수
│   │   │       ├── child_score_node.py     # 하위 카테고리 LLM 점수
│   │   │       ├── db_filter_node.py       # DB 상품 필터링
│   │   │       └── product_node.py         # 최종 상품 선택
│   │   ├── preprocess/               # 전처리 모듈
│   │   │   ├── main_processor.py     # 메인 전처리 스크립트
│   │   │   ├── csv_processor.py      # CSV 파일 처리
│   │   │   ├── text_processor.py     # 텍스트 파일 처리
│   │   │   └── utils/                # 전처리 유틸리티
│   │   ├── services/                 # 외부 서비스 연동
│   │   │   ├── llm/                  # LLM 관련 서비스
│   │   │   │   ├── client.py         # Upstage API 클라이언트
│   │   │   │   ├── prompts.py        # 프롬프트 템플릿
│   │   │   │   └── scorer.py         # LLM 점수 계산
│   │   │   └── repo/                 # 데이터 저장소
│   │   │       └── product_repo.py   # 상품 데이터 접근
│   │   ├── utils/                    # 유틸리티 함수들
│   │   │   ├── cache.py              # 캐싱 시스템
│   │   │   ├── softmax.py            # Softmax + 자동 온도 튜닝
│   │   │   └── text.py               # 텍스트 정규화
│   │   ├── run_preprocess_pipeline.py # 통합 실행 스크립트
│   │   └── requirements.txt          # 의존성 목록
│   ├── data_pipeline/                # 데이터 수집/전처리
│   │   ├── crawler/                  # 카카오기프트 크롤러
│   │   └── preprocessor/             # 상품 데이터 정규화
│   └── infra/                        # 인프라 설정
├── frontend_test/                    # 프론트엔드 테스트
└── README.md                         # 프로젝트 문서
```

## 🔧 **LangGraph 파이프라인 구조**

```mermaid
graph TD
    A[전처리] --> B[init_node]
    B --> C[parent_score_node]
    C --> D[child_score_node]
    D --> E[db_filter_node]
    E --> F[product_node]
    F --> G[최종 결과]
    
    B -.-> B1[LLM: 상위 카테고리 점수]
    C -.-> C1[LLM: 하위 카테고리 점수]
    E -.-> E1[PostgreSQL 상품 조회]
    F -.-> F1[LLM 스마트 선택]
    F -.-> F2[룰 기반 폴백]
```

### **노드별 상세 기능**

- **`전처리`**: 카카오톡 대화 → 정제된 CSV
- **`init_node`**: 카테고리 레이블 초기화
- **`parent_score_node`**: 상위 카테고리별 연관성/구매의도 점수 (LLM)
- **`child_score_node`**: 하위 카테고리별 점수 (LLM)
- **`db_filter_node`**: 예산 범위 내 상품 필터링 및 조회
- **`product_node`**: LLM 기반 스마트 선택 → 실패 시 룰 기반 폴백

## ⚙️ **env 설정**

```bash
# LLM 설정
UPSTAGE_API_KEY="your_api_key_here"
UPSTAGE_BASE_URL="https://api.upstage.ai/v1"
UPSTAGE_CHAT_MODEL="solar-1-mini-2024-08-28"

# 데이터베이스 설정
export DB_URL="postgresql://username:password@localhost:5432/database"

# 성능 설정
MAX_CONCURRENCY=8
CACHE_TTL_SECS=3600
TIMEOUT_SECS=30

# 하이퍼파라미터
ENTROPY_TARGET_PARENT=0.7
ENTROPY_TARGET_CHILD=0.8
ALPHA0=0.3
BETA=0.5
GAMMA=1.0
```

## 🗃️ **데이터베이스 연동**

### **PostgreSQL 테이블 스키마**

```sql
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    top_category VARCHAR(100),
    sub_category VARCHAR(100),
    brand VARCHAR(100),
    product_name VARCHAR(500),
    price INTEGER,
    satisfaction_pct FLOAT,
    review_count INTEGER,
    wish_count INTEGER,
    tags TEXT,
    product_url TEXT,
    updated_at TIMESTAMP
);

CREATE INDEX idx_sub_category ON products(sub_category);
CREATE INDEX idx_price ON products(price);
```

### **데이터 샘플**

```sql (Postgre-sql)
INSERT INTO products (top_category, sub_category, brand, product_name, price, satisfaction_pct, review_count, wish_count, product_url) VALUES
('식품', '과일/견과/채소', '대디스팜', '프리미엄 골드망고 2.8kg', 47800, 98.0, 1130, 11050, 'https://gift.kakao.com/product/9324040'),
('식품', '축산/수산', '미트팩토리', '프라임냉장 핑크 스테이크 600g', 39800, 99.0, 1430, 20270, 'https://gift.kakao.com/product/7201451');
```

## 📋 **입력 데이터 형식**

### **카카오톡 대화 CSV** :

```csv
Date,User,Message
2025-07-19 10:53:07,구남혁,넌 언제부터 가능해?
2025-07-19 10:53:11,구남혁,다른 사람다 자는거 같은디...
2025-07-19 11:15:49,박채연,난 사실 준비하고 바로 나갈 수 있긴...
```

### **사용자 프로필** (명령행 인자):

```bash
--user            # 대상 사용자
--age             # 나이
--gender          # 성별 (M/F)
--relation        # 관계
--budget-min      # 최소 예산
--budget-max      # 최대 예산
```

## 🎯 **출력 형식**

### **최종 추천 상품**

```
🎁 추천 상품 (5개):
================================================================================

1. 미트팩토리 프라임냉장 핑크 스테이크 600g
   💰 가격: 39,800원
   🏷️ 브랜드: 미트팩토리
   🔗 상품 URL: https://gift.kakao.com/product/7201451
   💡 추천 근거: 축산/수산 카테고리 신호(0.505) + 프로필 적합성
   ⭐ 만족도: 99.0%
   📝 리뷰: 1430개
   ❤️ 찜: 20,270개
   🏷️ 카테고리: 축산/수산
```

### **시스템 정보**

```
📊 시스템 정보:
   🎯 추천 카테고리: 축산/수산, 과일/견과/채소, 쌀/반찬/김치
   📦 후보 상품 수: 299개
   🤖 선택 방식: LLM 스마트 선택 (또는 룰 기반 폴백)
```

## 🚀 **주요 기능**

### **1. 전처리 최적화**
- **노이즈 제거**: 송금, 사진, 이모티콘 등 자동 필터링
- **텍스트 정제**: 반복 문자 축약, 의미없는 문자 삭제
- **문장 병합**: SBD(Sentence Boundary Detection) 기반 문장 병합
- **익명화**: 숫자 마스킹으로 개인정보 보호

### **2. LLM 기반 분석**
- **상위 카테고리 분석**: 대화 내용의 상위 카테고리 점수 계산
- **하위 카테고리 분석**: 구체적인 선물 카테고리 점수 계산

### **3. 상품 선택**
- **1차: LLM 스마트 선택**: 맥락과 근거를 고려한 지능적 선택
- **2차: 룰 기반 폴백**: LLM 실패 시 안전한 폴백 로직
- **다양성 보장**: 카테고리별 다양성과 브랜드 중복 제거
- **프로필 적합성**: 나이, 성별, 관계 기반 맞춤형 추천


## 🐛 **문제 해결**

### **일반적인 오류**

#### **1. Import 오류**
```bash
# 패키지 재설치
pip install -r requirements.txt


#### **2. DB 연결 오류**
```bash
# PostgreSQL 연결 테스트
psql $DB_URL

# 환경변수 확인
echo $DB_URL
```

#### **3. API 키 오류**
```bash
# 환경변수 확인
echo $UPSTAGE_API_KEY
echo $UPSTAGE_BASE_URL
```


### **로그 확인**

```bash
# 상세 로그 확인
python run_preprocess_pipeline.py --input chatt.csv --user "" --age 23 --gender M --relation "친구" --budget-min 30000 --budget-max 50000 2>&1 | tee log.txt
```

## 🔬 **개발 및 테스트**


### **단위 테스트**
```bash
# 전체 파이프라인 import 테스트
python -c "from core.pipeline import run_pipeline; print('Success')"

# 개별 노드 테스트
python -c "from core.nodes import *; print('All nodes imported successfully')"
```



