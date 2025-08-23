# 카카오톡 대화 기반 맞춤형 선물 추천 시스템

## 📋 프로젝트 개요

카카오톡 대화 내용을 분석하여 사용자에게 최적화된 선물을 추천하는 AI 기반 시스템입니다. LLM(Upstage Solar)을 활용한 자연어 처리와 계층적 카테고리 분석을 통해 정확한 선물 추천을 제공합니다.

## 🏗️ 시스템 아키텍처

### 전체 구조
```
27th-project-kakao/
├── backend/                 # 백엔드 API 서버
│   ├── app/
│   │   ├── core/           # 핵심 로직
│   │   │   ├── nodes/      # 파이프라인 노드들
│   │   │   ├── llm/        # LLM 관련 모듈
│   │   │   ├── database/   # 데이터베이스 유틸리티
│   │   │   ├── utils/      # 공통 유틸리티
│   │   │   └── state.py    # 상태 관리
│   │   ├── config/         # 설정 관리
│   │   ├── preprocess/     # 전처리 모듈
│   │   ├── main.py         # FastAPI 서버
│   │   └── pipeline.py     # 메인 파이프라인
│   └── requirements.txt
├── data_pipeline/          # 데이터 수집 및 전처리
├── frontend_test/          # 프론트엔드 (React)
└── infra/                  # 인프라 설정
```

### 파이프라인 흐름
1. **전처리** → 카카오톡 대화 파일 정제
2. **상위 카테고리 분석** → 13개 대분류 점수화
3. **하위 카테고리 분석** → 세부 카테고리 점수화
4. **계층 결합** → 상위/하위 점수 통합
5. **Top-3 선택** → 최적 카테고리 선정
6. **DB 필터링** → 예산/카테고리 기반 상품 검색
7. **상품 선택** → LLM 기반 최종 상품 선정
8. **패키징** → 결과 포맷팅

## 🚀 주요 기능

### 🤖 AI 기반 분석
- **LLM 활용**: Upstage Solar 모델로 자연어 이해
- **병렬 처리**: 5개 API 키로 고속 처리
- **하이브리드 처리**: 개별/배치 처리 자동 선택
- **캐싱**: 중복 요청 최적화

### 📊 계층적 카테고리 분석
- **상위 카테고리**: 13개 대분류 (교환권, 뷰티, 패션 등)
- **하위 카테고리**: 100+ 세부 카테고리
- **신호 분석**: 연관성, 구매의도, 감정 극성 등

### 🎯 맞춤형 추천
- **사용자 프로필**: 나이, 성별, 관계 반영
- **예산 고려**: 설정된 예산 범위 내 상품
- **다양성 보장**: 브랜드/카테고리 중복 방지

## 🛠️ 기술 스택

### Backend
- **Framework**: FastAPI
- **LLM**: Upstage Solar Pro
- **Database**: PostgreSQL (AWS RDS)
- **Async**: asyncio, aiohttp
- **Data Processing**: pandas, numpy

### Frontend
- **Framework**: React + TypeScript

### Infrastructure
- **Cloud**: AWS RDS

## 📦 설치 및 실행

### 1. 환경 설정
```bash
# 저장소 클론
git clone [repository-url]
cd 27th-project-kakao-5

# 백엔드 디렉토리로 이동
cd backend

# 환경변수 설정
cp backend/env.example .env
# .env 파일을 편집하여 실제 값 입력
```

### 2. 의존성 설치
```bash
# Python 가상환경 생성
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 패키지 설치
pip install -r requirements.txt
```

### 3. 환경변수 설정
```bash
# .env 파일에 다음 값들을 설정
UPSTAGE_API_KEY=your_api_key
UPSTAGE_API_KEY_1=your_api_key_1
UPSTAGE_API_KEY_2=your_api_key_2
UPSTAGE_API_KEY_3=your_api_key_3
UPSTAGE_API_KEY_4=your_api_key_4
UPSTAGE_API_KEY_5=your_api_key_5
DB_URL=your_postgresql_url
```

### 4. 서버 실행
```bash
# 개발 서버 실행
python app/main.py

# 또는 uvicorn으로 실행
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
```

## 📡 API 사용법

### 선물 추천 API
```http
POST /recommendations
Content-Type: multipart/form-data

Parameters:
- file: 카카오톡 대화 파일 (.txt, .csv)
- age: 사용자 나이 (int)
- gender: 성별 ("M" | "F")
- relation: 관계 ("연인", "친구", "가족" 등)
- budget_min: 최소 예산 (int)
- budget_max: 최대 예산 (int)
```

### 응답 예시
```json
{
  "success": true,
  "message": "선물 추천이 완료되었습니다.",
  "data": {
    "user_context": {
      "age": 25,
      "gender": "F",
      "relation": "연인",
      "budget_min": 50000,
      "budget_max": 100000
    },
    "analysis": {
      "parent_categories": {
        "scores": {"뷰티": 0.8, "패션": 0.6},
        "evidence": {"뷰티": [0, 2], "패션": [1]},
        "reasoning": {"뷰티": ["화장품 언급"], "패션": ["옷 스타일 언급"]}
      },
      "child_categories": {
        "scores": {"향수": 0.9, "주얼리": 0.7},
        "evidence": {"향수": [0], "주얼리": [2]},
        "reasoning": {"향수": ["향수 선물 원함"], "주얼리": ["반지 언급"]}
      },
      "top3_selection": ["향수", "주얼리", "스킨케어"]
    },
    "products": {
      "candidates_count": 150,
      "selected_count": 5,
      "selected_products": [
        {
          "id": "product_1",
          "title": "샤넬 샹스 오 뗑뗑",
          "brand": "샤넬",
          "price": 85000,
          "category_child": "향수",
          "url": "https://...",
          "rationale": "대화에서 향수 선물을 원한다고 언급"
        }
      ],
      "rationales": {
        "product_1": "대화에서 향수 선물을 원한다고 언급"
      }
    }
  }
}
```

## 🔧 개발 가이드

### 파이프라인 노드 추가
```python
# backend/app/core/nodes/new_node.py
async def new_node(state: GraphState) -> GraphState:
    """새로운 노드 구현"""
    # 노드 로직 구현
    return state

# backend/app/core/nodes/__init__.py에 추가
from .new_node import new_node
__all__ = [..., 'new_node']
```

### LLM 프롬프트 수정
```python
# backend/app/core/llm/prompts.py에서 프롬프트 수정
NEW_PROMPT = """새로운 프롬프트 내용"""
```

### 환경변수 추가
```python
# backend/app/config/settings.py
NEW_SETTING = _env("NEW_SETTING", "default_value")

# backend/env.example
NEW_SETTING=your_value_here
```

## 📈 성능 최적화

### 병렬 처리
- **5개 API 키**: 라운드 로빈 방식으로 병렬 처리
- **하이브리드 처리**: 문장 수에 따라 개별/배치 처리 자동 선택
- **캐싱**: 중복 LLM 요청 방지

### 메모리 최적화
- **스트리밍 처리**: 대용량 파일 처리 시 메모리 효율성
- **상태 관리**: GraphState로 파이프라인 상태 통합 관리




