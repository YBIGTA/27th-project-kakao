
# 카카오톡 대화내역 기반 개인 맞춤형 선물 추천 서비스

LangGraph를 기반으로 한 카카오톡 대화내역 분석을 통한 개인 맞춤형 선물 추천 시스템입니다.

## 🚀 주요 기능

- **대화내역 분석**: 카카오톡 대화를 LLM으로 분석하여 선물 선호도 파악
- **계층적 카테고리 분류**: 상위/하위 카테고리별 점수 계산 및 결합
- **맞춤형 추천**: 상대방 프로필과 예산을 고려한 최적 상품 선택
- **LangGraph 파이프라인**: 확장 가능하고 유지보수가 용이한 워크플로우

## 🏗️ 아키텍처

```
app/
├── core/                    # 핵심 비즈니스 로직
│   ├── config.py           # 환경변수, 하이퍼파라미터
│   ├── state.py            # 파이프라인 상태 관리
│   ├── pipeline.py         # 메인 파이프라인 실행
│   └── nodes/              # LangGraph 노드들
│       ├── init_node.py    # 초기화 (카테고리 레이블)
│       ├── parent_score_node.py    # 상위 카테고리 LLM 점수
│       ├── child_score_node.py     # 하위 카테고리 LLM 점수
│       ├── combine_node.py         # 계층 결합
│       ├── db_filter_node.py       # DB 상품 필터링
│       └── product_node.py         # 최종 상품 선택
├── services/               # 외부 서비스 연동
│   ├── llm/               # LLM 관련 서비스
│   │   ├── client.py      # Upstage API 클라이언트
│   │   ├── prompts.py     # 프롬프트 템플릿
│   │   └── scorer.py      # LLM 점수 계산
│   └── repo/              # 데이터 저장소
│       └── product_repo.py # 상품 데이터 접근
├── utils/                  # 유틸리티 함수들

│   ├── softmax.py         # Softmax + 자동 온도 튜닝
│   └── text.py            # 텍스트 정규화
└── main.py                # 메인 실행 파일
```

## 🔧 설치 및 설정

### 1. 의존성 설치

```bash
pip install -r requirements.txt
```

### 2. 환경 변수 설정

```bash
cp env.example .env
# .env 파일을 편집하여 실제 값 입력
```

필수 환경 변수:
- `UPSTAGE_API_KEY`: Upstage API 키
- `UPSTAGE_BASE_URL`: Upstage API 기본 URL
- `UPSTAGE_MODEL`: 사용할 모델명 (기본값: solar-pro-2)

### 3. 데이터 준비

- **대화내역 CSV**: `chatt-1.csv` 형식 (text 컬럼 포함)
- **상품 데이터**: `kakao_gifts.normalized.csv` 형식

## 📖 사용법

### 기본 사용법

```bash
python app/main.py \
  --chat_csv chatt-1.csv \
  --age 25 \
  --gender 여성 \
  --relation 친구 \
  --budget_min 10000 \
  --budget_max 50000 \
  --products_csv_path kakao_gifts.normalized.csv
```

### 매개변수 설명

- `--chat_csv`: 전처리된 대화 CSV 파일 경로
- `--age`: 선물 받는 상대방의 나이 (1-120)
- `--gender`: 성별 (남성/여성/기타)
- `--relation`: 관계 (친구/연인/가족/동료 등)
- `--budget_min`: 최소 예산 (원)
- `--budget_max`: 최대 예산 (원)
- `--products_csv_path`: 상품 데이터베이스 CSV 파일 경로

## 🔄 파이프라인 흐름

1. **초기화**: 카테고리 분류 체계 구축
2. **상위 카테고리 점수**: LLM을 사용한 상위 카테고리별 점수 계산
3. **하위 카테고리 점수**: LLM을 사용한 하위 카테고리별 점수 계산
4. **계층 결합**: 상위/하위 카테고리 점수 결합 및 Top-3 선정
5. **상품 필터링**: 예산 범위 내 후보 상품 조회
6. **최종 선택**: LLM을 사용한 최종 상품 5개 선택

## 🎯 카테고리 분류

### 상위 카테고리 (13개)
- 교환권, 상품권, 뷰티, 패션, 식품, 와인/양주/전통주
- 리빙/도서, 레저/스포츠, 골프선물, 아티스트/캐릭터
- 유아동/반려, 디지털/가전, 카카오프렌즈

### 하위 카테고리 (100+개)
각 상위 카테고리별로 세분화된 하위 카테고리 제공

## ⚙️ 설정 옵션

### 점수 가중치
- `BETA_CHILD`: 하위 카테고리 점수 가중치 (기본값: 0.6)
- `GAMMA_PARENT`: 상위 카테고리 점수 가중치 (기본값: 0.4)
- `SINGLE_CHILD_PENALTY_LAMBDA`: 단일 자식 패널티 (기본값: 0.1)

### Softmax 설정
- `SOFTMAX_TEMPERATURE`: 온도 파라미터 (기본값: 0.9)
- `SCORE_CLAMP_MIN/MAX`: 점수 범위 제한

## 🚨 주의사항

1. **API 키 보안**: `.env` 파일을 `.gitignore`에 추가하여 API 키 노출 방지
2. **데이터 형식**: CSV 파일의 컬럼명이 예상 형식과 일치해야 함
3. **메모리 사용량**: 대용량 데이터 처리 시 메모리 사용량 모니터링 필요
4. **API 제한**: Upstage API 사용량 및 속도 제한 확인

## 🐛 문제 해결

### 일반적인 오류

1. **API 키 오류**: `UPSTAGE_API_KEY` 환경 변수 확인
2. **파일 없음**: CSV 파일 경로 및 존재 여부 확인
3. **메모리 부족**: 대용량 데이터 처리 시 배치 크기 조정

### 로그 확인

```bash
# 로그 레벨을 DEBUG로 설정하여 상세 정보 확인
export PYTHONPATH=.
python -c "import logging; logging.basicConfig(level=logging.DEBUG)"
```

## 📝 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다.

## 🤝 기여

버그 리포트, 기능 제안, 풀 리퀘스트를 환영합니다.
