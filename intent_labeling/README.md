# 라벨링 파이프라인

한국어 일상 대화 문장을 6개 라벨로 분류하는 자동화된 라벨링 파이프라인입니다.

## 라벨 정의

- **필요**: 교체/보충 의도 (예: "노트북이 버벅거려")
- **구매**: 구매 의사 표현 (예: "맨투맨 사고 싶네")
- **관심**: 흥미 표현 (예: "에어팟 봤어?")
- **고민**: 선택/결정 망설임 (예: "어떤 신발 살까")
- **부정**: 특정 상품이나 카테고리에 대한 부정적 의도 (예: "나는 립스틱은 별로야")
- **단순 언급**: 선물과 무관한 단순 정보 공유 (예: "오늘 날씨 예쁘다")

## 아키텍처

**"Groq(속도·비용) + OpenAI(정확도) 혼합 전략"**

1. **Groq 초벌 라벨링**: 빠르고 무료로 대량 처리
2. **OpenAI 재라벨링**: 신뢰도가 낮은 샘플만 정밀하게 재평가
3. **Few-shot 학습**: 각 라벨별 2개 예시로 정확도 향상

## 설치 및 설정

### 1. 의존성 설치
```bash
pip install -r requirements.txt
```

### 2. 환경변수 설정
```bash
# .env 파일 생성 또는 환경변수 설정
export GROQ_API_KEY="your_groq_api_key_here"
export OPENAI_API_KEY="your_openai_api_key_here"
```

**⚠️ 보안 주의사항**: 
- `.env` 파일은 절대 Git에 커밋하지 마세요!
- API 키는 절대 코드에 하드코딩하지 마세요!
- 이 파일들은 `.gitignore`에 포함되어 있습니다.

### 3. API 키 발급
- [Groq API](https://console.groq.com/)에서 API 키 발급
- [OpenAI API](https://platform.openai.com/)에서 API 키 발급

## 사용법

### 기본 실행
```bash
python run_pipeline.py
```

### 설정 변경
`config.py` 파일에서 다음 설정을 조정할 수 있습니다:
- `CONFIDENCE_THRESHOLD`: OpenAI 재라벨링 임계값 (기본값: 0.7)
- `FEWSHOT_PER_LABEL`: 각 라벨별 예시 개수 (기본값: 2)
- `RATE_LIMIT_DELAY`: API 호출 간격 (기본값: 0.2초)

## 파일 구조

```
labeling_pipeline_final/
├── config.py              # 설정 파일
├── run_pipeline.py        # 메인 실행 파일
├── labeling_agent.py      # 라벨링 에이전트
├── groq_labeler.py        # Groq API 호출
├── openai_refiner.py      # OpenAI API 호출
├── utils.py               # 유틸리티 함수
├── labelling_sample.csv   # Few-shot 예시 데이터
├── dataset1/              # 입력 데이터셋
├── results/               # 결과 출력 폴더
└── requirements.txt       # 의존성 패키지
```

## 출력 형식

각 샘플은 다음과 같은 JSONL 형식으로 저장됩니다:

```json
{
  "sample_id": "파일명#인덱스",
  "file": "원본파일명",
  "index": 0,
  "text": "원본 텍스트",
  "label": "분류된 라벨",
  "confidence": 0.85,
  "source": "groq 또는 openai_refined",
  "groq_result": "Groq API 응답 전체"
}
```

## 성능 최적화

- **병렬 처리**: 여러 파일을 동시에 처리할 수 있도록 설계
- **중단 재개**: 이미 처리된 샘플은 건너뛰고 재시작 가능
- **에러 처리**: API 호출 실패 시 자동으로 fallback 처리
- **진행상황 추적**: 실시간 진행상황 모니터링

## 문제 해결

### API 키 오류
```bash
GROQ_API_KEY 환경변수가 설정되어 있지 않습니다.
```
→ 환경변수를 올바르게 설정했는지 확인

### 메모리 부족
→ `config.py`에서 `RATE_LIMIT_DELAY`를 늘려서 API 호출 속도 조절

### 결과 폴더 생성 실패
→ `results/` 폴더가 자동으로 생성되며, 권한 문제가 있다면 수동으로 생성

## 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다.
