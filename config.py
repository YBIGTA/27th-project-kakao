# -*- coding: utf-8 -*-
import os
from dotenv import load_dotenv

# .env 파일 자동 로드
load_dotenv()

# API 설정
API_KEYS = {
    "groq": os.getenv("GROQ_API_KEY", ""),
    "openai": os.getenv("OPENAI_API_KEY", "")
}

# 모델 설정
GROQ_MODEL = "llama3-8b-8192"
OPENAI_MODEL = "gpt-4o-mini"

# 라벨링 설정 (6개 → 5개로 조정)
LABELS = ["필요", "구매", "관심", "부정", "단순 언급"]
CONFIDENCE_THRESHOLD = 0.8  # 0.7 → 0.8로 상향 조정

# 경로 설정
DATASET_DIR = "dataset1"
RESULTS_DIR = "results"
FEWSHOT_SOURCE = "labelling_sample.csv"
FEWSHOT_PER_LABEL = 8  # 2 → 8로 증가 (정확도 향상)

# API 호출 설정
GROQ_TIMEOUT = 60
OPENAI_TIMEOUT = 60
RATE_LIMIT_DELAY = 30.0  # 초 (30초마다 요청으로 변경 - Rate Limit 방지)
