import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

def _env(name: str, default: str = ""):
    return os.getenv(name, default)

# LLM 설정
LLM_PROVIDER = _env("LLM_PROVIDER", "UPSTAGE")
UPSTAGE_API_KEY = _env("UPSTAGE_API_KEY", "")
UPSTAGE_BASE_URL = _env("UPSTAGE_BASE_URL", "")
UPSTAGE_CHAT_MODEL = _env("UPSTAGE_CHAT_MODEL", "solar-1-mini-chat")

# 병렬 처리를 위한 API 키들
UPSTAGE_API_KEYS = []
for i in range(1, 6):
    key = _env(f"UPSTAGE_API_KEY_{i}", "")
    if key:
        UPSTAGE_API_KEYS.append(key)

# 기본 API 키도 추가 (중복 제거)
if UPSTAGE_API_KEY and UPSTAGE_API_KEY not in UPSTAGE_API_KEYS:
    UPSTAGE_API_KEYS.append(UPSTAGE_API_KEY)

# 성능 설정
MAX_CONCURRENCY = int(_env("MAX_CONCURRENCY", "20"))
TIMEOUT_SECS    = int(_env("TIMEOUT_SECS", "60"))

# 하이퍼파라미터
ENTROPY_TARGET_PARENT = float(_env("ENTROPY_TARGET_PARENT", "0.70"))
ENTROPY_TARGET_CHILD  = float(_env("ENTROPY_TARGET_CHILD",  "0.80"))
BETA   = float(_env("BETA",   "1.0"))
GAMMA  = float(_env("GAMMA",  "1.0"))

# 병렬 처리 설정
INDIVIDUAL_PARALLEL_THRESHOLD = int(_env("INDIVIDUAL_PARALLEL_THRESHOLD", "5"))  # 개별 병렬 처리 임계값


# 인기도 점수 가중치
POPULARITY_WEIGHTS = {
    "satisfaction": float(_env("POP_SATISFACTION_WEIGHT", "0.5")),
    "review_count": float(_env("POP_REVIEW_WEIGHT", "0.3")),
    "wish_count": float(_env("POP_WISH_WEIGHT", "0.2"))
}

# 정규화 기준값
NORMALIZATION_THRESHOLDS = {
    "review_count": int(_env("REVIEW_COUNT_THRESHOLD", "1000")),
    "wish_count": int(_env("WISH_COUNT_THRESHOLD", "10000"))
}

# 신뢰도 가중치
CONFIDENCE_WEIGHTS = {
    "확실": float(_env("CONFIDENCE_HIGH", "1.0")),
    "보통": float(_env("CONFIDENCE_MEDIUM", "0.6")),
    "약함": float(_env("CONFIDENCE_LOW", "0.3"))
}

# 상품 선택 가중치
PRODUCT_SELECTION_WEIGHTS = {
    "category": float(_env("CATEGORY_WEIGHT", "0.7")),
    "popularity": float(_env("POPULARITY_WEIGHT", "0.3"))
}

# 싱글-차일드 가드
SINGLE_CHILD_PENALTY = float(_env("SINGLE_CHILD_PENALTY", "0.3"))

# 캐시 설정
CACHE_BACKEND = _env("CACHE_BACKEND", "none")  # redis | memory | none
REDIS_URL     = _env("REDIS_URL", "")
CACHE_TTL_SECS = int(_env("CACHE_TTL_SECS", "1209600"))  # 14 days

# 데이터베이스 설정
DB_URL = _env("DB_URL", "")

# 기타 설정
NEAR_DUP_THRESH = float(_env("NEAR_DUP_THRESH", "0.8"))
LOG_LEVEL = _env("LOG_LEVEL", "INFO")
PORT = int(_env("PORT", "8000"))

# 카테고리 정의
PARENT_LABELS = [
    "교환권","상품권","뷰티","패션","식품","와인/양주/전통주",
    "리빙/도서","레저/스포츠","골프선물","아티스트/캐릭터",
    "유아동/반려","디지털/가전","카카오프렌즈"
]

CHILDREN_OF = {
    "교환권": ["베이커리/도넛/떡","카페","아이스크림/빙수","치킨","버거/피자","편의점","한식/중식/일식","패밀리/호텔뷔페","퓨전/외국/펍","분식/죽/도시락"],
    "상품권": ["상품권/마트","뷰티/패션/건강","영화/OTT/게임","헤어/네일/스파","전시/테마/체험","생활/교육/취미","종교/나눔"],
    "뷰티": ["명품화장품","향수","바디","스킨케어","메이크업","헤어/미용","남성화장품"],
    "패션": ["명품브랜드","주얼리","파자마","브랜드 가방/지갑","브랜드 의류","브랜드 신발","언더웨어","디자이너 브랜드","브랜드 잡화","브랜드 시계","주문각인"],
    "식품": ["과일/견과/채소","축산/수산","쌀/반찬/김치","건강식품","다이어트/이너뷰티","가공/보양식","케이크","디저트","유제품/아이스크림","커피/차/음료"],
    "와인/양주/전통주": ["와인","양주","전통주","맥주/기타"],
    "리빙/도서": ["주방/수입주방","캔들디퓨저 인센스","식물/꽃배달","침대/패브릭","조명/무드등","인테리어","생필품","수납/생활","가구/DIY","팬시/캐릭터","문구/취미","도서","명품리빙","리빙편집샵"],
    "레저/스포츠": ["글로벌 브랜드본사","스포츠 의류","스포츠 슈즈","스포츠 잡화","요가/헬스/수영","레저/캠핑","등산/아웃도어","차량용품","여행용품","차량용 방향제"],
    "골프선물": ["골프/테니스"],
    "아티스트/캐릭터": ["스타앨범","애니메이션 캐릭터","인디작가","애니멀캐릭터","웹소설","게임"],
    "유아동/반려": ["신생아선물세트","베이비패션","키즈패션","임신/출산/육아","장난감/인형","유아교육/도서","기저귀/물티슈","분유/간식/영양제","강아지 간식/용품","고양이 간식/용품","기타 소동물용품"],
    "디지털/가전": ["프리미엄 가전","케이스","모바일 액세서리","미니가전","건강용품/가전","디지털/음향기기","생활가전","주방가전","미용가전","카메라"],
    "카카오프렌즈": ["토이","리빙","테크","문구","패션","푸드","골프"],
}

# 하위 카테고리의 상위 카테고리 매핑
PARENT_OF = {}
for p, kids in CHILDREN_OF.items():
    for s in kids:
        PARENT_OF[s] = p

