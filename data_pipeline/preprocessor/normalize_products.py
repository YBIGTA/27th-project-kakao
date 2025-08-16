import re, ast, hashlib, collections
from pathlib import Path
import pandas as pd
import numpy as np

# ── 경로 고정(어디서 실행해도 동작) ─────────────────────
HERE = Path(__file__).resolve().parent          # .../data_pipeline/preprocessor
ROOT = HERE.parent                              # .../data_pipeline
RAW_DIR = ROOT / "raw_data"                     # 원본 CSV 위치
OUT_CSV = ROOT / "kakao_gifts.normalized.csv"   # 결과 CSV
UNKNOWN_CSV = HERE / "_unknown_subcategories.csv"

# ── 허용 상위 카테고리 (엑셀 표 기준) ────────────────
TOP_ALLOWED = {
    "교환권","상품권","뷰티","패션","식품","와인/양주/전통주",
    "리빙/도서","레저/스포츠","아티스트/캐릭터","유아동/반려",
    "디지털/가전","카카오프렌즈","기타"
}

# ── 하위→상위 매핑 (엑셀 표를 코드에 내장) ─────────────
# 표에서 보인 항목들을 최대한 반영. 부족한 건 아래 KEYWORD_RULES 또는 기타 처리.
SUB_TO_TOP_MAP = {
    # 교환권
    "베이커리/도넛/떡": "교환권",
    "카페": "교환권",
    "아이스크림/빙수": "교환권",
    "치킨": "교환권",
    "버거/피자": "교환권",
    "편의점": "교환권",
    "한식/중식/일식": "교환권",
    "패밀리/호텔뷔페": "교환권",
    "퓨전/외국/펍": "교환권",
    "분식/죽/도시락": "교환권",

    # 상품권
    "상품권/마트": "상품권",
    "뷰티/패션/건강": "상품권",
    "영화/OTT/게임": "상품권",
    "헤어/네일/스파": "상품권",
    "전시/테마/체험": "상품권",
    "생활/교육/취미": "상품권",
    "종교/나눔": "상품권",

    # 뷰티
    "명품화장품": "뷰티",
    "향수": "뷰티",
    "바디": "뷰티",
    "스킨케어": "뷰티",
    "메이크업": "뷰티",
    "헤어/미용": "뷰티",
    "남성화장품": "뷰티",

    # 패션
    "명품브랜드": "패션",
    "주얼리": "패션",
    "파자마": "패션",
    "브랜드 가방/지갑": "패션",
    "브랜드 의류": "패션",
    "브랜드의류": "패션",
    "브랜드신발": "패션",
    "브랜드 신발": "패션",
    "언더웨어": "패션",
    "디자이너 브랜드": "패션",
    "브랜드 잡화": "패션",
    "브랜드잡화": "패션",
    "브랜드시계": "패션",
    "브랜드 시계": "패션",
    "주문각인": "패션",
    "트렌드패션": "패션",

    # 식품
    "과일/견과/채소": "식품",
    "축산/수산": "식품",
    "쌀/반찬/김치": "식품",
    "건강식품": "식품",
    "다이어트/이너뷰티": "식품",
    "가공/보양식": "식품",
    "케이크": "식품",
    "디저트": "식품",
    "유제품/아이스크림": "식품",
    "커피/차/음료": "식품",

    # 와인/양주/전통주
    "와인": "와인/양주/전통주",
    "양주": "와인/양주/전통주",
    "전통주": "와인/양주/전통주",
    "맥주/기타": "와인/양주/전통주",

    # 리빙/도서
    "주방/수입주방": "리빙/도서",
    "캔들디퓨저 인센스": "리빙/도서",
    "식물/꽃배달": "리빙/도서",
    "침대/패브릭": "리빙/도서",
    "침구/패브릭": "리빙/도서",
    "조명/무드등": "리빙/도서",
    "인테리어": "리빙/도서",
    "생필품": "리빙/도서",
    "수납/생활": "리빙/도서",
    "가구/DIY": "리빙/도서",
    "팬시/캐릭터": "리빙/도서",
    "문구/취미": "리빙/도서",
    "도서": "리빙/도서",
    "리빙편집샵": "리빙/도서",
    "명품리빙": "리빙/도서",

    # 레저/스포츠
    "글로벌 브랜드본사": "레저/스포츠",
    "스포츠 의류": "레저/스포츠",
    "스포츠의류": "레저/스포츠",
    "스포츠 슈즈": "레저/스포츠",
    "스포츠슈즈": "레저/스포츠",
    "스포츠 잡화": "레저/스포츠",
    "스포츠잡화": "레저/스포츠",
    "요가/헬스/수영": "레저/스포츠",
    "레저/캠핑": "레저/스포츠",
    "등산/아웃도어": "레저/스포츠",
    "차량용품": "레저/스포츠",
    "여행용품": "레저/스포츠",
    "차량용 방향제": "레저/스포츠",
    "골프선물": "레저/스포츠",
    "골프/테니스": "레저/스포츠",

    # 아티스트/캐릭터
    "스타앨범": "아티스트/캐릭터",
    "애니메이션 캐릭터": "아티스트/캐릭터",
    "인디작가": "아티스트/캐릭터",
    "애니멀캐릭터": "아티스트/캐릭터",
    "웹소설": "아티스트/캐릭터",
    "게임": "아티스트/캐릭터",

    # 유아동/반려
    "신생아선물세트": "유아동/반려",
    "베이비패션": "유아동/반려",
    "키즈패션": "유아동/반려",
    "임신/출산/육아": "유아동/반려",
    "장난감/인형": "유아동/반려",
    "유아교육/도서": "유아동/반려",
    "기저귀/물티슈": "유아동/반려",
    "분유/간식/영양제": "유아동/반려",
    "강아지 간식/용품": "유아동/반려",
    "고양이 간식/용품": "유아동/반려",
    "기타 소동물용품": "유아동/반려",

    # 디지털/가전
    "프리미엄 가전": "디지털/가전",
    "케이스": "디지털/가전",
    "모바일 액세서리": "디지털/가전",
    "미니가전": "디지털/가전",
    "건강용품/가전": "디지털/가전",
    "디지털/음향기기": "디지털/가전",
    "생활가전": "디지털/가전",
    "주방가전": "디지털/가전",
    "미용가전": "디지털/가전",
    "카메라": "디지털/가전",

    # 카카오프렌즈
    "토이": "카카오프렌즈",
    "리빙": "카카오프렌즈",
    "테크": "카카오프렌즈",
    "문구": "카카오프렌즈",
    "패션": "카카오프렌즈",
    "푸드": "카카오프렌즈",
    "골프": "카카오프렌즈",
}

# ── 키워드 보정 규칙 (미매핑 대비) ─────────────────────
KEYWORD_RULES = [
    # 와인/양주/전통주
    (r"(전통주|와인|위스키|양주|맥주|막걸리|소주|청주|막걸리|복분자|매실주|증류주|발효주)", "와인/양주/전통주"),
    
    # 식품
    (r"(케이크|디저트|쿠키|초콜릿|빵|베이커리|아이스크림|빙수|마카롱|티라미수|크로와상)", "식품"),
    (r"(커피|차|음료|티백|드립백|녹차|홍차|허브티|캐모마일|페퍼민트|루이보스)", "식품"),
    (r"(반찬|김치|젓갈|햄|소세지|밀키트|간편식|보양|가공식품|반찬세트|김치세트)", "식품"),
    (r"(건강식품|영양제|홍삼|비타민|이너뷰티|프로바이오틱스|오메가3|칼슘|철분제)", "식품"),
    (r"(과일|견과|채소|사과|바나나|오렌지|포도|딸기|블루베리|아몬드|호두|피스타치오)", "식품"),
    (r"(축산|수산|소고기|돼지고기|닭고기|생선|새우|게|조개|굴|홍합|전복)", "식품"),
    (r"(쌀|현미|흑미|잡곡|보리|퀴노아|아마란스|귀리|밀|옥수수)", "식품"),
    (r"(유제품|우유|요구르트|치즈|버터|크림|아이스크림|요거트|발효유)", "식품"),
    
    # 교환권
    (r"(치킨|피자|버거|편의점|뷔페|한식|중식|일식|분식|도시락|외국|펍|카페|베이커리|도넛|떡)", "교환권"),
    (r"(아이스크림|빙수|패밀리|호텔뷔페|퓨전|죽|편의점쿠폰|기프티콘)", "교환권"),
    
    # 상품권
    (r"(상품권|모바일쿠폰|기프티콘|OTT|영화|게임|전시|체험|종교|나눔|마트|편의점)", "상품권"),
    (r"(뷰티|패션|건강|헤어|네일|스파|생활|교육|취미|종교|나눔)", "상품권"),
    
    # 뷰티
    (r"(스킨케어|메이크업|립|쿠션|선크림|토너|앰플|클렌징|네일|미용|향수|바디|명품화장품)", "뷰티"),
    (r"(남성화장품|남성스킨케어|남성메이크업|남성향수|남성바디케어)", "뷰티"),
    
    # 패션
    (r"(의류|가방|지갑|신발|주얼리|팔찌|목걸이|귀걸이|파자마|속옷|언더웨어|모자|양말)", "패션"),
    (r"(명품브랜드|디자이너브랜드|브랜드시계|브랜드잡화|주문각인|커스텀각인)", "패션"),
    
    # 리빙/도서
    (r"(인테리어|조명|무드등|러그|패브릭|침구|수납|생활|청소|세제|캔들|디퓨저|인센스|화분|꽃|주방)", "리빙/도서"),
    (r"(문구|펜|노트|스티커|퍼즐|보드게임|프라모델|취미|도서|책|수입주방|가구|DIY)", "리빙/도서"),
    (r"(식물|꽃배달|침대|패브릭|생필품|수납|팬시|캐릭터)", "리빙/도서"),
    
    # 레저/스포츠
    (r"(캠핑|등산|아웃도어|헬스|요가|수영|러닝|스포츠|골프|테니스|자전거|휘트니스)", "레저/스포츠"),
    (r"(스포츠의류|스포츠슈즈|스포츠잡화|글로벌브랜드|차량용품|여행용품|차량용방향제|골프선물)", "레저/스포츠"),
    
    # 아티스트/캐릭터
    (r"(애니|캐릭터|스타|아이돌|앨범|포토카드|굿즈|웹소설|웹툰|스타앨범|인디작가)", "아티스트/캐릭터"),
    (r"(애니메이션캐릭터|애니멀캐릭터|게임|웹소설|웹툰)", "아티스트/캐릭터"),
    
    # 유아동/반려
    (r"(유아|신생아|출산|육아|키즈|장난감|인형|젖병|기저귀|베이비패션|키즈패션)", "유아동/반려"),
    (r"(강아지|고양이|펫|반려|사료|간식|장난감|하우스|리터|배변|소동물)", "유아동/반려"),
    (r"(신생아선물세트|임신|출산|육아|유아교육|도서|물티슈|분유|영양제)", "유아동/반려"),
    
    # 디지털/가전
    (r"(디지털|전자|가전|이어폰|헤드폰|스피커|키보드|마우스|카메라|충전기|보조배터리|케이스|액정보호)", "디지털/가전"),
    (r"(프리미엄가전|미니가전|건강용품|가전|음향기기|생활가전|주방가전|미용가전)", "디지털/가전"),
    (r"(모바일액세서리|테크|디지털기기|전자제품)", "디지털/가전"),
    
    # 카카오프렌즈
    (r"(카카오프렌즈|라이언|어피치|무지|콘|춘식|카카오토이|카카오리빙|카카오테크|카카오문구|카카오패션|카카오푸드|카카오골프)", "카카오프렌즈"),
]

# ── 클린업 유틸 ────────────────────────────────────────
PRICE_RE2 = re.compile(r"[^\d]")
BRAND_PREFIX_RE = re.compile(r"^\s*브랜드명\s*:?[\s]*", re.IGNORECASE)
QUOTE_RE = re.compile(r'(^[\'"]+|[\'"]+$)')

def to_int_safe(x):
    if pd.isna(x): return None
    s = PRICE_RE2.sub("", str(x))
    return int(s) if s.isdigit() else None

def clean_brand(x):
    if pd.isna(x): return ""
    return BRAND_PREFIX_RE.sub("", str(x)).strip()

def clean_name(x):
    if pd.isna(x): return ""
    return QUOTE_RE.sub("", str(x).strip())

def to_float2(x):
    if pd.isna(x): return None
    try:
        return float(str(x).replace("%","").strip())
    except Exception:
        return None

def to_int_or_none(x):
    if pd.isna(x): return None
    try:
        return int(float(x))
    except Exception:
        return None

def clean_tags(x):
    if pd.isna(x): return ""
    s = str(x).strip()
    if s.startswith("[") and s.endswith("]"):
        try:
            v = ast.literal_eval(s)
            if isinstance(v, (list, tuple)):
                return ",".join(str(i).strip() for i in v if str(i).strip())
        except Exception:
            pass
    return s

def sha256_hex(s):
    if not s or (isinstance(s, float) and np.isnan(s)): return None
    return hashlib.sha256(str(s).encode("utf-8")).hexdigest()

# ── 정규화 실행 ─────────────────────────────────────────
def main():
    csvs = sorted(RAW_DIR.glob("*.csv"))
    if not csvs:
        raise SystemExit(f"No CSVs found under {RAW_DIR}")

    frames = [pd.read_csv(p) for p in csvs]
    raw = pd.concat(frames, ignore_index=True)

    expected = [
        "top_category","sub_category","sub_tab","brand",
        "product_name","price","satisfaction_pct","review_count",
        "wish_count","tags","product_url"
    ]
    for col in expected:
        if col not in raw.columns:
            raw[col] = np.nan

    # 문자열 정리
    raw["sub_category"] = raw["sub_category"].astype(str).str.strip()
    raw["top_category"] = raw["top_category"].astype(str).str.strip()

    # 1) 하위→상위 매핑 (정확 일치)
    mapped = []
    unknown = []
    for sc, tc in zip(raw["sub_category"], raw["top_category"]):
        sc_norm = sc.strip()
        if sc_norm in SUB_TO_TOP_MAP:
            mapped.append(SUB_TO_TOP_MAP[sc_norm])
        else:
            unknown.append(sc_norm)
            mapped.append(tc)  # 일단 원래 top 유지
    raw["top_category"] = mapped

    # 2) 키워드 규칙 보정 (미매핑 케이스에 한해)
    def apply_keyword_rules(sub, top):
        if top in TOP_ALLOWED and top != "" and sub not in unknown:
            return top
        s = sub or ""
        for pat, tgt in KEYWORD_RULES:
            if re.search(pat, s):
                return tgt
        return top

    raw["top_category"] = [
        apply_keyword_rules(sc, tc) for sc, tc in zip(raw["sub_category"], raw["top_category"])
    ]

    # 3) 허용 집합으로 최종 보정
    raw.loc[~raw["top_category"].isin(TOP_ALLOWED), "top_category"] = "기타"

    # 표준 스키마 생성
    norm = pd.DataFrame()
    norm["product_url"]      = raw["product_url"].astype(str).str.strip().replace("nan","")
    norm["url_hash"]         = norm["product_url"].apply(sha256_hex)
    norm["top_category"]     = raw["top_category"]
    norm["sub_category"]     = raw["sub_category"]
    norm["brand"]            = raw["brand"].map(clean_brand)
    norm["product_name"]     = raw["product_name"].map(clean_name)
    norm["price"]            = raw["price"].map(to_int_safe)
    norm["satisfaction_pct"] = raw["satisfaction_pct"].map(to_float2)
    norm["review_count"]     = raw["review_count"].map(to_int_or_none)
    norm["wish_count"]       = raw["wish_count"].map(to_int_or_none)
    norm["tags"]             = raw["tags"].map(clean_tags)

    # 필수/중복 처리
    norm = norm[(norm["product_url"]!="") & (norm["product_name"]!="") & norm["price"].notna()]
    norm.drop_duplicates(subset=["url_hash"], inplace=True)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    norm.to_csv(OUT_CSV, index=False)

    # 미매핑/리뷰용 리포트 저장 (팀 보강용)
    cnts = collections.Counter([u for u in unknown if u and u not in SUB_TO_TOP_MAP])
    if cnts:
        pd.DataFrame({"sub_category": list(cnts.keys()), "count": list(cnts.values())}) \
          .sort_values("count", ascending=False) \
          .to_csv(UNKNOWN_CSV, index=False)

    print(f"[normalize] Saved {OUT_CSV} rows={len(norm)}")
    if cnts:
        print(f"[normalize] Unknown sub_categories saved to {UNKNOWN_CSV} (unique={len(cnts)})")

if __name__ == "__main__":
    main()
