
from typing import Dict, List
import logging
import pandas as pd
from core.state import PipelineState

logger = logging.getLogger(__name__)

DEFAULT_TAXONOMY = {
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
    "카카오프렌즈": ["토이","리빙","테크","문구","패션","푸드","골프"]
}

def make_child_to_parent(parent_to_children: Dict[str, List[str]]):
    """하위 카테고리에서 상위 카테고리로의 매핑 생성"""
    m = {}
    for p, children in parent_to_children.items():
        for c in children:
            m[c] = p
    return m

def init_node(state: PipelineState) -> PipelineState:
    """초기화 노드: 카테고리 분류 체계 및 기본 설정"""
    try:
        logger.info("Initializing pipeline state")
        
        # CSV 경로 결정
        csv_path = state.get("profile", {}).get("products_csv_path") or "kakao_gifts.normalized.csv"
        logger.info(f"Using products CSV path: {csv_path}")
        
        # 기본 택소노미 직접 사용
        taxonomy = DEFAULT_TAXONOMY
        parent_list = list(taxonomy.keys())
        child_list = []
        for children in taxonomy.values(): 
            child_list.extend(children)
        child_to_parent = make_child_to_parent(taxonomy)

        # 디버깅 로그 추가
        logger.info(f"Taxonomy built: {len(taxonomy)} parent categories")
        logger.info(f"Parent list: {parent_list}")
        logger.info(f"Child list length: {len(child_list)}")
        logger.info(f"First few children: {child_list[:5] if child_list else 'None'}")

        # 상태 업데이트
        state["parent_list"] = parent_list
        state["child_list"] = child_list
        state["parent_to_children"] = taxonomy
        state["child_to_parent"] = child_to_parent
        state["parent_categories"] = parent_list  # 부모 카테고리 목록 추가
        
        logger.info(f"Initialization completed: {len(parent_list)} parent categories, {len(child_list)} child categories")
        return state
        
    except Exception as e:
        logger.error(f"Error in initialization: {e}")
        # 에러 시 기본 분류 체계 사용
        state["parent_list"] = list(DEFAULT_TAXONOMY.keys())
        state["child_list"] = []
        for children in DEFAULT_TAXONOMY.values():
            state["child_list"].extend(children)
        state["parent_to_children"] = DEFAULT_TAXONOMY
        state["child_to_parent"] = make_child_to_parent(DEFAULT_TAXONOMY)
        state["parent_categories"] = list(DEFAULT_TAXONOMY.keys())  # 에러 시에도 추가
        logger.info("Using default taxonomy due to initialization error")
        return state
