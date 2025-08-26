
from typing import Dict, List, TypedDict, Any, Optional

class CategoryScore(TypedDict, total=False):
    name: str
    relevance_raw: float
    interest_raw: float
    score: float
    evidence_idx: List[int]
    reasoning: str

class ProcessedMessage(TypedDict, total=False):
    index: int  # 전처리 후 인덱스 (0부터 시작)
    original_index: int  # 원본 CSV 인덱스
    message: str  # 정리된 메시지
    user: str  # 사용자명
    date: str  # 날짜
    original_message: str  # 원본 메시지
    gift_relevance_score: int  # 선물 관련성 점수

class PipelineState(TypedDict, total=False):
    # Inputs
    sentences: List[str]  # LLM 분석용 정리된 문장 리스트
    processed_messages: List[ProcessedMessage]  # 전처리된 메시지 상세 정보
    profile: Dict[str, Any]  # age, gender, relation, budget_min, budget_max, chat_csv_path, products_csv_path
    parent_list: List[str]
    child_list: List[str]
    parent_to_children: Dict[str, List[str]]
    child_to_parent: Dict[str, str]
    parent_categories: List[str]  # 부모 카테고리 목록

    # Parent scoring
    parent_scores_raw: List[CategoryScore]
    parent_scores_prob: Dict[str, float]
    parent_reasoning: Dict[str, str]
    parent_evidence_idx: Dict[str, List[int]]

    # Child scoring
    child_scores_raw: List[CategoryScore]
    child_scores_prob: Dict[str, float]
    child_reasoning: Dict[str, str]
    child_evidence_idx: Dict[str, List[int]]
    child_scores_info: List[Dict[str, Any]]  # 하위 카테고리 점수 상세 정보

    # Combination
    final_child_scores: Dict[str, float]
    top3_children: List[str]
    top3_children_reasoning: Dict[str, str]

    # Products
    candidate_products: List[Dict[str, Any]]
    selected_products: List[Dict[str, Any]]
    rationales: Dict[str, Dict[str, str]]  # 상품명 -> {rationale, product_url}

