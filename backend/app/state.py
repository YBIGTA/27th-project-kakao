from dataclasses import dataclass, field
from typing import List, Dict, Any

@dataclass
class GiftContext:
    age: int
    gender: str
    relation: str
    budget_min: int
    budget_max: int

@dataclass
class MessageRow:
    idx: int      # 문장 인덱스 (0부터 시작)
    date: str
    user: str
    text: str

@dataclass
class GraphState:
    rows: List[MessageRow]
    ctx: GiftContext

    parent_labels: List[str] = field(default_factory=list)
    child_labels_map: Dict[str, List[str]] = field(default_factory=dict)

    # 상위 카테고리 스코어링 결과
    parent_scores: Dict[str, float] = field(default_factory=dict)
    parent_reasoning: Dict[str, List[str]] = field(default_factory=dict)
    parent_evidence_idx: Dict[str, List[int]] = field(default_factory=dict)
    
    # 새로운 상위 카테고리 신호들
    parent_polarity: Dict[str, str] = field(default_factory=dict)  # pos/neg/neutral
    parent_exclusivity: Dict[str, str] = field(default_factory=dict)  # high/mid/low
    parent_form_signals: Dict[str, Dict[str, int]] = field(default_factory=dict)  # form_signals

    # 하위 카테고리 스코어링 결과
    child_scores: Dict[str, float] = field(default_factory=dict)
    child_reasoning: Dict[str, List[str]] = field(default_factory=dict)
    child_evidence_idx: Dict[str, List[int]] = field(default_factory=dict)
    
    # 새로운 하위 카테고리 신호들
    child_specificity: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # specificity
    child_ownership_hint: Dict[str, str] = field(default_factory=dict)  # ownership_hint
    child_explicit_rejection: Dict[str, bool] = field(default_factory=dict)  # explicit_rejection

    # 계층 결합 결과
    final_child_scores: Dict[str, float] = field(default_factory=dict)
    top3_children: List[str] = field(default_factory=list)

    # 상품 선택 결과
    candidate_products: List[Dict[str, Any]] = field(default_factory=list)
    selected_products: List[Dict[str, Any]] = field(default_factory=list)
    rationales: Dict[str, str] = field(default_factory=dict)

    # 디버그 정보
    debug: Dict[str, Any] = field(default_factory=dict)