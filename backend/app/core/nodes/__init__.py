"""
파이프라인 노드들
"""

from ..state import GraphState
from ...config.settings import PARENT_LABELS, CHILDREN_OF

# init_node
def init_node(state: GraphState) -> GraphState:
    """INTEGRATION POINT: Rows are assumed preprocessed already."""
    state.parent_labels = list(PARENT_LABELS)
    state.child_labels_map = {k: list(v) for k, v in CHILDREN_OF.items()}
    return state

# 노드 클래스들
from .uppercategory_node import parent_score_node
from .lowercategory_node import child_score_node
from .joint_gate_node import hierarchy_node
from .db_filter_node import db_filter_node
from .product_node import product_node
from .pack_node import pack_node
from .select_top3_node import select_top3_node

__all__ = [
    'init_node',
    'parent_score_node',
    'child_score_node', 
    'hierarchy_node',
    'db_filter_node',
    'product_node',
    'pack_node',
    'select_top3_node'
    ]