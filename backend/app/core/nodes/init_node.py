from core.state import GraphState
from core.config import PARENT_LABELS, CHILDREN_OF

def init_node(state: GraphState) -> GraphState:
    """INTEGRATION POINT: Rows are assumed preprocessed already."""
    state.parent_labels = list(PARENT_LABELS)
    state.child_labels_map = {k: list(v) for k, v in CHILDREN_OF.items()}
    return state
