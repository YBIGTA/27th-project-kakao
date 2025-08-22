from langgraph.graph import StateGraph, END
from .nodes.uppercategory_node import uppercategory_node
from .nodes.lowercategory_node import lowercategory_node
from .nodes.scoring_node import scoring_node
from .nodes.product_node import product_node

def build_graph():
    graph = StateGraph()

    # 노드 등록
    graph.add_node("upper", uppercategory_node)
    graph.add_node("lower", lowercategory_node)
    graph.add_node("scoring", scoring_node)
    graph.add_node("product", product_node)

    # 엣지 연결
    graph.add_edge("upper", "lower")
    graph.add_edge("upper", "scoring")
    graph.add_edge("lower", "scoring")
    graph.add_edge("scoring", "product")
    graph.add_edge("product", END)

    return graph
