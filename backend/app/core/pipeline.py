
from typing import Dict, Any
import logging

# LangGraph 호환성 패치 사용
try:
    from utils.langgraph_patch import StateGraph, END
    print("✅ LangGraph 패치를 통한 import 성공")
except ImportError:
    # 패치가 실패한 경우 더미 클래스 사용
    print("⚠️ LangGraph 패치 실패, 더미 클래스 사용")
    
    class DummyStateGraph:
        def __init__(self, state_type):
            self.state_type = state_type
            self.nodes = {}
            self.edges = {}
        
        def add_node(self, name, func):
            self.nodes[name] = func
        
        def add_edge(self, from_node, to_node):
            self.edges[from_node] = to_node
        
        def set_entry_point(self, node_name):
            self.entry_point = node_name
        
        def compile(self):
            return self
    
    class DummyEND:
        pass
    
    StateGraph = DummyStateGraph  # type: ignore
    END = DummyEND()  # type: ignore

from core.state import PipelineState
from core.nodes.init_node import init_node
from core.nodes.parent_score_node import parent_score_node
from core.nodes.child_score_node import run_child_score
from core.nodes.combine_node import hierarchy_combine
from core.nodes.db_filter_node import db_filter_node
from core.nodes.product_node import product_node
from core.nodes.preprocess_node import preprocess_node

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def build_graph():
    """LangGraph 파이프라인 구축"""
    try:
        logger.info("Building LangGraph pipeline")
        
        g = StateGraph(PipelineState)
        
        # 노드 추가
        g.add_node("preprocess", preprocess_node)  # 대화내역 전처리
        g.add_node("init", init_node)              # 카테고리 분류 체계 초기화
        g.add_node("parent_score", parent_score_node)  # 상위 카테고리 점수
        g.add_node("child_score", run_child_score)    # 하위 카테고리 점수
        g.add_node("combine", hierarchy_combine)       # 계층 결합
        g.add_node("db_filter", db_filter_node)        # 상품 필터링
        g.add_node("product", product_node)            # 최종 상품 선택

        # 엔트리 포인트 설정
        g.set_entry_point("preprocess")
        
        # 엣지 연결 (순서 중요)
        g.add_edge("preprocess", "init")           # 전처리 → 초기화
        g.add_edge("init", "parent_score")         # 초기화 → 상위 카테고리 점수
        g.add_edge("parent_score", "child_score")  # 상위 → 하위 카테고리 점수
        g.add_edge("child_score", "combine")       # 하위 → 계층 결합
        g.add_edge("combine", "db_filter")         # 결합 → 상품 필터링
        g.add_edge("db_filter", "product")         # 필터링 → 최종 선택
        g.add_edge("product", END)                 # 최종 선택 → 종료
        
        compiled_graph = g.compile()
        logger.info("Pipeline graph built and compiled successfully")
        return compiled_graph
        
    except Exception as e:
        logger.error(f"Error building pipeline graph: {e}")
        raise

def run_pipeline(profile: Dict[str, Any]) -> PipelineState:
    """파이프라인 실행"""
    try:
        logger.info("Starting pipeline execution")
        logger.info(f"Input: profile={profile}")
        
        # 그래프 구축 및 실행
        graph = build_graph()
        init_state: PipelineState = {
            "profile": profile,
        }
        
        logger.info("Executing pipeline graph")
        result: PipelineState = graph.invoke(init_state)
        
        logger.info("Pipeline execution completed successfully")
        logger.info(f"Output: {len(result.get('selected_products', []))} selected products")
        
        return result
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        # 에러 시 기본 결과 반환
        return {
            "error": str(e),
            "selected_products": [],
            "rationales": {}
        }  # type: ignore
