#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Dict, Any
from langgraph import graph
from core.state import PipelineState
from core.nodes.preprocess_node import preprocess_node
from core.nodes.init_node import init_node
from core.nodes.parent_score_node import parent_score_node
from core.nodes.child_score_node import run_child_score
from core.nodes.combine_node import hierarchy_combine
from core.nodes.db_filter_node import db_filter_node
from core.nodes.product_node import product_node

def create_gift_recommendation_workflow():
    """카카오 기프트 추천 LangGraph 워크플로우 생성"""
    
    # StateGraph 정의
    workflow = graph.StateGraph(PipelineState)
    
    # 노드 등록
    workflow.add_node("preprocess", preprocess_node)
    workflow.add_node("init", init_node)
    workflow.add_node("parent_score", parent_score_node)
    workflow.add_node("child_score", run_child_score)
    workflow.add_node("combine", hierarchy_combine)
    workflow.add_node("db_filter", db_filter_node)
    workflow.add_node("product", product_node)
    
    # 엣지 연결 (순차적 실행)
    workflow.add_edge(graph.START, "preprocess")
    workflow.add_edge("preprocess", "init")
    workflow.add_edge("init", "parent_score")
    workflow.add_edge("parent_score", "child_score")
    workflow.add_edge("child_score", "combine")
    workflow.add_edge("combine", "db_filter")
    workflow.add_edge("db_filter", "product")
    workflow.add_edge("product", graph.END)
    
    # 그래프 컴파일
    app = workflow.compile()
    
    return app

def run_gift_recommendation(profile: Dict[str, Any]) -> PipelineState:
    """카카오 기프트 추천 워크플로우 실행"""
    
    # 워크플로우 생성
    app = create_gift_recommendation_workflow()
    
    # 초기 상태 설정
    initial_state = PipelineState()
    initial_state["profile"] = profile
    
    # 워크플로우 실행
    try:
        result = app.invoke(initial_state)
        return result
    except Exception as e:
        print(f"❌ 워크플로우 실행 실패: {e}")
        raise
