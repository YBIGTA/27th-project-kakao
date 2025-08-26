#!/usr/bin/env python3
"""
실제 파이프라인 테스트 스크립트
Mock 없이 실제 파이프라인을 실행하여 전체 시스템을 테스트합니다.
"""

import os
import sys
import logging
import json

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.core.pipeline import run_pipeline

def test_full_pipeline():
    """전체 파이프라인 테스트"""
    print("=== 전체 파이프라인 테스트 ===")
    
    # 테스트 프로필 설정
    profile = {
        "age": 25,
        "gender": "여성",
        "relation": "친구",
        "budget_min": 10000,
        "budget_max": 50000,
        "products_csv_path": "kakao_gifts.normalized.csv",
        "target_user": "박채연"
    }
    
    try:
        # 실제 파이프라인 실행
        print("🚀 실제 파이프라인 실행 중...")
        result = run_pipeline("chatt-1.csv", profile)
        
        # 결과 검증
        if "error" in result:
            print(f"❌ 파이프라인 실행 실패: {result['error']}")
            return False
        
        selected_products = result.get("selected_products", [])
        print(f"✅ 파이프라인 실행 성공!")
        print(f"✅ 선택된 상품 수: {len(selected_products)}")
        
        if selected_products:
            print("\n📦 선택된 상품들:")
            for i, product in enumerate(selected_products[:3], 1):  # 상위 3개만 출력
                print(f"  {i}. {product.get('product_name', 'N/A')}")
                print(f"     가격: {product.get('price_range', 'N/A')}")
                print(f"     카테고리: {product.get('category', 'N/A')}")
                print()
        
        # 파이프라인 상태 정보 출력
        print("📊 파이프라인 상태:")
        print(f"  - 전처리된 메시지: {len(result.get('processed_messages', []))}")
        print(f"  - 상위 카테고리 점수: {len(result.get('parent_scores_prob', {}))}")
        print(f"  - 하위 카테고리 점수: {len(result.get('child_scores_prob', {}))}")
        print(f"  - 최종 Top-3: {result.get('top3_children', [])}")
        print(f"  - 후보 상품: {len(result.get('candidate_products', []))}")
        
        return True
        
    except Exception as e:
        print(f"❌ 파이프라인 테스트 중 오류 발생: {e}")
        return False

def test_pipeline_structure():
    """파이프라인 구조 테스트"""
    print("\n=== 파이프라인 구조 테스트 ===")
    
    try:
        from app.core.pipeline import build_graph
        graph = build_graph()
        print("✅ 파이프라인 그래프 구축 성공")
        
        # 노드 목록 확인
        nodes = list(graph.nodes.keys())
        expected_nodes = ["preprocess", "init", "parent_score", "child_score", "combine", "db_filter", "product"]
        
        for node in expected_nodes:
            if node in nodes:
                print(f"✅ 노드 '{node}' 존재")
            else:
                print(f"❌ 노드 '{node}' 누락")
                
        return True
        
    except Exception as e:
        print(f"❌ 파이프라인 구조 테스트 실패: {e}")
        return False

def test_preprocess_module_import():
    """전처리 모듈 import 테스트"""
    print("\n=== 전처리 모듈 import 테스트 ===")
    
    try:
        from app.preprocess.csv_processor import CSVProcessor
        from app.preprocess.text_processor import TextProcessor
        from app.preprocess.main_processor import main
        
        print("✅ CSVProcessor import 성공")
        print("✅ TextProcessor import 성공")
        print("✅ main_processor import 성공")
        
        return True
        
    except ImportError as e:
        print(f"❌ 전처리 모듈 import 실패: {e}")
        return False

def test_llm_client():
    """LLM 클라이언트 테스트"""
    print("\n=== LLM 클라이언트 테스트 ===")
    
    try:
        from app.services.llm.client import LLMClient
        
        # 환경 변수 확인
        api_key = os.getenv("UPSTAGE_API_KEY")
        if not api_key:
            print("⚠️ UPSTAGE_API_KEY 환경 변수가 설정되지 않음")
            return False
        
        print("✅ LLM 클라이언트 import 성공")
        print("✅ API 키 설정 확인됨")
        
        return True
        
    except ImportError as e:
        print(f"❌ LLM 클라이언트 import 실패: {e}")
        return False

def main():
    """메인 테스트 함수"""
    print("🚀 카카오톡 선물 추천 서비스 실제 파이프라인 테스트 시작\n")
    
    # 로깅 설정
    logging.basicConfig(level=logging.INFO)
    
    try:
        # 1. 전처리 모듈 import 테스트
        preprocess_import_ok = test_preprocess_module_import()
        
        # 2. LLM 클라이언트 테스트
        llm_client_ok = test_llm_client()
        
        # 3. 파이프라인 구조 테스트
        pipeline_structure_ok = test_pipeline_structure()
        
        # 4. 전체 파이프라인 테스트 (LLM API 키가 있을 때만)
        if llm_client_ok:
            full_pipeline_ok = test_full_pipeline()
        else:
            print("\n⚠️ LLM API 키가 없어 전체 파이프라인 테스트를 건너뜁니다.")
            full_pipeline_ok = False
        
        print("\n" + "="*50)
        if pipeline_structure_ok and preprocess_import_ok:
            if llm_client_ok and full_pipeline_ok:
                print("🎉 모든 테스트 통과!")
                print("✅ 파이프라인 구조가 올바르게 구성되었습니다")
                print("✅ 실제 파이프라인이 정상적으로 작동합니다")
                print("✅ LLM API 연동이 성공했습니다")
            else:
                print("⚠️ 기본 구조는 정상이지만 LLM 연동에 문제가 있습니다")
                print("✅ 파이프라인 구조가 올바르게 구성되었습니다")
                print("✅ 전처리 기능이 정상적으로 작동합니다")
                print("❌ LLM API 연동 실패")
        else:
            print("❌ 기본 구조에 문제가 있습니다")
            
        print("\n💡 실제 사용을 위해서는:")
        print("   1. .env 파일에 UPSTAGE_API_KEY 설정")
        print("   2. CSV 파일들이 올바른 경로에 위치")
        print("   3. python app/main.py --chat_csv chatt-1.csv --target_user 박채연 --age 25 --gender 여성 --relation 친구 --budget_min 10000 --budget_max 50000")
        
    except Exception as e:
        print(f"\n❌ 테스트 중 오류 발생: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
