#!/usr/bin/env python3
# -*- coding: utf-8 -*-

def test_langgraph_workflow():
    """LangGraph 워크플로우 테스트"""
    try:
        print("🚀 LangGraph 워크플로우 테스트 시작")
        
        from app.core.workflow import run_gift_recommendation
        
        # 프로필 설정
        profile = {
            "chat_csv_path": "chatt-1.csv",
            "target_user": "박채연",
            "age": 25,
            "gender": "여성",
            "relation": "친구",
            "budget_min": 30000,
            "budget_max": 50000,
            "products_csv_path": "kakao_gifts.normalized.csv"
        }
        
        print("📍 LangGraph 워크플로우 실행 중...")
        
        # LangGraph 워크플로우 실행
        result = run_gift_recommendation(profile)
        
        print("✅ LangGraph 워크플로우 실행 완료!")
        
        # 결과 출력
        print("\n" + "="*60)
        print("🎯 LangGraph 워크플로우 결과")
        print("="*60)
        
        # 전처리 결과
        sentences = result.get('sentences', [])
        print(f"📋 전처리된 문장: {len(sentences)}개")
        
        # 카테고리 결과
        parent_list = result.get('parent_list', [])
        child_list = result.get('child_list', [])
        print(f"📋 상위 카테고리: {len(parent_list)}개")
        print(f"📋 하위 카테고리: {len(child_list)}개")
        
        # 점수 결과
        parent_scores = result.get('parent_scores_prob', {})
        child_scores = result.get('child_scores_prob', {})
        print(f"📊 상위 카테고리 점수: {len(parent_scores)}개")
        print(f"📊 하위 카테고리 점수: {len(child_scores)}개")
        
        # Top-3 결과
        top3_children = result.get('top3_children', [])
        print(f"🏆 Top-3 하위 카테고리: {len(top3_children)}개")
        for i, child in enumerate(top3_children, 1):
            print(f"  {i}. {child}")
        
        # 후보 상품 결과
        candidate_products = result.get('candidate_products', [])
        print(f"📦 후보 상품: {len(candidate_products)}개")
        
        # 선택된 상품 결과
        selected_products = result.get('selected_products', [])
        rationales = result.get('rationales', {})
        if selected_products:
            print(f"🎁 최종 추천 상품: {len(selected_products)}개")
            for i, product in enumerate(selected_products, 1):
                product_name = product.get('product_name', 'N/A')
                product_url = product.get('product_url', 'N/A')
                rationale_info = rationales.get(product_name, {})
                rationale = rationale_info.get('rationale', '이유 없음') if isinstance(rationale_info, dict) else rationale_info
                
                print(f"  {i}. {product_name}")
                print(f"     URL: {product_url}")
                print(f"     추천 이유: {rationale[:100]}...")
                print()
        
        # 상태 키 요약
        print(f"\n📋 상태 키들:")
        for key in result.keys():
            if key not in ['sentences', 'processed_messages']:
                value = result[key]
                if isinstance(value, (list, dict)):
                    print(f"  {key}: {type(value).__name__} ({len(value)}개)")
                else:
                    print(f"  {key}: {type(value).__name__} = {value}")
        
        print("\n✅ LangGraph 워크플로우 테스트 완료!")
        
    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_langgraph_workflow()
