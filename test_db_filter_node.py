#!/usr/bin/env python3
# -*- coding: utf-8 -*-

def test_db_filter_node():
    """db_filter_node까지 LangGraph 워크플로우로 테스트"""
    try:
        print("🚀 db_filter_node까지 LangGraph 워크플로우 테스트 시작")
        
        from app.core.workflow import create_gift_recommendation_workflow
        
        # 프로필 설정
        profile = {
            "chat_csv_path": "chatt-1.csv",
            "target_user": "박채연",
            "age": 25,
            "gender": "여성",
            "relation": "친구",
            "budget_min": 10000,
            "budget_max": 50000,
            "products_csv_path": "kakao_gifts.normalized.csv"
        }
        
        # LangGraph 워크플로우 생성 (db_filter_node까지)
        workflow = create_gift_recommendation_workflow()
        
        # 초기 상태 설정
        from app.core.state import PipelineState
        initial_state = PipelineState()
        initial_state["profile"] = profile
        
        print("📍 LangGraph 워크플로우 실행 중...")
        
        # db_filter_node까지 실행
        # 워크플로우에서 특정 노드까지만 실행하기 위해 상태를 수동으로 설정
        state = initial_state
        
        # 1. 전처리 노드
        print("\n📍 1단계: 전처리 노드 실행 중...")
        from app.core.nodes.preprocess_node import preprocess_node
        state = preprocess_node(state)
        print(f"✅ 전처리 완료: {len(state.get('sentences', []))}개 문장")
        
        # 전처리 노드 결과 출력
        print("\n" + "="*60)
        print("🎯 전처리 노드 결과")
        print("="*60)
        sentences = state.get('sentences', [])
        print(f"📋 전처리된 문장 ({len(sentences)}개):")
        for i, sentence in enumerate(sentences[:5], 1):
            print(f"  {i}. {sentence[:100]}...")
        if len(sentences) > 5:
            print(f"  ... 외 {len(sentences) - 5}개")

        # 2. init 노드
        print("\n📍 2단계: init 노드 실행 중...")
        from app.core.nodes.init_node import init_node
        state = init_node(state)
        print(f"✅ init 완료: {len(state.get('parent_list', []))}개 상위, {len(state.get('child_list', []))}개 하위")
        
        # init 노드 결과 출력
        print("\n" + "="*60)
        print("🎯 init 노드 결과")
        print("="*60)
        print(f"📋 상위 카테고리 ({len(state.get('parent_list', []))}개):")
        for i, parent in enumerate(state.get('parent_list', [])[:5], 1):
            print(f"  {i}. {parent}")
        if len(state.get('parent_list', [])) > 5:
            print(f"  ... 외 {len(state.get('parent_list', [])) - 5}개")
        
        print(f"\n📋 하위 카테고리 ({len(state.get('child_list', []))}개):")
        for i, child in enumerate(state.get('child_list', [])[:10], 1):
            print(f"  {i}. {child}")
        if len(state.get('child_list', [])) > 10:
            print(f"  ... 외 {len(state.get('child_list', [])) - 10}개")

        # 3. parent_score 노드
        print("\n📍 3단계: parent_score 노드 실행 중...")
        from app.core.nodes.parent_score_node import parent_score_node
        state = parent_score_node(state)
        print(f"✅ parent_score 완료: {len(state.get('parent_scores_prob', {}))}개 상위 카테고리 점수")
        
        # parent_score 노드 결과 출력
        print("\n" + "="*60)
        print("🎯 parent_score 노드 결과")
        print("="*60)
        parent_scores_prob = state.get('parent_scores_prob', {})
        print(f"📊 상위 카테고리 확률 점수 (상위 10개):")
        for name, prob in sorted(parent_scores_prob.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {name}: {prob:.4f}")
        
        parent_reasoning = state.get('parent_reasoning', {})
        print(f"\n🧠 상위 카테고리 추론 이유 (상위 5개):")
        for name, reasoning in list(parent_reasoning.items())[:5]:
            print(f"  {name}: {reasoning[:100]}...")

        # 4. child_score 노드
        print("\n📍 4단계: child_score 노드 실행 중...")
        from app.core.nodes.child_score_node import run_child_score
        state = run_child_score(state)
        print(f"✅ child_score 완료: {len(state.get('child_scores_prob', {}))}개 하위 카테고리 점수")
        
        # child_score 노드 결과 출력
        print("\n" + "="*60)
        print("🎯 child_score 노드 결과")
        print("="*60)
        child_scores_prob = state.get('child_scores_prob', {})
        print(f"📊 하위 카테고리 확률 점수 (상위 10개):")
        for name, prob in sorted(child_scores_prob.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {name}: {prob:.4f}")
        
        # child_scores_info 상세 정보 출력
        child_scores_info = state.get('child_scores_info', [])
        print(f"\n🔍 하위 카테고리 상세 정보 (상위 5개):")
        for i, info in enumerate(child_scores_info[:5], 1):
            print(f"  {i}. {info['name']}:")
            print(f"     최종 점수: {info.get('score', 'N/A'):.4f}")
            print(f"     관련성 원시 점수: {info.get('relevance_raw', 'N/A'):.4f}")
            print(f"     관심도 원시 점수: {info.get('interest_raw', 'N/A'):.4f}")
            print(f"     추론 이유: {info.get('reasoning', 'N/A')[:100]}...")
            print(f"     증거 문장 인덱스: {info.get('evidence_idx', 'N/A')}")
            print()
        
        if len(child_scores_info) > 5:
            print(f"  ... 외 {len(child_scores_info) - 5}개")

        # 5. combine_node
        print("\n📍 5단계: combine_node 실행 중...")
        from app.core.nodes.combine_node import hierarchy_combine
        state = hierarchy_combine(state)
        print(f"✅ combine_node 완료: {len(state.get('top3_children', []))}개 Top-3 하위 카테고리")
        
        # combine_node 결과 출력
        print("\n" + "="*60)
        print("🎯 combine_node 결과")
        print("="*60)
        final_scores = state.get('final_child_scores', {})
        print(f"📊 최종 점수 (상위 10개):")
        for name, score in sorted(final_scores.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {name}: {score:.4f}")
        
        top3_children = state.get('top3_children', [])
        print(f"\n🏆 Top-3 하위 카테고리:")
        for i, name in enumerate(top3_children, 1):
            score = final_scores.get(name, 0.0)
            print(f"  {i}. {name}: {score:.4f}")

        # 6. db_filter_node
        print("\n📍 6단계: db_filter_node 실행 중...")
        from app.core.nodes.db_filter_node import db_filter_node
        state = db_filter_node(state)
        print(f"✅ db_filter_node 완료: {len(state.get('candidate_products', []))}개 후보 상품")
        
        # db_filter_node 결과 출력
        print("\n" + "="*60)
        print("🎯 db_filter_node 결과")
        print("="*60)
        candidate_products = state.get('candidate_products', [])
        print(f"📦 후보 상품 ({len(candidate_products)}개):")
        for i, product in enumerate(candidate_products[:5], 1):
            print(f"  {i}. {product.get('product_name', 'N/A')}")
            print(f"     브랜드: {product.get('brand', 'N/A')}")
            print(f"     가격: {product.get('price', 'N/A')}원")
            print(f"     카테고리: {product.get('sub_category', 'N/A')}")
            print()
        if len(candidate_products) > 5:
            print(f"  ... 외 {len(candidate_products) - 5}개")

        # 최종 상태 요약
        print("\n" + "="*60)
        print("🎯 전체 파이프라인 상태 요약")
        print("="*60)
        print(f"📋 상태 키들:")
        for key in state.keys():
            if key not in ['sentences', 'processed_messages']:
                value = state[key]
                if isinstance(value, (list, dict)):
                    print(f"  {key}: {type(value).__name__} ({len(value)}개)")
                else:
                    print(f"  {key}: {type(value).__name__} = {value}")
        
        print("\n✅ db_filter_node까지 LangGraph 워크플로우 테스트 완료!")
        
    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_db_filter_node()
