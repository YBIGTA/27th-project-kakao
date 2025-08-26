#!/usr/bin/env python3
# -*- coding: utf-8 -*-

def test_combine_node():
    """combine_node까지 LangGraph 워크플로우로 테스트"""
    try:
        print("🚀 combine_node까지 LangGraph 워크플로우 테스트 시작")
        
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
        
        # LangGraph 워크플로우 생성 (combine_node까지만)
        workflow = create_gift_recommendation_workflow()
        
        # 초기 상태 설정
        from app.core.state import PipelineState
        initial_state = PipelineState()
        initial_state["profile"] = profile
        
        print("📍 LangGraph 워크플로우 실행 중...")
        
        # combine_node까지만 실행
        # 워크플로우에서 특정 노드까지만 실행하기 위해 상태를 수동으로 설정
        state = initial_state
        
        # 1. 전처리 노드
        print("\n📍 1단계: 전처리 노드 실행 중...")
        from app.core.nodes.preprocess_node import preprocess_node
        state = preprocess_node(state)
        print(f"✅ 전처리 완료: {len(state.get('sentences', []))}개 문장")
        
        # 2. init 노드
        print("\n📍 2단계: init 노드 실행 중...")
        from app.core.nodes.init_node import init_node
        state = init_node(state)
        print(f"✅ init 완료: {len(state.get('parent_list', []))}개 상위, {len(state.get('child_list', []))}개 하위")
        
        # 3. parent_score 노드
        print("\n📍 3단계: parent_score 노드 실행 중...")
        from app.core.nodes.parent_score_node import parent_score_node
        state = parent_score_node(state)
        print(f"✅ parent_score 완료: {len(state.get('parent_scores_prob', {}))}개 상위 카테고리 점수")
        
        # 4. child_score 노드
        print("\n📍 4단계: child_score 노드 실행 중...")
        from app.core.nodes.child_score_node import run_child_score
        state = run_child_score(state)
        print(f"✅ child_score 완료: {len(state.get('child_scores_prob', {}))}개 하위 카테고리 점수")
        
        # 5. combine_node
        print("\n📍 5단계: combine_node 실행 중...")
        from app.core.nodes.combine_node import hierarchy_combine
        state = hierarchy_combine(state)
        print(f"✅ combine_node 완료: {len(state.get('top3_children', []))}개 Top-3 하위 카테고리")
        
        # 결과 출력
        print("\n" + "="*60)
        print("🎯 combine_node까지 LangGraph 워크플로우 결과")
        print("="*60)
        
        final_scores = state.get("final_child_scores", {})
        print(f"\n📊 최종 점수 (상위 10개):")
        for name, score in sorted(final_scores.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {name}: {score:.4f}")
        
        top3_children = state.get("top3_children", [])
        print(f"\n🏆 Top-3 하위 카테고리:")
        for i, name in enumerate(top3_children, 1):
            score = final_scores.get(name, 0.0)
            print(f"  {i}. {name}: {score:.4f}")
        
        top3_reasoning = state.get("top3_children_reasoning", {})
        print(f"\n🧠 Top-3 추론 이유:")
        for name in top3_children:
            reasoning = top3_reasoning.get(name, "이유 없음")
            print(f"  {name}: {reasoning[:100]}...")
        
        # 상태 키 요약
        print(f"\n📋 상태 키들:")
        for key in state.keys():
            if key not in ['sentences', 'processed_messages']:
                value = state[key]
                if isinstance(value, (list, dict)):
                    print(f"  {key}: {type(value).__name__} ({len(value)}개)")
                else:
                    print(f"  {key}: {type(value).__name__} = {value}")
        
        print("\n✅ combine_node까지 LangGraph 워크플로우 테스트 완료!")
        
    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_combine_node()
