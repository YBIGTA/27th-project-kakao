#!/usr/bin/env python3
"""
parent_score 노드 단독 테스트
"""

import os
import sys
import logging
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# 환경변수 설정
os.environ["BETA_CHILD"] = "0.6"
os.environ["GAMMA_PARENT"] = "0.4"
os.environ["SINGLE_CHILD_PENALTY_LAMBDA"] = "0.1"
os.environ["SOFTMAX_TEMPERATURE"] = "0.9"
os.environ["SCORE_CLAMP_MIN"] = "0.0"
os.environ["SCORE_CLAMP_MAX"] = "1.0"

def test_parent_score_node():
    """parent_score 노드만 테스트"""
    try:
        print("🚀 parent_score 노드 단독 테스트 시작")
        
        # 필요한 모듈들 import
        from app.core.state import PipelineState
        from app.core.nodes.parent_score_node import parent_score_node
        
        # 테스트용 상태 생성
        state = PipelineState()
        
        # 전처리 노드 실행 (의존성)
        print("📍 전처리 노드 실행 중...")
        from app.core.nodes.preprocess_node import preprocess_node
        
        # 더미 프로필 설정
        state["profile"] = {
            "chat_csv_path": "chatt-1.csv",
            "target_user": "박채연"
        }
        
        # 전처리 실행
        state = preprocess_node(state)
        print(f"✅ 전처리 완료: {len(state.get('sentences', []))}개 문장")
        
        # init 노드 실행 (의존성)
        print("📍 init 노드 실행 중...")
        from app.core.nodes.init_node import init_node
        
        # 더미 프로필 추가
        state["profile"].update({
            "age": 25,
            "gender": "여성",
            "relation": "친구",
            "budget_min": 10000,
            "budget_max": 50000,
            "products_csv_path": "kakao_gifts.normalized.csv"
        })
        
        # init 실행
        state = init_node(state)
        print(f"✅ init 완료: {len(state.get('parent_categories', []))}개 상위, {len(state.get('child_categories', []))}개 하위")
        
        # parent_score 노드 실행
        print("📍 parent_score 노드 실행 중...")
        state = parent_score_node(state)
        
        # 결과 출력
        print("\n" + "="*60)
        print("🎯 parent_score 노드 결과")
        print("="*60)
        
        # 상위 카테고리 점수
        parent_scores = state.get("parent_scores", {})
        print(f"\n📊 상위 카테고리 원시 점수 ({len(parent_scores)}개):")
        for name, score in sorted(parent_scores.items(), key=lambda x: x[1], reverse=True):
            print(f"  {name}: {score:.4f}")
        
        # 상위 카테고리 확률
        parent_scores_prob = state.get("parent_scores_prob", {})
        print(f"\n📊 상위 카테고리 확률 점수 ({len(parent_scores_prob)}개):")
        for name, prob in sorted(parent_scores_prob.items(), key=lambda x: x[1], reverse=True):
            print(f"  {name}: {prob:.4f}")
        
        # 상위 카테고리 추론 이유
        parent_reasoning = state.get("parent_reasoning", {})
        print(f"\n🧠 상위 카테고리 추론 이유 ({len(parent_reasoning)}개):")
        for name, reasoning in parent_reasoning.items():
            print(f"  {name}: {reasoning[:100]}...")
        
        # 상태 전체 확인
        print(f"\n📋 상태 키들:")
        for key in state.keys():
            if key not in ['sentences', 'processed_messages']:  # 너무 긴 데이터는 제외
                print(f"  {key}: {type(state[key])}")
        
        print("\n✅ parent_score 노드 테스트 완료!")
        
    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_parent_score_node()
