#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
sys.path.append('/Users/yezzi/Desktop/kakao_gift_langgraph')

from app.core.state import PipelineState
from app.core.nodes.child_score_node import run_child_score

def test_mini_workflow():
    """최소한의 워크플로우로 child_score_node 테스트"""
    try:
        print("🧪 최소 워크플로우 테스트 시작...")
        
        # 초기 상태 생성
        state = PipelineState()
        
        # 테스트 데이터 설정
        state["sentences"] = [
            "오늘 카페에서 커피 마셨어",
            "스타벅스에서 아메리카노 주문했어",
            "차 한잔 마시면서 대화했어"
        ]
        
        state["child_list"] = ["커피/차/음료", "베이커리/도넛/떡", "카페"]
        
        # parent_scores_prob 설정 (parent_score_node가 만든 것과 동일한 구조)
        state["parent_scores_prob"] = {
            "식품": 0.3,
            "교환권": 0.7
        }
        
        print(f"📝 상태 설정 완료:")
        print(f"  - sentences: {len(state['sentences'])}개")
        print(f"  - child_list: {len(state['child_list'])}개")
        print(f"  - parent_scores_prob: {len(state['parent_scores_prob'])}개")
        print(f"  - parent_categories: {list(state['parent_scores_prob'].keys())}")
        
        # child_score_node 실행
        print("\n🚀 child_score_node 실행 중...")
        result_state = run_child_score(state)
        
        print(f"✅ child_score_node 완료")
        
        # 결과 확인
        print(f"\n📊 결과 확인:")
        print(f"  - final_child_scores: {len(result_state.get('final_child_scores', {}))}개")
        print(f"  - child_scores_prob: {len(result_state.get('child_scores_prob', {}))}개")
        print(f"  - child_scores_info: {len(result_state.get('child_scores_info', []))}개")
        
        if result_state.get('final_child_scores'):
            print("\n📋 상세 점수:")
            for name, score in result_state['final_child_scores'].items():
                print(f"  {name}: {score}")
        
        if result_state.get('child_scores_info'):
            print("\n📋 상세 정보:")
            for info in result_state['child_scores_info']:
                print(f"  {info['name']}: score={info['score']}, relevance={info['relevance_raw']}, interest={info['interest_raw']}")
        
        return True
        
    except Exception as e:
        print(f"❌ 최소 워크플로우 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_mini_workflow()
    if success:
        print("\n🎉 최소 워크플로우 테스트 완료!")
    else:
        print("\n💥 최소 워크플로우 테스트 실패!")
