#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
sys.path.append('/Users/yezzi/Desktop/kakao_gift_langgraph')

from app.services.llm.scorer import run_child_scoring

def test_scorer_direct():
    """scorer.py를 직접 테스트"""
    try:
        print("🧪 Scorer 직접 테스트 시작...")
        
        # 테스트 데이터
        sentences = [
            "오늘 카페에서 커피 마셨어",
            "스타벅스에서 아메리카노 주문했어",
            "차 한잔 마시면서 대화했어"
        ]
        
        child_list = ["커피/차/음료", "베이커리/도넛/떡", "카페"]
        parent_categories = ["식품", "교환권"]
        
        print(f"📝 테스트 문장: {len(sentences)}개")
        print(f"🎯 하위 카테고리: {child_list}")
        print(f"🏷️ 부모 카테고리: {parent_categories}")
        
        # Scorer 직접 호출
        print("\n🚀 run_child_scoring 직접 호출...")
        scores, scores_info = run_child_scoring(sentences, child_list, parent_categories)
        
        print(f"✅ run_child_scoring 완료")
        print(f"📊 scores: {type(scores)}, 개수: {len(scores) if scores else 0}")
        print(f"📋 scores_info: {type(scores_info)}, 개수: {len(scores_info) if scores_info else 0}")
        
        if scores:
            print("\n📋 상세 결과:")
            for name, score in scores.items():
                print(f"  {name}: {score}")
        
        if scores_info:
            print("\n📋 상세 정보:")
            for info in scores_info:
                print(f"  {info}")
        
        return True
        
    except Exception as e:
        print(f"❌ Scorer 직접 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_scorer_direct()
    if success:
        print("\n🎉 Scorer 직접 테스트 완료!")
    else:
        print("\n💥 Scorer 직접 테스트 실패!")
