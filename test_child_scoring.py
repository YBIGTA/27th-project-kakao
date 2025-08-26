#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
sys.path.append('/Users/yezzi/Desktop/kakao_gift_langgraph')

from app.services.llm.client import LLMClient

def test_child_scoring():
    """Child scoring 직접 테스트"""
    try:
        print("🧪 Child Scoring 테스트 시작...")
        
        # LLM 클라이언트 생성
        llm_client = LLMClient()
        print("✅ LLM 클라이언트 생성 성공")
        
        # 테스트 데이터 - 올바른 매핑 사용
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
        
        # Child scoring 실행
        print("\n🚀 Child scoring 실행 중...")
        result = llm_client.score_children(sentences, child_list, parent_categories)
        
        print(f"✅ Child scoring 완료: {type(result)}")
        print(f"📊 결과 개수: {len(result) if result else 0}")
        
        if result:
            print("\n📋 상세 결과:")
            for name, data in result.items():
                print(f"  {name}:")
                print(f"    - relevance_raw: {data.get('relevance_raw', 'N/A')}")
                print(f"    - interest_raw: {data.get('interest_raw', 'N/A')}")
                print(f"    - reasoning: {data.get('reasoning', 'N/A')[:100]}...")
                print(f"    - evidence_idx: {data.get('evidence_idx', 'N/A')}")
        else:
            print("❌ 결과가 없습니다")
        
        return True
        
    except Exception as e:
        print(f"❌ Child scoring 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_child_scoring()
    if success:
        print("\n🎉 Child scoring 테스트 완료!")
    else:
        print("\n💥 Child scoring 테스트 실패!")
