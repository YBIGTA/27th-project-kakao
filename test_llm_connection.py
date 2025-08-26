#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
sys.path.append('/Users/yezzi/Desktop/kakao_gift_langgraph')

from app.services.llm.client import LLMClient

def test_llm_connection():
    """LLM 연결 테스트"""
    try:
        print("🔌 LLM 연결 테스트 시작...")
        
        # 환경 변수 확인
        print(f"UPSTAGE_API_KEY: {'설정됨' if os.getenv('UPSTAGE_API_KEY') else '설정되지 않음'}")
        print(f"UPSTAGE_BASE_URL: {os.getenv('UPSTAGE_BASE_URL', '설정되지 않음')}")
        print(f"UPSTAGE_MODEL: {os.getenv('UPSTAGE_MODEL', '설정되지 않음')}")
        
        # LLM 클라이언트 생성
        print("\n📡 LLM 클라이언트 생성 중...")
        llm_client = LLMClient()
        print("✅ LLM 클라이언트 생성 성공")
        
        # 간단한 테스트 프롬프트
        test_prompt = "다음 카테고리 중에서 가장 적합한 것을 하나만 선택하고 JSON으로 응답해주세요: ['커피', '차', '음료']"
        
        print(f"\n🧪 테스트 프롬프트 전송: {test_prompt[:50]}...")
        response = llm_client._chat_json(test_prompt)
        
        print(f"✅ LLM 응답 성공: {type(response)}")
        print(f"📄 응답 내용: {response}")
        
        return True
        
    except Exception as e:
        print(f"❌ LLM 연결 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_llm_connection()
    if success:
        print("\n🎉 LLM 연결 테스트 성공!")
    else:
        print("\n💥 LLM 연결 테스트 실패!")
