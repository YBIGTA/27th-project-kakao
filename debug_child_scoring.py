#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
sys.path.append('/Users/yezzi/Desktop/kakao_gift_langgraph')

from app.services.llm.client import LLMClient

def debug_child_scoring():
    """Child scoring 과정을 단계별로 디버깅"""
    try:
        print("🔍 Child Scoring 디버깅 시작...")
        
        # LLM 클라이언트 생성
        llm_client = LLMClient()
        print("✅ LLM 클라이언트 생성 성공")
        
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
        
        # 1단계: 부모별 그룹화
        print("\n🔍 1단계: 부모별 그룹화")
        parent_child_groups = llm_client._group_children_by_parent(child_list, parent_categories)
        print(f"그룹화 결과: {parent_child_groups}")
        
        # 2단계: 각 부모별로 처리
        all_results = {}
        for parent_name, children in parent_child_groups.items():
            print(f"\n🔍 부모 '{parent_name}' 처리 중...")
            print(f"  자식 카테고리: {children}")
            
            # 배치 처리
            batch_size = len(children)
            batches = [children[i:i + batch_size] for i in range(0, len(children), batch_size)]
            print(f"  배치 구성: {len(batches)}개 배치, 배치 크기: {batch_size}")
            
            parent_results = {}
            
            for batch_idx, batch in enumerate(batches):
                print(f"  🔍 배치 {batch_idx + 1}/{len(batches)}: {batch}")
                
                # LLM 호출
                batch_result = llm_client._score_children_batch(sentences, batch)
                print(f"  LLM 응답: {type(batch_result)}, 결과 수: {len(batch_result) if batch_result else 0}")
                
                if batch_result:
                    print(f"  배치 결과 상세:")
                    for name, data in batch_result.items():
                        print(f"    {name}: relevance={data.get('relevance_raw')}, interest={data.get('interest_raw')}")
                    parent_results.update(batch_result)
                
            # Softmax 적용
            if parent_results:
                print(f"  🔍 Softmax 적용 전: {len(parent_results)}개")
                print(f"  원본 데이터 구조:")
                for name, data in parent_results.items():
                    print(f"    {name}: {data}")
                
                parent_results = llm_client._apply_softmax_to_parent_group(parent_results)
                print(f"  🔍 Softmax 적용 후: {len(parent_results)}개")
                print(f"  수정된 데이터 구조:")
                for name, data in parent_results.items():
                    print(f"    {name}: {data}")
                
                all_results.update(parent_results)
            else:
                print(f"  ❌ 부모 '{parent_name}'에서 결과 없음")
        
        print(f"\n🎯 최종 결과: {len(all_results)}개")
        if all_results:
            print("상세 결과:")
            for name, data in all_results.items():
                print(f"  {name}: {data}")
        else:
            print("❌ 최종 결과가 없습니다")
        
        return True
        
    except Exception as e:
        print(f"❌ 디버깅 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = debug_child_scoring()
    if success:
        print("\n🎉 디버깅 완료!")
    else:
        print("\n💥 디버깅 실패!")
