# -*- coding: utf-8 -*-
"""
종결어미 시스템 테스트 스크립트
- 문맥 점수 시스템 테스트 (공백 앞 -3점 포함)
- 종결어미 탐지, 문맥 점수, 분할 매커니즘 테스트
"""

import sys
import os
sys.path.append('.')

from utils.korean_eomi_dict import (
    END_EOMI_RE, 
    END_EOMI_SPLIT_RE,
    get_eomi_context_score
)
from utils.sbd_processor import SBDConfig, count_eomi_in_text, split_long_sentence

# 테스트할 긴 문장
test_text = """창문을 조금 열어 두었더니 밤새 축적된 더운 공기가 미련 없이 빠져나갔다... 식탁 위에는 미처 정리하지 못한 노트와 펜이 엉켜 있었고, 찬물에 적신 머그컵은 유리 표면에 얇은 물기를 ... 휴대폰 알람은 세 번째로 울렸지만, 나는 굳이 끄지 않고 리듬을 잠시 더 듣다가 조용히 멈췄다. 오늘 해야 할 일들이 머릿속에서 목록처럼 줄을 섰지만, 이상하게도 마음은 서두르지 않았다 창밖으로 골목을 청소하는 바람의 소리가 들릴 때, 한 모금의 물이 몸 안쪽으로 내려가며 작은 스위치를 누르는 듯했다... 그 순간이 지나자, 미뤄 둔 문장을 다시 이어 쓸 시간이라는 사실이 자연스럽게 받아들여졌다."""

def test_eomi_detection():
    """종결어미 탐지 테스트"""
    print("=== 종결어미 탐지 테스트 ===")
    print(f"테스트 텍스트: {test_text}")
    print()
    
    # END_EOMI_SPLIT_RE로 모든 종결어미 찾기
    matches = list(END_EOMI_SPLIT_RE.finditer(test_text))
    
    if matches:
        print(f"총 {len(matches)}개의 종결어미 발견:")
        for i, match in enumerate(matches):
            eomi_str = match.group()
            start, end = match.start(), match.end()
            context = test_text[max(0, start-5):end+5]
            
            print(f"  {i+1}. '{eomi_str}' (위치: {start}-{end})")
            print(f"      문맥: ...{context}...")
        print()
    else:
        print("❌ 종결어미를 찾지 못함!")
        print()

def test_context_score():
    """문맥 점수 테스트"""
    print("=== 문맥 점수 테스트 ===")
    
    # 실제 발견된 종결어미들의 문맥 점수 계산
    matches = list(END_EOMI_SPLIT_RE.finditer(test_text))
    
    print(f"발견된 {len(matches)}개 종결어미의 문맥 점수:")
    for i, match in enumerate(matches):
        eomi_str = match.group()
        start, end = match.start(), match.end()
        
        # 문맥 점수 계산 (정확한 위치 정보 전달)
        context_score = get_eomi_context_score(test_text, eomi_str, start)
        
        # 문맥 정보
        context_before = test_text[max(0, start-3):start]
        context_after = test_text[end:min(len(test_text), end+3)]
        
        print(f"  {i+1}. '{eomi_str}' (위치: {start}-{end})")
        print(f"      문맥: ...{context_before}【{eomi_str}】{context_after}...")
        print(f"      문맥 점수: {context_score:.2f}")
        print(f"      4점 이상: {'✅' if context_score >= 4.0 else '❌'}")
        print()
    
    print()

def test_context_score_detailed():
    """상세 문맥 점수 분석 테스트"""
    print("=== 상세 문맥 점수 분석 ===")
    
    # 실제 발견된 종결어미들의 상세 분석
    matches = list(END_EOMI_SPLIT_RE.finditer(test_text))
    
    for i, match in enumerate(matches):
        eomi_str = match.group()
        start, end = match.start(), match.end()
        
        # 문맥 점수 계산 (정확한 위치 정보 전달)
        context_score = get_eomi_context_score(test_text, eomi_str, start)
        
        # 문맥 분석
        eomi_pos = start  # 정확한 위치 사용
        prev_char = test_text[eomi_pos - 1] if eomi_pos > 0 else "없음"
        next_char = test_text[eomi_pos + len(eomi_str)] if eomi_pos + len(eomi_str) < len(test_text) else "없음"
        
        print(f"  {i+1}. '{eomi_str}' (위치: {start}-{end})")
        print(f"      앞 문자: '{prev_char}', 뒤 문자: '{next_char}'")
        print(f"      문맥 점수: {context_score:.2f}")
        print(f"      4점 이상: {'✅' if context_score >= 4.0 else '❌'}")
        print()
    
    print()

def test_eomi_count():
    """종결어미 개수 세기 테스트"""
    print("=== 종결어미 개수 세기 테스트 ===")
    
    count = count_eomi_in_text(test_text)
    print(f"4점 이상인 종결어미 개수: {count}개")
    print()

def test_split_long_sentence():
    """긴 문장 분할 테스트"""
    print("=== 긴 문장 분할 테스트 ===")
    
    cfg = SBDConfig(
        max_sentence_length=200,
        max_eomi_count=4
    )
    
    split_sentences = split_long_sentence(test_text, cfg)
    
    print(f"원본 길이: {len(test_text)}자")
    print(f"분할된 문장 수: {len(split_sentences)}개")
    print()
    
    for i, sentence in enumerate(split_sentences, 1):
        print(f"문장 {i} ({len(sentence)}자):")
        print(f"  {sentence}")
        print()

def test_threshold_system():
    """임계치 시스템 테스트"""
    print("=== 임계치 시스템 테스트 ===")
    print("4점 이상인 종결어미만 분할/카운트 기준으로 사용")
    print()
    
    # 실제 발견된 종결어미들의 임계치 테스트
    matches = list(END_EOMI_SPLIT_RE.finditer(test_text))
    
    valid_count = 0
    for i, match in enumerate(matches):
        eomi_str = match.group()
        context_score = get_eomi_context_score(test_text, eomi_str, match.start())
        is_valid = context_score >= 4.0
        
        if is_valid:
            valid_count += 1
        
        print(f"  {i+1}. '{eomi_str}' → 문맥 점수: {context_score:.2f} {'✅ 분할 가능' if is_valid else '❌ 분할 안 됨'}")
    
    print(f"\n총 {len(matches)}개 중 {valid_count}개가 4점 이상 (분할 가능)")
    print()

def main():
    """메인 테스트 실행"""
    print("🔍 종결어미 시스템 종합 테스트 (문맥 점수 기반)")
    print("=" * 60)
    print()
    
    try:
        test_eomi_detection()
        test_context_score()
        test_context_score_detailed()
        test_threshold_system()
        test_eomi_count()
        test_split_long_sentence()
        
        print("✅ 모든 테스트 완료!")
        
    except Exception as e:
        print(f"❌ 테스트 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
