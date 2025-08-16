# -*- coding: utf-8 -*-
"""
텍스트 처리 유틸리티
"""

import re
import regex  # 이모지 패턴을 위해 regex 모듈 추가
from typing import List, Dict, Any, Optional
from datetime import datetime

def extract_date_from_line(line: str) -> Optional[str]:
    """라인에서 날짜를 추출합니다."""
    pattern = r'---------------\s*(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일\s*\w+요일\s*---------------'
    match = re.search(pattern, line)
    if match:
        year, month, day = match.groups()
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    return None

def extract_time_from_line(line: str) -> Optional[str]:
    """라인에서 시간을 추출합니다."""
    pattern = r'\[(오전|오후)\s*(\d{1,2}):(\d{2})\]'
    match = re.search(pattern, line)
    if match:
        ampm, hour, minute = match.groups()
        hour = int(hour)
        if ampm == "오후" and hour != 12:
            hour += 12
        elif ampm == "오전" and hour == 12:
            hour = 0
        return f"{hour:02d}:{minute}"
    return None

def extract_sender_from_line(line: str) -> Optional[str]:
    """라인에서 발신자를 추출합니다."""
    pattern = r'\[([^\]]+)\]'
    match = re.search(pattern, line)
    if match:
        return match.group(1).strip()
    return None

def extract_message_content(line: str) -> str:
    """라인에서 메시지 내용을 추출합니다."""
    # 발신자와 시간 정보 제거
    content = re.sub(r'\[[^\]]+\]\s*\[[^\]]+\]\s*', '', line)
    return content.strip()

def convert_korean_date_to_iso(date_str: str, time_str: str) -> Optional[str]:
    """한국어 날짜와 시간을 ISO 형식으로 변환합니다."""
    if not date_str or not time_str:
        return None
    
    try:
        # 날짜 파싱
        date_parts = date_str.split('-')
        if len(date_parts) != 3:
            return None
        
        year, month, day = int(date_parts[0]), int(date_parts[1]), int(date_parts[2])
        
        # 시간 파싱
        time_parts = time_str.split(':')
        if len(time_parts) != 2:
            return None
        
        hour, minute = int(time_parts[0]), int(time_parts[1])
        
        # datetime 객체 생성
        dt = datetime(year, month, day, hour, minute)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    
    except (ValueError, TypeError):
        return None

# ===== 전처리 함수들 =====

def drop_deleted_messages(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """삭제된 메시지를 필터링합니다."""
    filtered_data = []
    
    for item in data:
        message = item.get('message', '')
        if not isinstance(message, str):
            continue
            
        message = message.strip()
        
        # "삭제된 메시지입니다." 체크
        if message != "삭제된 메시지입니다.":
            filtered_data.append(item)
        else:
            print(f"🗑️ 삭제: '{message}' (이유: 삭제된 메시지)")
    
    return filtered_data


def drop_noise_rows(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """노이즈 행들을 삭제합니다."""
    filtered_data = []
    
    for item in data:
        message = item.get('message', '')
        if not isinstance(message, str):
            continue
            
        message = message.strip()
        
        # 송금/환불/수취 관련 (포함하는 경우도 삭제)
        remit_regex = (
            r"(송금이\s*취소되었어요)"
            r"|((?:송금[:：]\s*)?[\d,]+원\s*(보냈어요|받기\s*완료!?|받았어요|자동\s*환불\s*예정))"
            r"|(송금봉투)"  # 송금봉투 추가
            r"|([\d,]+원\s*송금취소\s*완료)"  # {}원 송금취소 완료 추가
            r"|([\d,]+원을\s*보냈어요)"  # {}원을 보냈어요 추가
            r"|([\d,]+원을\s*받았어요)"  # {}원을 받았어요 추가
        )
        
        # 보이스톡/페이스톡 (정확히 일치하는 경우만 삭제)
        call_regex = r"^(보이스톡|페이스톡)\s*(해요|[0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?)\s*$"
        
        # 보이스톡이 포함된 메시지 (새로 추가)
        voice_call_regex = r"보이스톡"
        
        # 사진만 있는 행
        photo_only_regex = r"^사진(\s*\d+장)?$"
        
        # '파일:' 로 시작
        file_prefix_regex = r"^\s*파일\s*:"
        
        # '이모티콘'으로 된 행 (정확히 일치하는 경우)
        emoji_only_regex = r"^이모티콘$"
        
        # 이모지 문자만으로 이루어진 행 (새로 추가)
        emoji_chars_regex = regex.compile(r"^[\p{Extended_Pictographic}\s]+$")
        
        # 이모지가 포함된 행 (선택적 - 필요시 주석 해제)
        # emoji_contained_regex = regex.compile(r".*\p{Extended_Pictographic}.*")
        
        # 노이즈 패턴 체크
        is_remit = re.search(remit_regex, message)
        is_call = re.match(call_regex, message)
        is_voice_call = re.search(voice_call_regex, message)  # 새로 추가
        is_photo = re.match(photo_only_regex, message)
        is_file = re.match(file_prefix_regex, message)
        is_emoji_only = re.match(emoji_only_regex, message)
        is_emoji_chars = regex.match(emoji_chars_regex, message)
        
        is_noise = (is_remit or is_call or is_voice_call or is_photo or is_file or is_emoji_only or is_emoji_chars)
        
        # 디버깅: 삭제되는 메시지와 이유 출력
        if is_noise:
            reason = []
            if is_remit: reason.append("송금/환불")
            if is_call: reason.append("통화")
            if is_voice_call: reason.append("보이스톡")  # 새로 추가
            if is_photo: reason.append("사진")
            if is_file: reason.append("파일")
            if is_emoji_only: reason.append("이모티콘")
            if is_emoji_chars: reason.append("이모지문자")
            print(f"🗑️ 삭제: '{message[:30]}...' (이유: {', '.join(reason)})")
        else:
            filtered_data.append(item)
    
    return filtered_data

def remove_emojis(text: str) -> str:
    """이모티콘을 제거합니다."""
    if not isinstance(text, str):
        return text
    
    # regex 모듈 사용하여 정확한 이모티콘 제거
    # import regex as re  # 이미 상단에서 import됨
    
    # 이모지 기본 문자 + 변형 선택자(ufe0f/ufe0e) + ZWJ 연결 시퀀스까지 삭제
    emoji_pattern = regex.compile(
        r'(?:\p{Extended_Pictographic}(?:\uFE0F|\uFE0E)?(?:\u200D\p{Extended_Pictographic}(?:\uFE0F|\uFE0E)?)*)',
        regex.UNICODE
    )
    
    return emoji_pattern.sub('', text).strip()

def truncate_repeats(text: str) -> str:
    """반복 문자를 3개로 축약합니다."""
    if not isinstance(text, str):
        return text
    
    # 같은 문자 3회 이상 → 3회로
    return re.sub(r"(.)\1{2,}", r"\1\1\1", text, flags=re.DOTALL)

def drop_only_k_chars(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """ㅋ/ㄱ/ㅎ/ㅌ/ㅠ/큐로만 이루어진 행을 삭제합니다."""
    filtered_data = []
    
    for item in data:
        message = item.get('message', '')
        if not isinstance(message, str):
            continue
            
        message = message.strip()
        
        # ㅋ/ㄱ/ㅎ/ㅌ/ㅠ/큐 전용행 체크
        only_k_regex = r"^[ㅋㄱㅎㅌㅠㅜㅇ웅응엥앗음흠아와와헐헉오네넵옹후휴쿠큐\s]+$"
        
        if not re.fullmatch(only_k_regex, message):
            filtered_data.append(item)
    
    return filtered_data

def preprocess_messages(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """메시지들을 전처리합니다."""
    print("🧹 전처리 시작...")
    original_count = len(data)
    
    # 1. 시스템/노이즈 행 삭제
    print("  • 노이즈 행 삭제 중...")
    data = drop_noise_rows(data)
    
    # 2. 이모티콘 제거 → 비어있는 행 삭제
    print("  • 이모티콘 제거 중...")
    for item in data:
        if 'message' in item:
            item['message'] = remove_emojis(item['message'])
    
    # 비어있는 메시지 행 삭제
    data = [item for item in data if item.get('message', '').strip()]
    
    # 3. 반복 3개로 축약
    print("  • 반복 문자 축약 중...")
    for item in data:
        if 'message' in item:
            item['message'] = truncate_repeats(item['message'])
    
    # 4. ㅋ/ㄱ/ㅎ/ㅌ/ㅠ/큐 전용행 삭제
    print("  • 의미없는 문자 행 삭제 중...")
    data = drop_only_k_chars(data)
    
    # 5. 삭제된 메시지 필터링
    print("  • 삭제된 메시지 필터링 중...")
    data = drop_deleted_messages(data)
    
    final_count = len(data)
    print(f"✅ 전처리 완료: {original_count}개 → {final_count}개 메시지")
    
    return data
