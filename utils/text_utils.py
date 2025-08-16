# -*- coding: utf-8 -*-
"""
텍스트 처리 유틸리티
"""

import re
import regex  # 이모지 패턴을 위해 regex 모듈 추가
from typing import List, Dict, Any, Optional
from datetime import datetime

# 의미 없는 메시지 패턴 리스트
REMOVE_LIST = [
    # ── 인사 / 작별 ──
    r"안녕", r"하이", r"헬로", r"하위", r"하이루", r"ㅎㅇ", r"방가", r"안뇽", r"안농",
    r"안녕하세요", r"안녕하세염", r"안녕하세욤", r"안녕하세용",
    r"ㅎㅇㄹ", r"반갑", r"반갑습니다", r"반갑다",r"반갑고" r"어서오고", r"어서오세요",
    r"잘자", r"굿밤", r"잘자용", r"굿나잇", r"잘가", r"수고", r"수고링", r"수고용",
    r"내일\s?봐", r"낼봐", r"담에봐", r"또봐", r"이따봐", r"나중에\s?봐",
    r"빠이", r"ㅂㅂ", r"ㅃㅃ", r"바이", r"ㅂㅇ", r"ㅂ2", r"bye", r"즐",
    r"먼저\s?갈게", r"먼저\s?간다", r"들어가세요", r"들갑니다", r"수고하세요",

    # ── 맞장구 / 동의 / 되묻기 ──
    r"^[ㅇ응웅엉넵네넹]+$",
    r"엉", r"어", r"아", r"응", r"어\?", r"아\?", r"응\?",r"잉",
    r"콜", r"ㄱㄱ+", r"고고", r"gogo", r"go", r"ㄱㄱㅅ",
    r"굿", r"굳", r"조아", r"좋아", r"좋아요", r"좋다", r"좋지", r"나이스",
    r"마자", r"맞아", r"맞음", r"ㅇㅈ", r"인정", r"ㅆㅇㅈ", r"쌉인정",
    r"ㄹㅇ", r"레알", r"진심", r"ㅇㅈㄸㅇㅈ",
    r"ㅇㄱㄹㅇ", r"ㅂㅂㅂㄱ",
    r"내말이", r"내말이\s?그말", r"그니까", r"그러니까",
    r"오키", r"오케이", r"ㅇㅋ", r"ok", r"OK", r"ㅇㅋㅇㅋ", r"ㅇㅋㄷㅋ",
    r"당근", r"당근이지", r"그래", r"그래서", r"그럼", r"그럼그럼", r"글치", r"그치", r"당연하지",
    r"쌉가능", r"쌉파서블", r"가능",
    r"아하", r"오호", r"이해했음", r"알겠음", r"알았다", r"접수",
    r"그건맞지", r"그건\s?인정", r"팩트", r"ㄹㅇㅋㅋ",
    r"진짜", r"정말", r"리얼", r"뭐", r"어디", r"누구", r"언제", r"어케", r"어쩐지", r"어쩔",

    # ── 웃음소리 ──
    r"^[ㅋㅎㅠㅜ풉키]+$", r"ㅋㅋ+", r"ㅎㅎ+", r"ㅋㄷㅋㄷ", r"킥킥", r"크크",
    r"풉", r"푸핫", r"푸흡", r"피식", r"빵터짐", r"현웃",
    r"육성으로\s?터짐", r"육성\s?터짐", r"ㅋㄹㅃㅃ",

    # ── 감탄사 / 감정표현 ──
    r"ㅠㅠ+", r"ㅜㅜ+", r"흑", r"흐규+", r"엉엉", r"맴찢", r"눈물",
    r"헐", r"헉", r"허걱", r"헐랭", r"헐퀴", r"띠용", r"어머나", r"세상에",
    r"와우", r"오우", r"오예", r"아싸", r"오예스", r"유레카",
    r"쩐다", r"대박", r"오졌다", r"지렸다", r"미쳤다", r"미쳤네",
    r"레전드", r"레게노", r"폼\s?미쳤다", r"그저\s?빛",
    r"가슴이\s?웅장해진다", r"이왜진",
    r"아이구", r"아이고", r"어머", r"오마이갓", r"맙소사",
    r"머선129", r"오히려\s?좋아", r"가보자고",
    r"아놔", r"아오", r"ㅡㅡ", r"ㅂㄷㅂㄷ", r"킹받네", r"딥빡", r"빡침", r"황당", r"어이없네",
    r"존맛", r"존잼", r"JMT",

    # ── 기타 불필요 토막 ──
    r"ㄴㄴ", r"노노", r"아냐", r"아님", r"아니", r"아니거든", r"놉", r"절대", r"전혀", r"응\s?아니야",
    r"걍", r"그냥", r"왤케", r"어케", r"솔까말",
    r"ㅈㄱㄴ", r"TMI", r"갑분싸", r"알빠노", r"누물보",
    r"어쩔티비", r"저쩔티비", r"뇌절",
    r"ㄱㅅ", r"감사", r"ㄳ", r"땡큐", r"감사링", r"고맙다", r"고마워",
    r"ㅈㅅ", r"죄송", r"미안", r"쏘리",
    r"ㅊㅋ", r"축하",
    r"잠만", r"잠시만", r"잠깐만", r"ㄱㄷ", r"기다려봐", r"잠시",
    r"몰라", r"모름", r"글쎄", r"암튼", r"아무튼", r"일단",
    r"음", r"흠", r"뭔가", r"딱히", r"혹시", r"근데", r"저기", r"있잖아", r"뭐랄까", r"약간",
    r"GG", r"gg", r"GGWP", r"ㅈㅈ", r"서렌", r"트롤", r"즐겜"
]

def clean_message(text: str, min_tail: int = 7) -> str | None:
    """
    규칙:
    1) REMOVE_LIST 패턴이 있으면 패턴만 제거
    2) 단, 패턴 제거 후 남은 문자열이 min_tail 이하라면 전체 문장 삭제
    3) 매칭이 없으면 원문 반환
    4) 모든 패턴을 반복적으로 제거 (한 번에 하나씩)
    """
    new_text = text
    changed = True
    
    # 패턴이 더 이상 제거되지 않을 때까지 반복
    while changed:
        changed = False
        for pattern in REMOVE_LIST:
            match = re.search(pattern, new_text)
            if match:
                start, end = match.span()
                candidate = (new_text[:start] + new_text[end:]).strip()
                # 남은 게 너무 짧으면 None 반환
                if len(candidate) <= min_tail:
                    return None
                new_text = candidate
                changed = True
                break  # 패턴을 찾았으면 다시 처음부터 검사
    
    return new_text if new_text.strip() else None


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
        return f"{hour:02d}:{minute}"
    return None

def extract_user_from_line(line: str) -> Optional[str]:
    """라인에서 사용자명을 추출합니다."""
    pattern = r'\[([^\]]+)\]'
    match = re.search(pattern, line)
    if match:
        return match.group(1)
    return None

def extract_sender_from_line(line: str) -> Optional[str]:
    """라인에서 발신자를 추출합니다. (extract_user_from_line과 동일)"""
    return extract_user_from_line(line)

def extract_message_from_line(line: str) -> Optional[str]:
    """라인에서 메시지 내용을 추출합니다."""
    # 사용자명과 시간 부분을 제거
    pattern = r'^\[[^\]]+\]\s*\[[^\]]+\]\s*(.+)$'
    match = re.search(pattern, line)
    if match:
        return match.group(1).strip()
    return None

def extract_message_content(line: str) -> str:
    """라인에서 메시지 내용을 추출합니다. (extract_message_from_line과 동일)"""
    result = extract_message_from_line(line)
    return result if result else ""

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
    
    # 6. 의미 없는 메시지 필터링 (새로운 로직)
    print("  • 의미 없는 메시지 필터링 중...")
    filtered_data = []
    for item in data:
        message = item.get('message', '')
        cleaned_message = clean_message(message, min_tail=7)
        if cleaned_message is not None:
            # 메시지가 정리된 경우 업데이트
            item['message'] = cleaned_message
            filtered_data.append(item)
        # None인 경우 (삭제 대상)는 제외
    
    data = filtered_data
    
    final_count = len(data)
    print(f"✅ 전처리 완료: {original_count}개 → {final_count}개 메시지")
    
    return data


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

