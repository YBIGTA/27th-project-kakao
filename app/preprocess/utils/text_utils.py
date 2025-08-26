# -*- coding: utf-8 -*-
"""
텍스트 처리 유틸리티
"""

import re
import regex  # 이모지 패턴을 위해 regex 모듈 추가
from typing import List, Dict, Any, Optional
from datetime import datetime
from .filter_non_korean import (
    filter_dataframe_by_korean_ratio,
    get_filtering_stats,
    should_drop_row
)
from .korean_eomi_dict import END_EOMI_SPLIT_RE, get_eomi_context_score

# 감정표현과 감정표현 강도만으로 이루어진 행을 찾기 위한 패턴들
EMOTION_ONLY_PATTERNS = [
    # 감정표현 단어들
    r"\b좋아\b", r"\b좋아요\b", r"\b좋다\b", r"\b좋지\b", r"\b나이스\b", r"\b굿\b", r"\b굳\b", r"\b조아\b",
    r"\b대박\b", r"\b쩐다\b", r"\b오졌다\b", r"\b지렸다\b", r"\b미쳤다\b", r"\b미쳤네\b",
    r"\b레전드\b", r"\b레게노\b", r"\b존맛\b", r"\b존잼\b", r"JMT",
    r"\b헐\b", r"\b헉\b", r"\b허걱\b", r"\b헐랭\b", r"\b헐퀴\b", r"\b띠용\b",
    r"\b와우\b", r"\b오우\b", r"\b오예\b", r"\b아싸\b", r"\b오예스\b", r"\b유레카\b",
    r"\b아이구\b", r"\b아이고\b", r"\b어머\b", r"\b어머나\b", r"\b세상에\b",
    r"\b아놔\b", r"\b아오\b", r"\b킹받네\b", r"\b딥빡\b", r"\b빡침\b", r"\b황당\b", r"\b어이없네\b",
    
    # 감정표현 강도 (웃음소리, 감탄사 등)
    r"ㅋㅋ+", r"ㅎㅎ+", r"ㅠㅠ+", r"ㅜㅜ+", r"ㅡㅡ", r"ㅂㄷㅂㄷ",
    r"\b풉\b", r"\b푸핫\b", r"\b푸흡\b", r"\b피식\b", r"\b빵터짐\b", r"\b현웃\b",
    r"\b흑\b", r"\b흐규+\b", r"\b엉엉\b", r"\b맴찢\b", r"\b눈물\b",
    
    # 맞장구/동의 표현들
    r"\b엉\b", r"\b어\b", r"\b아\b", r"\b응\b", r"\b잉\b", r"\b오\b", r"\b엥\b"
    r"\b콜\b", r"\b고고\b", r"\b마자\b", r"\b맞아\b", r"\b맞음\b", r"\b인정\b",
    r"\b오키\b", r"\b오케이\b", r"\b그래\b", r"\b그래서\b", r"\b그럼\b", r"\b그치\b",
    r"\b당근\b", r"\b당근이지\b", r"\b당연하지\b", r"\b아하\b", r"\b오호\b",
    r"\b진짜\b", r"\b정말\b", r"\b리얼\b", r"\b레알\b", r"\b진심\b", r"\b너무\b",
    
    # 기타 감정표현
    r"\bㅇㅈ\b", r"\bㄹㅇ\b", r"\bㅆㅇㅈ\b", r"\b쌉인정\b", r"\b쌉가능\b", r"\b쌉파서블\b",
    r"\b팩트\b", r"\b가능\b", r"\b이해했음\b", r"\b알겠음\b", r"\b알았다\b", r"\b접수\b",
    r"\bㅇㅋ\b", r"\bㅇㅋㅇㅋ\b", r"\bㅇㅋㄷㅋ\b", r"\bok\b", r"\bOK\b",
    
    # 감탄사/의문사
    r"\b뭐\b", r"\b어디\b", r"\b누구\b", r"\b언제\b", r"\b어케\b", r"\b어쩐지\b", r"\b어쩔\b",
    r"\b음\b", r"\b흠\b", r"\b뭔가\b", r"\b딱히\b", r"\b혹시\b", r"\b근데\b", r"\b저기\b",
    r"\b있잖아\b", r"\b뭐랄까\b", r"\b약간\b", r"\b걍\b", r"\b그냥\b", r"\b왤케\b", r"\b솔까말\b",
    
    # 부정/거부 표현
    r"\b노노\b", r"\b아냐\b", r"\b아님\b", r"\b아니\b", r"\b아니거든\b", r"\b놉\b", r"\b절대\b", r"\b전혀\b",
    r"\b몰라\b", r"\b모름\b", r"\b글쎄\b", r"\b암튼\b", r"\b아무튼\b", r"\b일단\b",
    
    # 인사/작별 표현
    r"\b안녕\b", r"\b하이\b", r"\b헬로\b", r"\b하위\b", r"\b하이루\b", r"\b방가\b", r"\b안뇽\b", r"\b안농\b",
    r"\b안녕하세요\b", r"\b안녕하세염\b", r"\b안녕하세욤\b", r"\b안녕하세용\b",
    r"\b반갑\b", r"\b반갑습니다\b", r"\b반갑다\b", r"\b반갑고\b", r"\b어서오고\b", r"\b어서오세요\b",
    r"\b잘자\b", r"\b굿밤\b", r"\b잘자용\b", r"\b굿나잇\b", r"\b잘가\b", r"\b수고\b", r"\b수고링\b", r"\b수고용\b",
    r"\b빠이\b", r"\b바이\b", r"\bbye\b", r"\b즐\b",
    
    # 감사/사과 표현
    r"\b감사\b", r"\b땡큐\b", r"\b감사링\b", r"\b고맙다\b", r"\b고마워\b", r"\b고마워요\b",
    r"\b죄송\b", r"\b미안\b", r"\b쏘리\b", r"\b축하\b",
    
    # 기타
    r"\b잠만\b", r"\b잠시만\b", r"\b잠깐만\b", r"\b기다려봐\b", r"\b잠시\b",
    r"\bTMI\b", r"\b갑분싸\b", r"\b알빠노\b", r"\b누물보\b", r"\b어쩔티비\b", r"\b저쩔티비\b", r"\b뇌절\b",
    r"\bGG\b", r"\bgg\b", r"\bGGWP\b", r"\b서렌\b", r"\b트롤\b", r"\b즐겜\b",
    
    # 지시대명사들 (의미없는 조합일 때만 전체 삭제)
    r"\b이거\b", r"\b저거\b", r"\b그거\b", r"\b요거\b", r"\b조거\b", r"\b고거\b",
    r"\b이게\b", r"\b저게\b", r"\b그게\b", r"\b요게\b", r"\b조게\b", r"\b고게\b",
    r"\b이런\b", r"\b저런\b", r"\b그런\b", r"\b요런\b", r"\b조런\b", r"\b고런\b",
    r"\b이런거\b", r"\b저런거\b", r"\b그런거\b", r"\b요런거\b", r"\b조런거\b", r"\b고런거\b",
    r"\b여기\b", r"\b거기\b", r"\b저기\b", r"\b요기\b", r"\b조기\b", r"\b고기\b",
    r"\b이쪽\b", r"\b저쪽\b", r"\b그쪽\b", r"\b요쪽\b", r"\b조쪽\b", r"\b고쪽\b",
    r"\b이때\b", r"\b저때\b", r"\b그때\b", r"\b요때\b", r"\b조때\b", r"\b고때\b",
    r"\b이제\b", r"\b저제\b", r"\b그제\b", r"\b요제\b", r"\b조제\b", r"\b고제\b"
]

# 순수 맞장구/감탄사 패턴 리스트 (의미 있는 감정표현은 제외)
REMOVE_LIST = [
    # ── 맞장구 / 동의 / 되묻기 (순수 맞장구만) ──
    r"^[ㅇ응웅엉넵네넹]+$",
    r"\b엉\b", r"\b어\b", r"\b아\b", r"\b응\b", r"\b어\?\b", r"\b아\?\b", r"\b응\?\b", r"\b잉\b", r"\b오\b", r"\b엥\b", r"\b웅\b", r"\b우웅\b", 
    r"\b콜\b", r"ㄱㄱ+", r"\b고고\b", r"\bgogo\b", r"\bgo\b", r"ㄱㄱㅅ",
    r"\b마자\b", r"\b맞아\b", r"\b맞음\b", r"ㅇㅈ", r"\b인정\b", r"ㅆㅇㅈ", r"\b쌉인정\b",
    r"ㄹㅇ", r"\b레알\b", r"ㅇㅈㄸㅇㅈ", r"ㅇㄱㄹㅇ", r"ㅂㅂㅂㄱ",
    r"\b내말이\b", r"내말이\s?그말", r"\b그니까\b", r"\b그러니까\b",
    r"오키", r"오케이", r"옹키", r"오키도키", r"ㅇㅋ", r"\bok\b", r"\bOK\b", r"ㅇㅋㅇㅋ", r"ㅇㅋㄷㅋ",
    r"\b당근\b", r"\b당근이지\b", r"\b그래\b", r"\b그럼그럼\b", r"\b글치\b", r"\b그치\b", r"\b당연하지\b",
    r"\b아하\b", r"앗하", r"\b오호\b", r"\b이해했음\b", r"\b알겠음\b", r"\b알았다\b", r"\b접수\b",
    r"\b그건맞지\b", r"그건\s?인정", r"\b팩트\b", r"ㄹㅇㅋㅋ",

    # ── 웃음소리 ──
    r"^[ㅋㅎㅠㅜ풉키]+$", r"ㅋㅋ+", r"ㅎㅎ+", r"ㅋㄷㅋㄷ", r"킥킥", r"크크",
    r"풉", r"푸핫", r"푸흡", r"피식", r"빵터짐", r"현웃",
    r"육성으로\s?터짐", r"육성\s?터짐", r"ㅋㄹㅃㅃ",

    # ── 순수 감탄사 (의미가 없는 것들만) ──
    r"ㅠㅠ+", r"ㅜㅜ+", r"흑", r"흐규+", r"엉엉", r"ㅡㅡ", r"ㅂㄷㅂㄷ",
    r"헐", r"헉", r"허걱", r"헐랭", r"헐퀴", r"띠용", r"어머나", r"세상에",
    r"아이구", r"아이고", r"어머", r"오마이갓", r"맙소사",
    r"머선129", r"가보자고", r"아놔", r"아오", r"이왜진",

    # ── 기타 불필요 토막 (의미없는 것들만) ──
    r"ㄴㄴ", r"\b노노\b", r"\b아냐\b", r"\b아님\b", r"\b아니\b", r"\b아니거든\b", r"\b놉\b", r"응\s?아니야",
    r"\b걍\b", r"\b그냥\b", r"\b왤케\b", r"\b어케\b", r"\b솔까말\b",
    r"ㅈㄱㄴ", r"TMI", r"\b갑분싸\b", r"\b알빠노\b", r"\b누물보\b",
    r"\b어쩔티비\b", r"\b저쩔티비\b", r"\b뇌절\b",
    r"ㄱㅅ", r"ㄳ", r"ㅈㅅ", r"ㅊㅋ", r"ㄱㄷ",
    r"\b잠만\b", r"\b잠시만\b", r"\b잠깐만\b", r"\b기다려봐\b", r"\b잠시\b",
    r"\b몰라\b", r"\b모름\b", r"\b글쎄\b", r"\b암튼\b", r"\b아무튼\b", r"\b일단\b",
    r"\b음\b", r"\b흠\b", r"\b딱히\b", r"\b혹시\b", r"\b저기\b", r"\b있잖아\b", r"\b뭐랄까\b",
    r"\bGG\b", r"\bgg\b", r"\bGGWP\b", r"ㅈㅈ", r"서렌", r"트롤", r"즐겜"
]

# 연결어류는 문두/문말에서만 정리(중간 등장 시 의미 보존)
EDGE_FILLERS = [r"그래서", r"근데", r"그러니까", r"그니까", r"그리고", r"그럼"]

def strip_edge_fillers(s: str) -> str:
    # 문두
    s = re.sub(rf"^\s*(?:{'|'.join(EDGE_FILLERS)})\s*[,\.…]*\s*", "", s)
    # 문말
    s = re.sub(rf"\s*[,\.…]*\s*(?:{'|'.join(EDGE_FILLERS)})\s*$", "", s)
    return s.strip()

def is_emotion_only_message(text: str) -> bool:
    """
    텍스트가 EMOTION_ONLY_PATTERNS와 REMOVE_LIST에 있는 단어들로만 이루어져 있는지 확인합니다.
    
    Args:
        text (str): 확인할 텍스트
        
    Returns:
        bool: 감정표현/불필요한 단어들로만 이루어져 있으면 True, 의미 있는 내용이 포함되어 있으면 False
    """
    if not text or not text.strip():
        return True
    
    # 모든 패턴을 하나로 합치기
    all_patterns = EMOTION_ONLY_PATTERNS + REMOVE_LIST
    
    # 텍스트를 단어로 분리 (한글, 영문, 숫자, 이모티콘 등 포함)
    words = re.findall(r'[가-힣a-zA-Z0-9ㅋㅎㅠㅜㅏㅓㅗㅡㅣ]+|[^\w\s]+', text.strip())
    
    if not words:
        return True
    
    # 모든 단어가 패턴에 매칭되는지 확인
    for word in words:
        # 문장부호만으로 된 토큰은 의미 없음 → 스킵
        if re.fullmatch(r'[^\w\s]+', word):
            continue
        
        is_matched = False
        
        # 각 패턴과 매칭 확인
        for pattern in all_patterns:
            if re.search(pattern, word):
                is_matched = True
                break
        
        # 순수 한글 감정 문자들 (ㅋㅋㅋ, ㅎㅎㅎ 등)
        if not is_matched and re.match(r'^[ㅋㅎㅠㅜㅏㅓㅗㅡㅣ]+$', word):
            is_matched = True
        
        # 하나라도 매칭되지 않으면 의미 있는 내용 포함
        if not is_matched:
            return False
    
    # 모든 단어가 매칭되면 감정표현/불필요한 단어로만 구성
    return True


def clean_message(text: str, min_tail: int = 7) -> str | None:
    """
    규칙:
    1) 감정표현만으로 이루어진 메시지는 전체 삭제
    2) REMOVE_LIST 패턴이 있으면 패턴만 제거
    3) 단, 패턴 제거 후 남은 문자열이 min_tail 이하라면 전체 문장 삭제
    4) 매칭이 없으면 원문 반환
    5) 모든 패턴을 반복적으로 제거 (한 번에 하나씩)
    """
        # 감정표현만으로 이루어진 메시지인지 먼저 확인
    if is_emotion_only_message(text):
        return None

    # 문두/문말 연결어만 정리(중간 정보는 보존)
    new_text = strip_edge_fillers(text)
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

    # 최종적으로 한 번 더 엣지 트리밍
    new_text = strip_edge_fillers(new_text)

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
        only_k_regex = r"^[ㅋㄱㅎㅌㅠㅜㅇㅗㄴ웅응엥앗음흠아와와헐헉오네넵옹후휴쿠큐.,!?;\s]+$"
        
        if not re.fullmatch(only_k_regex, message):
            filtered_data.append(item)
    
    return filtered_data

def drop_long_text_with_few_eomi(data: List[Dict[str, Any]], 
                                min_length: int = 200, 
                                min_eomi_count: int = 5,
                                keep_short: bool = True) -> List[Dict[str, Any]]:
    """
    긴 텍스트이면서 종결어미가 적은 행을 삭제합니다.
    
    Args:
        data: 메시지 데이터 리스트
        min_length: 이 길이 이상이면서 종결어미가 적으면 삭제
        min_eomi_count: 이 개수 미만의 종결어미를 가진 긴 텍스트는 삭제
        keep_short: True면 짧은 텍스트는 종결어미 개수와 관계없이 보존
    
    Returns:
        필터링된 데이터
    """
    filtered_data = []
    dropped_count = 0
    
    for item in data:
        message = item.get('message', '')
        if not isinstance(message, str):
            filtered_data.append(item)
            continue
            
        message = message.strip()
        message_length = len(message)
        
        # 짧은 텍스트는 보존 (keep_short=True인 경우)
        if keep_short and message_length < min_length:
            filtered_data.append(item)
            continue
        
        # 긴 텍스트인 경우 종결어미 개수 확인
        if message_length >= min_length:
            # 종결어미 개수 세기 (문맥 점수 5점 이상인 경우만)
            eomi_count = 0
            for match in END_EOMI_SPLIT_RE.finditer(message):
                eomi_str = match.group()
                
                # 문맥 점수 계산 (정확한 위치 정보 전달)
                context_score = get_eomi_context_score(message, eomi_str, match.start())
                
                # 문맥 점수가 5점 이상인 경우만 고려
                if context_score >= 5.0:
                    eomi_count += 1
            
            if eomi_count >= min_eomi_count:
                # 종결어미가 충분히 있으면 보존
                filtered_data.append(item)
            else:
                # 종결어미가 적으면 삭제
                dropped_count += 1
                print(f"🗑️ 종결어미 적은 긴 텍스트 삭제: '{message[:50]}...' (길이: {message_length}, 종결어미: {eomi_count}개)")
        else:
            # 짧은 텍스트는 보존
            filtered_data.append(item)
    
    if dropped_count > 0:
        print(f"  • 종결어미 적은 긴 텍스트 {dropped_count}개 삭제됨")
    
    return filtered_data


def preprocess_messages(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """메시지들을 전처리합니다. (clean_message 제외)"""
    print("🧹 기본 전처리 시작...")
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
    
    # 6. 비한국어 내용 필터링 (프로그래밍 코드, 터미널 로그 등)
    print("  • 비한국어 내용 필터링 중...")
    filtered_data = []
    dropped_count = 0
    
    for item in data:
        message = item.get('message', '')
        if should_drop_row(message, min_chars=200, nonko_threshold=0.60, 
                          special_threshold=0.12, keep_plain_english=True):
            dropped_count += 1
            print(f"🗑️ 비한국어 삭제: '{message[:50]}...'")
        else:
            filtered_data.append(item)
    
    data = filtered_data
    
    if dropped_count > 0:
        print(f"  • 비한국어 내용 {dropped_count}개 삭제됨")
    
    # 7. 종결어미 적은 긴 텍스트 필터링
    print("  • 종결어미 적은 긴 텍스트 필터링 중...")
    data = drop_long_text_with_few_eomi(data, min_length=200, min_eomi_count=5)
    
    final_count = len(data)
    print(f"✅ 기본 전처리 완료: {original_count}개 → {final_count}개 메시지")
    
    return data


def clean_emotion_messages(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """감정표현/의미 없는 메시지 필터링 (SBD 이후 실행)"""
    print("🧹 감정표현 메시지 필터링 중...")
    original_count = len(data)
    
    filtered_data = []
    dropped_count = 0
    
    for item in data:
        message = item.get('message', '')
        cleaned_message = clean_message(message, min_tail=7)
        
        if cleaned_message is not None:
            # 메시지가 정리된 경우 업데이트
            if cleaned_message != message:
                print(f"🧹 정리: '{message[:50]}...' → '{cleaned_message[:50]}...'")
            item['message'] = cleaned_message
            filtered_data.append(item)
        else:
            # None인 경우 (삭제 대상)는 제외
            dropped_count += 1
            print(f"🗑️ 감정표현 삭제: '{message[:50]}...'")
    
    final_count = len(filtered_data)
    print(f"✅ 감정표현 필터링 완료: {original_count}개 → {final_count}개 메시지")
    
    if dropped_count > 0:
        print(f"  • 감정표현 메시지 {dropped_count}개 삭제됨")
    
    return filtered_data


def drop_short_messages(data: List[Dict[str, Any]], min_length: int = 4) -> List[Dict[str, Any]]:
    """짧은 메시지를 삭제합니다."""
    print("  • 짧은 메시지 삭제 중...")
    original_count = len(data)
    
    filtered_data = []
    dropped_count = 0
    
    for item in data:
        message = item.get('message', '')
        if not isinstance(message, str):
            filtered_data.append(item)
            continue
            
        message = message.strip()
        message_length = len(message)
        
        if message_length > min_length:
            filtered_data.append(item)
        else:
            dropped_count += 1
            print(f"🗑️ 짧은 메시지 삭제: '{message}' (길이: {message_length}자)")
    
    final_count = len(filtered_data)
    if dropped_count > 0:
        print(f"  • 짧은 메시지 {dropped_count}개 삭제됨")
    
    return filtered_data


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

