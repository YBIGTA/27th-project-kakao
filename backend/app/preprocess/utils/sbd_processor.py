# -*- coding: utf-8 -*-
"""
SBD(Sentence Boundary Detection) 기반 문장 병합 프로세스
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict, Any
import regex as re
from datetime import datetime
from .korean_eomi_dict import (
    END_EOMI_RE, 
    CONT_EOMI_RE, 
    PARTICLE_END_RE,
    END_EOMI_SPLIT_RE,
    get_eomi_context_score
)

# --------- 백채널(추임새/이모티콘/한글자 감탄) 판정 ----------
# 길이/패턴을 함께 보며 과태깅을 줄임
BACKCHANNEL_RE = re.compile(
    r"^(?:"
    r"(웅|응|엉|어|아|오|헉|헐|앗|와|ㅋ+|ㅎ+|ㅠ+|ㅜ+|ㅇㅇ|ㅇㅋ|넵+|네+)"
    r")$"
)


def is_backchannel(text: str) -> bool:
    if not text:
        return False
    t = text.strip()
    # 긴 문장은 배제 (과태깅 방지)
    if len(t) > 6:  # 글자 수 기준, 필요시 8~10으로 늘려도 됨
        return False
    # 공백/기호 제거 후 재확인
    t2 = re.sub(r"[\p{P}\p{S}\s]+", "", t)  # 구두점/기호/공백 제거
    return bool(BACKCHANNEL_RE.fullmatch(t2))


# --------- 이모지 제거(Extended Pictographic + ZWJ 시퀀스) ----------
EMOJI_SEQ = re.compile(
    r'(?:\p{Extended_Pictographic}(?:\uFE0F|\uFE0E)?'
    r'(?:\u200D\p{Extended_Pictographic}(?:\uFE0F|\uFE0E)?)*)'
)


def strip_emojis(s: str) -> str:
    return EMOJI_SEQ.sub('', s).strip() if isinstance(s, str) else s


# --------- 규칙 패턴들 ----------
END_PUNCT_RE = re.compile(r"[.?!]+$")                     # 강한 종결부호
ELLIPSIS_RE = re.compile(r"(…|\.{3,}|~{2,})$")           # 말줄임/물결로 끝
LAUGHTER_END = re.compile(r"(ㅋ|ㅎ)+$")                    # ㅋㅋ, ㅎㅎ 등으로 끝

# 다음 문장 시작이 접속부사인 경우(계속 신호)
NEXT_CONNECTIVE_RE = re.compile(
    r"^(그리고|근데|그래서|그런데|그때|그다음|또|게다가|하지만|"
    r"그러다가|그러면|그러니까)\b"
)


@dataclass
class SBDConfig:
    # 턴 묶기/간격 기준
    t_merge_seconds: int = 60   # 동일 화자 turn 묶음 기준
    t_gap_seconds: int = 25     # SBD에서 '긴 간격'으로 간주하는 임계

    # 점수 가중치 (종결 +, 계속 -)
    w_end_punct: int = 2
    w_end_eomi: int = 2
    w_laughter_end: int = 1
    w_ellipsis_end: int = 1
    w_long_gap: int = 1
    w_speaker_change: int = 1   # turn 경계 외에는 사용 빈도 낮음

    w_cont_eomi: int = -2
    w_particle_end: int = -2
    w_next_connective: int = -1
    w_next_backchannel: int = -1

    # 새로운 설정
    theta: int = 3  # 임계치. 점수 >= theta이면 '끊기'
    max_sentence_length: int = 200  # 문장이 이 길이를 넘으면 강제 분할 고려
    max_eomi_count: int = 4  # 한 문장에 종결어미가 이 개수를 넘으면 분할

# --------- 화자-시간 윈도우로 턴 묶기 ----------
def group_turns(data: List[Dict[str, Any]], cfg: SBDConfig) -> List[Dict[str, Any]]:
    """데이터를 화자-시간 윈도우로 턴을 묶습니다."""
    # 원본 데이터 복사본 생성
    data_copy = []
    for item in data:
        item_copy = item.copy()
        if 'date' in item_copy and isinstance(item_copy['date'], str):
            try:
                item_copy['datetime'] = datetime.fromisoformat(item_copy['date'].replace('Z', '+00:00'))
            except:
                item_copy['datetime'] = None
        data_copy.append(item_copy)
    
    # 시간순 정렬
    data_copy = sorted(data_copy, key=lambda x: x.get('datetime') or datetime.min)
    
    cur_turn = -1
    prev_speaker = None
    prev_ts = None

    for item in data_copy:
        spk = item.get('user', '')
        ts = item.get('datetime')
        
        if prev_speaker is None:
            cur_turn = 0
        else:
            same_speaker = (spk == prev_speaker)
            if ts and prev_ts:
                gap_ok = (ts - prev_ts).total_seconds() <= cfg.t_merge_seconds
            else:
                gap_ok = False
                
            if not (same_speaker and gap_ok):
                cur_turn += 1
                
        item['turn_id'] = cur_turn
        prev_speaker, prev_ts = spk, ts

    return data_copy

# --------- 경계 점수 계산 ----------
def boundary_score(prev_text: str,
                   next_text: str,
                   delta_t: float,
                   speaker_changed: bool,
                   cfg: SBDConfig) -> float:
    score = 0.0
    pt = prev_text.strip() if isinstance(prev_text, str) else ""
    nt = next_text.strip() if isinstance(next_text, str) else ""

    # --- 종결 신호 (+) ---
    if END_PUNCT_RE.search(pt):
        score += cfg.w_end_punct
    if END_EOMI_RE.search(pt):
        # 종결어미 문맥 점수를 고려 (end_anchor=True이므로 문장 끝만 검사)
        for match in END_EOMI_SPLIT_RE.finditer(pt):
            eomi_str = match.group()
            # 문장 끝의 종결어미만 고려
            context_score = get_eomi_context_score(pt, eomi_str)
            score += cfg.w_end_eomi * context_score
            break  # 첫 번째 종결어미만 사용
    if LAUGHTER_END.search(pt):
        score += cfg.w_laughter_end
    if ELLIPSIS_RE.search(pt):
        score += cfg.w_ellipsis_end
    if delta_t is not None and delta_t > cfg.t_gap_seconds:
        score += cfg.w_long_gap
    if speaker_changed:
        score += cfg.w_speaker_change

    # --- 계속 신호 (-) ---
    if CONT_EOMI_RE.search(pt):
        score += cfg.w_cont_eomi
    if PARTICLE_END_RE.search(pt):
        score += cfg.w_particle_end
    if NEXT_CONNECTIVE_RE.search(nt):
        score += cfg.w_next_connective
    if is_backchannel(nt):
        score += cfg.w_next_backchannel

    return score

# --------- 턴 내부 SBD + 의미 단위 병합 ----------
def merge_within_turn(messages: List[Tuple[datetime, str]], cfg: SBDConfig) -> List[str]:
    """
    messages: [(ts, text)] for a single turn (same speaker, time-sorted)
    return: merged sentences (list)
    """
    if not messages:
        return []

    out: List[str] = []
    buf = messages[0][1].strip() if messages[0][1] else ""
    prev_ts = messages[0][0]

    for i in range(1, len(messages)):
        cur_ts, cur_text = messages[i]
        prev_text = buf.split()[-1] and buf or ""  # 전체 버퍼 내용
        next_text = cur_text.strip()

        delta_t = (cur_ts - prev_ts).total_seconds() if (cur_ts and prev_ts) else None
        # turn 내부는 동일 화자이므로 speaker_changed=False
        s = boundary_score(prev_text, next_text, delta_t, False, cfg)

        # 강제 분할 조건 확인
        should_force_split = False
        
        # 1. 문장 길이가 너무 길어질 경우
        if len(buf + next_text) > cfg.max_sentence_length:
            should_force_split = True
        
        # 2. 종결어미 개수가 너무 많아질 경우
        if should_force_split or count_eomi_in_text(buf + next_text) > cfg.max_eomi_count:
            should_force_split = True

        if s >= cfg.theta or should_force_split:
            # 끊기
            if buf:
                # 강제 분할이 필요한 경우 종결어미 기준으로 세분화
                if should_force_split and len(buf) > cfg.max_sentence_length:
                    split_sentences = split_long_sentence(buf, cfg)
                    out.extend(split_sentences)
                else:
                    out.append(buf.strip())
            buf = next_text
        else:
            # 이어붙이기
            sep = ", "
            if buf.endswith((".", "?", "!", "…")) or buf.endswith(("'", "'")):
                sep = " "
            if next_text.startswith((",", ".", "?", "!", "…")):
                sep = ""
            buf = f"{buf}{sep}{next_text}"

        prev_ts = cur_ts

    if buf:
        # 마지막 버퍼도 길이가 길면 분할
        if len(buf) > cfg.max_sentence_length:
            split_sentences = split_long_sentence(buf, cfg)
            out.extend(split_sentences)
        else:
            out.append(buf.strip())

    return out


def count_eomi_in_text(text: str) -> int:
    """텍스트에서 신뢰도 높은 종결어미 개수를 세는 함수"""
    if not text:
        return 0
    
    count = 0
    for match in END_EOMI_SPLIT_RE.finditer(text):
        eomi_str = match.group()
        
        # 문맥 점수 계산 (정확한 위치 정보 전달)
        context_score = get_eomi_context_score(text, eomi_str, match.start())
        
        # 문맥 점수가 4점 이상인 경우만 고려
        if context_score >= 4.0:
            count += 1
    
    return count


def split_long_sentence(text: str, cfg: SBDConfig) -> List[str]:
    """긴 문장을 신뢰도 높은 종결어미 기준으로 3개씩 분할하는 함수"""
    if len(text) <= cfg.max_sentence_length:
        return [text]
    
    # 종결어미 위치 찾기 (문맥 점수 4점 이상인 경우만 고려)
    eomi_matches = []
    for match in END_EOMI_SPLIT_RE.finditer(text):
        eomi_str = match.group()
        
        # 문맥 점수 계산 (정확한 위치 정보 전달)
        context_score = get_eomi_context_score(text, eomi_str, match.start())
        
        # 문맥 점수가 4점 이상인 경우만 고려
        if context_score >= 4.0:
            eomi_matches.append(match)
    
    # 4점 이상인 종결어미가 4개 미만이면 분할하지 않음
    if len(eomi_matches) < 4:
        return [text]
    
    sentences = []
    current_pos = 0
    
    # 3개 종결어미마다 분할
    for i in range(0, len(eomi_matches), 3):
        if i + 2 < len(eomi_matches):  # 3개 종결어미가 가능한 경우
            # 3번째 종결어미 뒤에서 분할
            split_pos = eomi_matches[i + 2].end()
            
            # 문장부호가 뒤에 오는 경우 문장부호까지 포함
            if split_pos < len(text):
                next_char = text[split_pos]
                if next_char in '.!?…':  # 문장부호인 경우
                    # 연속된 문장부호까지 모두 포함
                    while split_pos < len(text) and text[split_pos] in '.!?…':
                        split_pos += 1
            
            sentence = text[current_pos:split_pos].strip()
            if sentence:
                sentences.append(sentence)
            current_pos = split_pos
        else:
            # 마지막 남은 부분 (1-2개 종결어미)
            remaining_text = text[current_pos:].strip()
            if remaining_text:
                sentences.append(remaining_text)
            break
    
    return sentences if sentences else [text]

# --------- 전체 파이프라인 접점 ----------
def sbd_merge_messages(data: List[Dict[str, Any]], cfg: Optional[SBDConfig] = None) -> List[Dict[str, Any]]:
    """
    1) 백채널 태깅
    2) 화자-시간 윈도우로 turn 묶기
    3) turn 내부에서 SBD 점수 기반 병합
    """
    cfg = cfg or SBDConfig()

    # 원본 데이터 복사본 생성
    data_copy = []
    for item in data:
        item_copy = item.copy()
        if 'message' in item_copy:
            item_copy['is_backchannel'] = is_backchannel(item_copy['message'])
        data_copy.append(item_copy)

    # 2) turn 묶기
    data_copy = group_turns(data_copy, cfg)

    # 3) turn별 SBD 병합
    merged_rows = []
    for turn_id in range(max(item.get('turn_id', 0) for item in data_copy) + 1):
        turn_messages = [item for item in data_copy if item.get('turn_id') == turn_id]
        if turn_messages:
            seq = [(item.get('datetime'), item.get('message', '')) for item in turn_messages]
            merged_texts = merge_within_turn(seq, cfg)
            
            for merged_text in merged_texts:
                # 첫 번째 메시지의 정보를 기반으로 새 행 생성 (필수 필드만)
                base_item = {
                    'date': turn_messages[0].get('date', ''),
                    'user': turn_messages[0].get('user', ''),
                    'message': merged_text
                }
                merged_rows.append(base_item)

    return merged_rows

# --------- 메인 SBD 처리 함수 ----------
def process_sbd_merge(data: List[Dict[str, Any]], config: Optional[SBDConfig] = None) -> List[Dict[str, Any]]:
    """
    SBD 기반 문장 병합을 실행합니다.
    
    Args:
        data: [{'date': '...', 'user': '...', 'message': '...'}, ...] 형태의 데이터
        config: SBD 설정 (None이면 기본값 사용)
    
    Returns:
        병합된 메시지 데이터
    """
    print("🔗 SBD 문장 병합 시작...")
    original_count = len(data)
    
    try:
        # SBD 병합 실행
        merged_data = sbd_merge_messages(data, config)
        
        final_count = len(merged_data)
        print(f"✅ SBD 문장 병합 완료: {original_count}개 → {final_count}개 메시지")
        
        return merged_data
        
    except Exception as e:
        print(f"⚠️  SBD 문장 병합 중 오류 발생: {e}")
        print("   원문 메시지를 그대로 사용합니다.")
        return data
