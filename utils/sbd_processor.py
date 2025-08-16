# -*- coding: utf-8 -*-
"""
SBD(Sentence Boundary Detection) 기반 문장 병합 프로세스
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict, Any
import regex as re
import pandas as pd
from datetime import datetime

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
    r'(?:\p{Extended_Pictographic}(?:\uFE0F|\uFE0E)?(?:\u200D\p{Extended_Pictographic}(?:\uFE0F|\uFE0E)?)*)'
)

def strip_emojis(s: str) -> str:
    return EMOJI_SEQ.sub('', s).strip() if isinstance(s, str) else s

# --------- 규칙 패턴들 ----------
END_PUNCT_RE = re.compile(r"[.?!]+$")                     # 강한 종결부호
ELLIPSIS_RE  = re.compile(r"(…|\.{3,}|~{2,})$")           # 말줄임/물결로 끝
LAUGHTER_END = re.compile(r"(ㅋ|ㅎ)+$")                    # ㅋㅋ, ㅎㅎ 등으로 끝

# 평서/의문/감탄 종결어미(대표군)
END_EOMI_RE = re.compile(
    r"(다|요|죠|네|네요|습니다|습니까|했어|했네|겠네|랬어|랐어|군요|구나|구요|같아|같네요)$"
)

# 연결 어미(계속 신호)
CONT_EOMI_RE = re.compile(
    r"(는데|고|서|면서|다가|더니|자|며|지만|라고|니까|다니|든지|거나|려니)$"
)

# 조사로 끝나는 미완 단위 (예: '학교에서', '라면을')
PARTICLE_END_RE = re.compile(
    r"(은|는|이|가|을|를|에|에서|으로|로|와|과|랑|하고|보다|처럼|까지|부터|밖에|에게|께|께서|의)$"
)

# 다음 문장 시작이 접속부사인 경우(계속 신호)
NEXT_CONNECTIVE_RE = re.compile(
    r"^(그리고|근데|그래서|그런데|그때|그다음|또|게다가|하지만|그러다가|그러면|그러니까)\b"
)

@dataclass
class SBDConfig:
    # 턴 묶기/간격 기준
    t_merge_seconds: int = 60   # 동일 화자 turn 묶음 기준 (화자-시간 윈도우)
    t_gap_seconds: int = 25     # SBD에서 '긴 간격'으로 간주하는 임계

    # 점수 가중치 (종결 +, 계속 -)
    w_end_punct: int = 2
    w_end_eomi: int  = 2
    w_laughter_end: int = 1
    w_ellipsis_end: int = 1
    w_long_gap: int   = 1
    w_speaker_change: int = 1   # (turn 경계 외에는 사용 빈도 낮음)

    w_cont_eomi: int  = -2
    w_particle_end: int = -2
    w_next_connective: int = -1
    w_next_backchannel: int = -1

    theta: int = 2  # 임계치. 점수 >= theta이면 '끊기'. (높을수록 '붙이는' 쪽으로 바이어스)

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
                   cfg: SBDConfig) -> int:
    score = 0
    pt = prev_text.strip() if isinstance(prev_text, str) else ""
    nt = next_text.strip() if isinstance(next_text, str) else ""

    # --- 종결 신호 (+) ---
    if END_PUNCT_RE.search(pt):
        score += cfg.w_end_punct
    if END_EOMI_RE.search(pt):
        score += cfg.w_end_eomi
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

        if s >= cfg.theta:
            # 끊기
            if buf:
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
        out.append(buf.strip())

    return out

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
