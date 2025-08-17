# -*- coding: utf-8 -*-
"""
비한국어 내용 필터링 유틸리티
- 프로그래밍 언어 코드, 터미널 로그 등 비자연어 내용 제거
- 평문 영어 대화는 보존 (과삭제 방지)
"""

import re
import regex
import pandas as pd

HANGUL_RE = regex.compile(r"\p{Hangul}")
# 이모지/그림문자 제거용(선택)
EMOJI_RE = regex.compile(r"[\p{Extended_Pictographic}]")

# 평문 영어(일상 대화) 힌트 단어들 — 과삭제 방지용
COMMON_ENGLISH_HINTS = re.compile(
    r"\b(hi|hello|thanks?|sorry|please|ok(?:ay)?|yeah|nope|cool|great|"
    r"see you|good (?:morning|night|evening)|how (?:are|r) you|lol|idk|brb)\b",
    re.IGNORECASE
)


def non_korean_ratio(text: str) -> float:
    """
    텍스트에서 '한글' 비중을 계산해 비한국어 비율을 반환.
    숫자/공백/일반 구두점/이모지 등은 중립으로 보고 분모에서 제외(선택적).
    """
    if not text:
        return 0.0
    # 이모지는 제거(분석 잡음 감소)
    t = EMOJI_RE.sub("", text)

    # 알파벳/한자/기타 기호 등 전체 길이
    # 분모: 의미 있는 문자만 집계(공백, 단순 구두점은 제외)
    meaningful_chars = re.findall(r"[^\s\.,;:!\?\-\(\)\[\]\{\}\"'`~|\\/]", t)
    if not meaningful_chars:
        return 0.0

    ko_chars = HANGUL_RE.findall(t)
    ko_cnt = len(ko_chars)
    total_cnt = len(meaningful_chars)
    non_ko = max(total_cnt - ko_cnt, 0)
    return non_ko / total_cnt


def special_char_ratio(text: str) -> float:
    """
    코드/로그에 흔한 특수문자 밀도. 높을수록 비자연어 가능성↑
    """
    if not text:
        return 0.0
    specials = re.findall(r"[{}\[\]\(\);:=<>/*#\$`\\|@&^%_~]", text)
    return len(specials) / max(len(text), 1)


def looks_like_plain_english(text: str) -> bool:
    """
    평문 영어 대화로 보이면 True: 과삭제 방지.
    - 영어 힌트 단어 + 낮은 특수문자 비율 + 문장부호 패턴
    """
    if COMMON_ENGLISH_HINTS.search(text) and special_char_ratio(text) <= 0.06:
        # 마침표/의문문 등 일반 문장부호가 있으면 가산
        if re.search(r"[\.!?](\s|$)", text) or re.search(r"\b(I|You|We|They|He|She|It)\b", text):
            return True
    return False


def should_drop_row(
    text: str,
    min_chars: int = 40,
    nonko_threshold: float = 0.60,
    special_threshold: float = 0.12,
    keep_plain_english: bool = True,
) -> bool:
    """
    삭제 기준:
      - 길이 ≥ min_chars
      - 비한국어 비율 ≥ nonko_threshold
      - (보조) 특수문자 비율이 높거나(코드/로그 시그널), 
        평문영어 보존 옵션에 안 걸릴 때
    """
    if not isinstance(text, str):
        return False

    s = text.strip()
    if len(s) < min_chars:
        return False

    nk = non_korean_ratio(s)
    if nk < nonko_threshold:
        return False

    # 평문 영어면 보존 (옵션)
    if keep_plain_english and looks_like_plain_english(s):
        return False

    # 코드/로그에 흔한 특수문자 밀도가 낮아도, 이미 nk가 높다면 삭제 대상일 가능성이 큼
    # 다만 과삭제 방지를 위해 특수문자 밀도가 아주 낮은 순수 영어 문장일 땐 
    # 한 번 더 보류
    sp = special_char_ratio(s)
    if sp >= special_threshold:
        return True  # 코드/로그 느낌 → 삭제
    # 특수문자 적어도 비한국어 비중이 매우 높으면 삭제(설정값: 0.75 이상일 때 강제)
    if nk >= max(nonko_threshold, 0.75):
        return True

    # 기본은 보존
    return False


def drop_non_korean_heavy_rows_in_csv(
    in_path: str,
    out_path: str,
    text_col: str = "message",
    **kwargs
) -> pd.DataFrame:
    """
    CSV에서 text_col 기준으로 조건 충족 행을 삭제.
    kwargs는 should_drop_row의 파라미터 전달.
    """
    df = pd.read_csv(in_path)
    mask_drop = []
    for i, row in df.iterrows():
        msg = "" if pd.isna(row.get(text_col, "")) else str(row[text_col])
        mask_drop.append(should_drop_row(msg, **kwargs))
    df2 = df.loc[~pd.Series(mask_drop, index=df.index)].copy()
    df2.to_csv(out_path, index=False)
    return df2


def filter_dataframe_by_korean_ratio(
    df: pd.DataFrame,
    text_col: str = "message",
    **kwargs
) -> pd.DataFrame:
    """
    DataFrame에서 text_col 기준으로 조건 충족 행을 삭제.
    kwargs는 should_drop_row의 파라미터 전달.
    """
    mask_drop = []
    for i, row in df.iterrows():
        msg = "" if pd.isna(row.get(text_col, "")) else str(row[text_col])
        mask_drop.append(should_drop_row(msg, **kwargs))
    
    filtered_df = df.loc[~pd.Series(mask_drop, index=df.index)].copy()
    return filtered_df


def get_filtering_stats(
    df: pd.DataFrame,
    text_col: str = "message",
    **kwargs
) -> dict:
    """
    필터링 통계 정보를 반환
    """
    total_rows = len(df)
    mask_drop = []
    
    for i, row in df.iterrows():
        msg = "" if pd.isna(row.get(text_col, "")) else str(row[text_col])
        mask_drop.append(should_drop_row(msg, **kwargs))
    
    dropped_rows = sum(mask_drop)
    kept_rows = total_rows - dropped_rows
    
    return {
        "total_rows": total_rows,
        "dropped_rows": dropped_rows,
        "kept_rows": kept_rows,
        "drop_ratio": dropped_rows / total_rows if total_rows > 0 else 0.0
    }
