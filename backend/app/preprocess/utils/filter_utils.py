# -*- coding: utf-8 -*-
"""
데이터 필터링 유틸리티
"""

import pandas as pd
from typing import List, Dict, Any

def filter_by_user(
    data: List[Dict[str, Any]], 
    target_user: str
) -> List[Dict[str, Any]]:
    """
    특정 사용자의 메시지만 필터링합니다.
    """
    if not data:
        return []
    
    print(f"🔍 사용자 필터링: '{target_user}'")
    
    # 사용자 이름으로 필터링 (공백 제거)
    filtered_data = [
        item for item in data 
        if item.get('user', '').strip().lower() == target_user.strip().lower()
    ]
    
    print(f"👤 필터링 결과: {len(filtered_data)}개 메시지")
    return filtered_data

def filter_recent_messages_pandas(
    data: List[Dict[str, Any]], 
    months: int = 3
) -> List[Dict[str, Any]]:
    """
    pandas를 사용하여 최근 N개월의 메시지만 필터링합니다.
    """
    if not data:
        return []
    
    # DataFrame으로 변환
    df = pd.DataFrame(data)
    
    # date 컬럼을 datetime으로 변환 (더 유연한 형식 지원)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])
    
    if df.empty:
        print("⚠️ 날짜 변환 후 데이터가 없습니다.")
        return []
    
    # 날짜 범위 확인
    min_date = df['date'].min()
    max_date = df['date'].max()
    print(f"📅 데이터 날짜 범위: {min_date} ~ {max_date}")
    
    # 현재 시간 기준으로 3개월 전 계산 (timezone 없이)
    now = pd.Timestamp.now()
    cutoff = now - pd.DateOffset(months=months)
    
    print(f"🔍 {months}개월 필터링: {cutoff.strftime('%Y-%m-%d %H:%M:%S')} 이후")
    
    # 필터링
    original_count = len(df)
    
    # 미래 날짜가 있는 경우 처리
    future_dates = df[df['date'] > now]
    if not future_dates.empty:
        print(f"⚠️ 미래 날짜 발견: {len(future_dates)}개 (최대: {future_dates['date'].max()})")
        # 미래 날짜를 현재 시간으로 조정
        df.loc[df['date'] > now, 'date'] = now
    
    # datetime 타입을 맞춰서 비교 (timezone 정보 제거)
    df['date'] = df['date'].dt.tz_localize(None)
    cutoff_no_tz = cutoff.tz_localize(None)
    
    filtered_df = df[df['date'] >= cutoff_no_tz]
    filtered_count = len(filtered_df)
    
    print(f"📊 필터링 결과: {original_count}개 → {filtered_count}개 메시지")
    
    # 딕셔너리 리스트로 변환하여 반환
    filtered_data = []
    for _, row in filtered_df.iterrows():
        filtered_data.append({
            'date': row['date'].strftime('%Y-%m-%d %H:%M:%S'),
            'user': row['user'],
            'message': row['message']
        })
    
    return filtered_data
