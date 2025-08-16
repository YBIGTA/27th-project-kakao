# -*- coding: utf-8 -*-
"""
카카오톡 처리 시스템 설정
"""

# 기본 파일 경로
INPUT_FILE = "db/input_db/KakaoTalk_Chat_경영 23 강예서(YBIGTA).csv"
OUTPUT_CSV = "processed_message.csv"
OUTPUT_DIR = "db/output_db"  # db/output_db 폴더에 저장

# CSV 컬럼 설정
CSV_COLUMNS = ['date', 'user', 'message']
