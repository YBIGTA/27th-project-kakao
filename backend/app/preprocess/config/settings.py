# -*- coding: utf-8 -*-
"""
카카오톡 처리 시스템 설정
"""

from pathlib import Path

# 기본 파일 경로
INPUT_DIR = "db/input_db"  # 입력 파일들이 있는 디렉토리
OUTPUT_CSV = "processed_message.csv"
OUTPUT_DIR = "db/output_db"  # db/output_db 폴더에 저장


def get_input_file():
    """
    db/input_db/ 폴더에서 txt 또는 csv 파일을 자동으로 찾아서 반환
    """
    input_path = Path(INPUT_DIR)
    if not input_path.exists():
        raise FileNotFoundError(f"입력 디렉토리를 찾을 수 없습니다: {INPUT_DIR}")
    
    # txt와 csv 파일을 찾기
    txt_files = list(input_path.glob("*.txt"))
    csv_files = list(input_path.glob("*.csv"))
    
    # 우선순위: txt 파일이 있으면 txt, 없으면 csv
    if txt_files:
        return str(txt_files[0])
    elif csv_files:
        return str(csv_files[0])
    else:
        raise FileNotFoundError(f"{INPUT_DIR} 폴더에 txt 또는 csv 파일을 찾을 수 없습니다.")


# 입력 파일 경로 (자동으로 찾아짐)
INPUT_FILE = get_input_file()

# CSV 컬럼 설정
CSV_COLUMNS = ['date', 'user', 'message']
