# -*- coding: utf-8 -*-
"""
카카오톡 처리 시스템 설정
"""

from pathlib import Path

# 기본 파일 경로 - 프로젝트 루트 기준
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
INPUT_DIR = PROJECT_ROOT / "data_pipeline" / "raw_data"  # 실제 존재하는 디렉토리
OUTPUT_CSV = "processed_message.csv"
OUTPUT_DIR = PROJECT_ROOT / "data_pipeline" / "processed"  # 출력 디렉토리

def get_input_file():
    """
    raw_data/ 폴더에서 txt 또는 csv 파일을 자동으로 찾아서 반환
    """
    input_path = Path(INPUT_DIR)
    if not input_path.exists():
        # 디렉토리가 없으면 생성
        input_path.mkdir(parents=True, exist_ok=True)
        print(f"✅ 입력 디렉토리 생성: {INPUT_DIR}")
    
    # txt와 csv 파일을 찾기
    txt_files = list(input_path.glob("*.txt"))
    csv_files = list(input_path.glob("*.csv"))
    
    # 우선순위: txt 파일이 있으면 txt, 없으면 csv
    if txt_files:
        return str(txt_files[0])
    elif csv_files:
        return str(csv_files[0])
    else:
        # 파일이 없으면 더미 파일 경로 반환
        print(f"⚠️ {INPUT_DIR} 폴더에 txt 또는 csv 파일을 찾을 수 없습니다. 더미 파일을 사용합니다.")
        return str(input_path / "dummy.txt")

# 입력 파일 경로 (자동으로 찾아짐)
INPUT_FILE = get_input_file()

# CSV 컬럼 설정
CSV_COLUMNS = ['date', 'user', 'message']
