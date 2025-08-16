# -*- coding: utf-8 -*-
"""
파일 처리 유틸리티 모듈
"""

import os
import csv
from pathlib import Path
from typing import List, Dict, Any

def ensure_directory(directory_path: str) -> None:
    """디렉토리가 존재하지 않으면 생성합니다."""
    Path(directory_path).mkdir(parents=True, exist_ok=True)

def read_text_file(file_path: str, encoding: str = 'utf-8') -> List[str]:
    """텍스트 파일을 읽어서 라인별로 반환합니다."""
    try:
        with open(file_path, 'r', encoding=encoding) as file:
            return file.readlines()
    except UnicodeDecodeError:
        with open(file_path, 'r', encoding='cp949') as file:
            return file.readlines()

def read_csv_file(file_path: str, encoding: str = 'utf-8') -> List[Dict[str, Any]]:
    """CSV 파일을 읽어서 딕셔너리 리스트로 반환합니다."""
    try:
        with open(file_path, 'r', encoding=encoding) as file:
            reader = csv.DictReader(file)
            return list(reader)
    except UnicodeDecodeError:
        with open(file_path, 'r', encoding='cp949') as file:
            reader = csv.DictReader(file)
            return list(reader)

def write_csv_file(data: List[Dict[str, Any]], output_path: str, columns: List[str]) -> None:
    """데이터를 CSV 파일로 저장합니다."""
    ensure_directory(os.path.dirname(output_path))
    with open(output_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()
        writer.writerows(data)
