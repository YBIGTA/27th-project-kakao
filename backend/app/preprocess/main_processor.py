# -*- coding: utf-8 -*-
"""
카카오톡 처리 시스템 메인 실행 파일
"""

import sys
import os
import argparse
import tempfile
from pathlib import Path
from typing import Union

# 상대 import 사용
from .config.settings import CSV_COLUMNS
from .utils.file_utils import write_csv_file
from .text_processor import TextProcessor
from .csv_processor import CSVProcessor

def detect_file_type(file_path: str) -> str:
    """파일 확장자를 기반으로 파일 타입을 감지합니다."""
    ext = Path(file_path).suffix.lower()
    if ext == '.csv':
        return 'csv'
    elif ext == '.txt':
        return 'txt'
    else:
        return 'unknown'

def main(input_file_path: str = None, file_bytes: bytes = None, filename: str = None, 
         output_dir: str = None, user_name: str = "default") -> str:
    """
    카카오톡 파일을 CSV로 변환합니다.
    
    Args:
        input_file_path: 입력 파일 경로 (명령줄 실행 시 사용)
        file_bytes: 파일 bytes (백엔드에서 사용)
        filename: 파일명 (백엔드에서 사용)
        output_dir: 출력 디렉토리 (None이면 임시 디렉토리 사용)
        user_name: 사용자명
    
    Returns:
        str: 생성된 CSV 파일 경로
    """
    input_path = None
    temp_input_file = None
    
    try:
        # 입력 파일 처리
        if file_bytes is not None and filename is not None:
            # bytes를 임시 파일로 저장
            temp_input_file = tempfile.NamedTemporaryFile(mode='wb', suffix=Path(filename).suffix, delete=False)
            temp_input_file.write(file_bytes)
            temp_input_file.close()
            input_path = temp_input_file.name
        elif input_file_path is not None:
            # 파일 경로 직접 사용
            input_path = input_file_path
        else:
            raise ValueError("입력 파일 정보가 없습니다.")
        
        # 파일 타입 감지
        file_type = detect_file_type(input_path)
        if file_type == 'unknown':
            raise ValueError(f"지원하지 않는 파일 형식입니다: {input_path}")
        
        # 출력 디렉토리 설정
        if output_dir is None:
            output_dir = tempfile.mkdtemp()
        else:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # 출력 파일 경로
        output_csv = os.path.join(output_dir, "processed.csv")
        
        print(f"📁 입력 파일: {input_path}")
        print(f"🔍 파일 타입: {file_type.upper()}")
        print(f"📁 출력 CSV: {output_csv}")
        print(f"👤 선택된 사용자: {user_name}")
        
        # 파일 타입에 따라 적절한 프로세서 선택
        processor: Union[CSVProcessor, TextProcessor]
        if file_type == 'csv':
            processor = CSVProcessor(input_path, user_name)
        else:
            processor = TextProcessor(input_path, output_dir, user_name)
        
        # 데이터 처리
        result = processor.process()
        
        # 최종 CSV 저장
        write_csv_file(result['data'], output_csv, CSV_COLUMNS)
        
        print(f"✅ 완료: {output_csv}")
        print(f"📊 최종 결과: {result['final_count']}개 메시지")
        
        return output_csv
        
    finally:
        # 임시 입력 파일 정리
        if temp_input_file is not None and os.path.exists(temp_input_file.name):
            try:
                os.unlink(temp_input_file.name)
            except Exception as e:
                print(f"임시 입력 파일 정리 실패: {e}")

def main_cli():
    """명령줄 인터페이스용 메인 함수"""
    parser = argparse.ArgumentParser(description='카카오톡 파일을 CSV로 변환합니다.')
    
    parser.add_argument(
        '-i', '--input',
        type=str,
        required=True,
        help='입력 카카오톡 파일 경로'
    )
    
    parser.add_argument(
        '-o', '--output',
        type=str,
        default=None,
        help='출력 디렉토리 경로 (기본값: 임시 디렉토리)'
    )
    
    parser.add_argument(
        '--user',
        type=str,
        required=True,
        help='특정 사용자의 메시지만 필터링 (예: --user "홍길동")'
    )
    
    args = parser.parse_args()
    
    try:
        # 입력 파일 존재 확인
        if not os.path.exists(args.input):
            print(f"❌ 입력 파일을 찾을 수 없습니다: {args.input}")
            return 1
        
        # 메인 함수 호출
        output_csv = main(
            input_file_path=args.input,
            output_dir=args.output,
            user_name=args.user
        )
        
        print(f"✅ 완료: {output_csv}")
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main_cli()
    sys.exit(exit_code)
