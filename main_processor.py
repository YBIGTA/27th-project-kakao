# -*- coding: utf-8 -*-
"""
카카오톡 처리 시스템 메인 실행 파일
"""

import sys
import os
import argparse
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from preprocess.config.settings import INPUT_FILE, OUTPUT_CSV, OUTPUT_DIR
from preprocess.utils.file_utils import write_csv_file
from preprocess.text_processor import TextProcessor
from preprocess.csv_processor import CSVProcessor

def detect_file_type(file_path: str) -> str:
    """파일 확장자를 기반으로 파일 타입을 감지합니다."""
    ext = Path(file_path).suffix.lower()
    if ext == '.csv':
        return 'csv'
    elif ext == '.txt':
        return 'txt'
    else:
        return 'unknown'

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='카카오톡 파일을 CSV로 변환합니다.')
    
    parser.add_argument(
        '-i', '--input',
        type=str,
        default=INPUT_FILE,
        help=f'입력 카카오톡 파일 경로 (기본값: {INPUT_FILE})'
    )
    
    parser.add_argument(
        '-o', '--output',
        type=str,
        default=OUTPUT_DIR,
        help=f'출력 디렉토리 경로 (기본값: {OUTPUT_DIR})'
    )
    
    parser.add_argument(
        '--user',
        type=str,
        help='특정 사용자의 메시지만 필터링 (예: --user "홍길동")'
    )
    
    args = parser.parse_args()
    
    try:
        # 입력 파일 존재 확인
        if not os.path.exists(args.input):
            print(f"❌ 입력 파일을 찾을 수 없습니다: {args.input}")
            return 1
        
        # 파일 타입 감지
        file_type = detect_file_type(args.input)
        if file_type == 'unknown':
            print(f"❌ 지원하지 않는 파일 형식입니다: {args.input}")
            print("   지원 형식: .txt, .csv")
            return 1
        
        # 출력 디렉토리 생성
        Path(args.output).mkdir(parents=True, exist_ok=True)
        
        # 출력 파일 경로
        output_csv = os.path.join(args.output, OUTPUT_CSV)
        
        print(f"📁 입력 파일: {args.input}")
        print(f"🔍 파일 타입: {file_type.upper()}")
        print(f"📁 출력 CSV: {output_csv}")
        
        # 사용자 이름 입력 받기 (명령줄 인자가 없으면)
        if not args.user:
            print("\n👤 대화 상대 이름을 입력하세요:")
            args.user = input("사용자 이름: ").strip()
            if not args.user:
                print("❌ 사용자 이름을 입력해야 합니다.")
                return 1
        
        print(f"👤 선택된 사용자: {args.user}")
        
        # 파일 타입에 따라 적절한 프로세서 선택
        if file_type == 'csv':
            processor = CSVProcessor(args.input, args.user)
        else:
            processor = TextProcessor(args.input, args.output, args.user)
        
        # 데이터 처리
        result = processor.process()
        
        # 최종 CSV 저장
        write_csv_file(result['data'], output_csv, ['date', 'user', 'message'])
        
        print(f"✅ 완료: {output_csv}")
        print(f"📊 최종 결과: {result['final_count']}개 메시지")
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
