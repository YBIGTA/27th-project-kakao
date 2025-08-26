# -*- coding: utf-8 -*-
"""
CSV 파일 처리기
"""

import os
import tempfile
import io
import csv
from .utils.file_utils import read_csv_file
from .utils.filter_utils import filter_recent_messages_pandas, filter_by_user
from .utils.text_utils import preprocess_messages, clean_emotion_messages, drop_short_messages
from .utils.sbd_processor import process_sbd_merge, SBDConfig
from typing import Optional

class CSVProcessor:
    """CSV 파일을 처리하는 클래스"""
    
    def __init__(self, input_file: str, user_name: str):
        self.input_file = input_file
        self.user_name = user_name
        self._temp_file: Optional[str] = None
    
    @classmethod
    def from_bytes(cls, file_bytes: bytes, user_name: str):
        """bytes로부터 CSVProcessor 인스턴스를 생성합니다."""
        # 임시 파일 생성
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as tmp:
            tmp.write(file_bytes)
            tmp_path = tmp.name
        
        instance = cls(tmp_path, user_name)
        instance._temp_file = tmp_path
        return instance
    
    def process(self) -> dict:
        """CSV 파일을 처리하는 메인 메서드"""
        print("📁 CSV 파일 감지")
        
        try:
            # 1. CSV 파일 읽기
            csv_data = read_csv_file(self.input_file)
            original_total = len(csv_data)
            
            # 디버깅: 원본 CSV 데이터 구조 확인
            print(f"🔍 원본 CSV 데이터 구조:")
            if csv_data:
                first_item = csv_data[0]
                print(f"  첫 번째 항목의 키: {list(first_item.keys())}")
                print(f"  첫 번째 항목의 값: {first_item}")
            
            # 컬럼명 매핑 (실제 CSV 파일의 컬럼명에 맞춤, BOM 제거)
            mapped_data = []
            for item in csv_data:
                # BOM 문자 제거
                date_key = 'Date' if 'Date' in item else '\ufeffDate'
                user_key = 'User' if 'User' in item else 'User'
                message_key = 'Message' if 'Message' in item else 'Message'
                
                mapped_item = {
                    'date': item.get(date_key, ''),
                    'user': item.get(user_key, ''),
                    'message': item.get(message_key, '')
                }
                mapped_data.append(mapped_item)
            
            # 디버깅: 매핑된 데이터 확인
            print(f"🔍 매핑된 데이터 샘플:")
            for i, item in enumerate(mapped_data[:3]):
                print(f"  {i+1}: date='{item['date']}', user='{item['user']}', message='{item['message'][:20]}...'")
            
            original_users = set(item.get('user', '') for item in mapped_data if item.get('user'))
            
            print(f"📊 원본 데이터: {original_total}개 메시지, {len(original_users)}명 참여자")
            print(f"👥 참여자: {', '.join(sorted(original_users))}")
            
            # 2. 3개월 필터링
            print(f"🔍 3개월 필터링 전: {len(mapped_data)}개")
            filtered_data = filter_recent_messages_pandas(mapped_data, months=3)
            print(f"🔍 3개월 필터링 후: {len(filtered_data)}개")
            
            # 3. 사용자별 필터링
            print(f"🔍 사용자 필터링 전: {len(filtered_data)}개")
            user_filtered_data = filter_by_user(filtered_data, self.user_name)
            print(f"🔍 사용자 필터링 후: {len(user_filtered_data)}개")
            
            # 4. 기본 전처리 (SBD 전에 실행)
            print(f"🔍 기본 전처리 전: {len(user_filtered_data)}개")
            preprocessed_data = preprocess_messages(user_filtered_data)
            print(f"🔍 기본 전처리 후: {len(preprocessed_data)}개")
            
            # 5. SBD 문장 병합 (기본 전처리 이후)
            print(f"🔍 SBD 전: {len(preprocessed_data)}개")
            sbd_config = SBDConfig()
            sbd_merged_data = process_sbd_merge(preprocessed_data, sbd_config)
            print(f"🔍 SBD 후: {len(sbd_merged_data)}개")
            
            # 6. 감정표현 메시지 필터링 (SBD 이후)
            print(f"🔍 감정표현 필터링 전: {len(sbd_merged_data)}개")
            emotion_filtered_data = clean_emotion_messages(sbd_merged_data)
            print(f"🔍 감정표현 필터링 후: {len(emotion_filtered_data)}개")
            
            # 7. 짧은 메시지 제거 (마지막)
            print(f"🔍 짧은 메시지 제거 전: {len(emotion_filtered_data)}개")
            short_filtered_data = drop_short_messages(emotion_filtered_data, min_length=4)
            print(f"🔍 짧은 메시지 제거 후: {len(short_filtered_data)}개")
            
            # 8. 익명화 처리 (민감정보 마스킹)
            print(f"🔍 익명화 전: {len(short_filtered_data)}개")
            from .utils.anonymize import anonymize_messages
            final_data = anonymize_messages(short_filtered_data)
            print(f"🔍 익명화 후: {len(final_data)}개")
            
            return {
                'data': final_data,
                'original_total': original_total,
                'final_count': len(final_data)
            }
        finally:
            # 임시 파일 정리
            if hasattr(self, '_temp_file') and self._temp_file is not None and os.path.exists(self._temp_file):
                os.unlink(self._temp_file)
