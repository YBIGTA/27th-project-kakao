# -*- coding: utf-8 -*-
"""
TXT 파일 처리기
"""

import os
import tempfile
from pathlib import Path
from .processor import KakaoProcessor
from .utils.filter_utils import filter_recent_messages_pandas, filter_by_user
from .utils.text_utils import preprocess_messages, clean_emotion_messages, drop_short_messages
from .utils.sbd_processor import process_sbd_merge, SBDConfig
from typing import Optional

class TextProcessor:
    """TXT 파일을 처리하는 클래스"""
    
    def __init__(self, input_file: str, output_dir: str, user_name: str):
        self.input_file = input_file
        self.output_dir = output_dir
        self.user_name = user_name
        self.output_csv = os.path.join(output_dir, "temp_converted.csv")
        self._temp_file: Optional[str] = None
    
    @classmethod
    def from_bytes(cls, file_bytes: bytes, output_dir: str, user_name: str):
        """bytes로부터 TextProcessor 인스턴스를 생성합니다."""
        # 임시 파일 생성
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.txt', delete=False) as tmp:
            tmp.write(file_bytes)
            tmp_path = tmp.name
        
        instance = cls(tmp_path, output_dir, user_name)
        instance._temp_file = tmp_path
        return instance
    
    def process(self) -> dict:
        """TXT 파일을 처리하는 메인 메서드"""
        print("🔄 TXT → CSV 변환 중...")
        
        try:
            # 1. TXT → CSV 변환
            processor = KakaoProcessor(self.input_file, self.output_csv)
            processor.run()
            
            # 원본 통계
            original_stats = processor.get_statistics()
            original_total = original_stats.get('total_messages', 0)
            original_users = original_stats.get('senders', [])
            
            print(f"📊 원본 데이터: {original_total}개 메시지, {len(original_users)}명 참여자")
            print(f"👥 참여자: {', '.join(sorted(original_users))}")
            
            # 2. 3개월 필터링
            filtered_data = filter_recent_messages_pandas(processor.processed_data, months=3)
            
            # 3. 사용자별 필터링
            user_filtered_data = filter_by_user(filtered_data, self.user_name)
            
            # 4. 기본 전처리 (SBD 전에 실행)
            print("🧹 기본 전처리 중...")
            preprocessed_data = preprocess_messages(user_filtered_data)
            
            # 5. SBD 문장 병합 (기본 전처리 이후)
            print("🔗 SBD 문장 병합 중...")
            sbd_config = SBDConfig()
            sbd_merged_data = process_sbd_merge(preprocessed_data, sbd_config)
            
            # 6. 감정표현 메시지 필터링 (SBD 이후)
            print("🧹 감정표현 메시지 필터링 중...")
            emotion_filtered_data = clean_emotion_messages(sbd_merged_data)
            
            # 7. 짧은 메시지 제거 (마지막)
            print("✂️ 짧은 메시지 제거 중...")
            short_filtered_data = drop_short_messages(emotion_filtered_data, min_length=4)
            
            # 8. 익명화 처리 (민감정보 마스킹)
            print("🔒 익명화 처리 중...")
            from .utils.anonymize import anonymize_messages
            final_data = anonymize_messages(short_filtered_data)
            
            return {
                'data': final_data,
                'original_total': original_total,
                'original_users': original_users,
                'final_count': len(final_data)
            }
        finally:
            # 임시 파일들 정리
            if hasattr(self, '_temp_file') and self._temp_file is not None and os.path.exists(self._temp_file):
                os.unlink(self._temp_file)
            if os.path.exists(self.output_csv):
                os.remove(self.output_csv)
