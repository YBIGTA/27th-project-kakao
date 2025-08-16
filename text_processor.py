# -*- coding: utf-8 -*-
"""
TXT 파일 처리기
"""

import os
from pathlib import Path
from preprocess.processor import KakaoProcessor
from preprocess.utils.filter_utils import filter_recent_messages_pandas, filter_by_user
from preprocess.utils.text_utils import preprocess_messages
from preprocess.utils.sbd_processor import process_sbd_merge, SBDConfig

class TextProcessor:
    """TXT 파일을 처리하는 클래스"""
    
    def __init__(self, input_file: str, output_dir: str, user_name: str):
        self.input_file = input_file
        self.output_dir = output_dir
        self.user_name = user_name
        self.output_csv = os.path.join(output_dir, "temp_converted.csv")
    
    def process(self) -> dict:
        """TXT 파일을 처리하는 메인 메서드"""
        print("🔄 TXT → CSV 변환 중...")
        
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
        
        # 4. 전처리 자동 적용
        preprocessed_data = preprocess_messages(user_filtered_data)
        
        # 5. 익명화 처리 (민감정보 마스킹)
        from utils.anonymize import anonymize_messages
        anonymized_data = anonymize_messages(preprocessed_data)
        
        # 6. 어미 교정 (SBD 전)
        # final_data = spell_check_kakao_messages(preprocessed_data)  # 함수가 정의되지 않아 주석 처리
        final_data = anonymized_data
        
        # 7. SBD 문장 병합
        sbd_config = SBDConfig()
        final_data = process_sbd_merge(final_data, sbd_config)
        
        # 임시 CSV 파일 삭제
        if os.path.exists(self.output_csv):
            os.remove(self.output_csv)
        
        return {
            'data': final_data,
            'original_total': original_total,
            'original_users': original_users,
            'final_count': len(final_data)
        }
