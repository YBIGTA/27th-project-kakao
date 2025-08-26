# -*- coding: utf-8 -*-
"""
카카오톡 처리기 구현 클래스
"""

from .base_processor import BaseProcessor

class KakaoProcessor(BaseProcessor):
    """카카오톡 메시지 처리기"""
    
    def __init__(self, input_file: str, output_file: str):
        super().__init__(input_file, output_file)
    
    def run(self):
        """카카오톡 처리를 실행합니다."""
        print("📖 카카오톡 파일 읽는 중...")
        self.load_data()
        
        print("🔍 메시지 파싱 중...")
        self.process_data()
        
        print("💾 CSV 파일 저장 중...")
        self.save_data()
        
        self.print_results()
    
    def print_results(self):
        """처리 결과를 출력합니다."""
        stats = self.get_statistics()
        print(f"\n📊 처리 완료: {stats.get('total_messages', 0)}개 메시지")
        print(f"👥 참여자: {stats.get('unique_senders', 0)}명")
