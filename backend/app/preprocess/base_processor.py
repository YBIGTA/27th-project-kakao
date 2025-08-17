# -*- coding: utf-8 -*-
"""
카카오톡 처리 기본 클래스
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

from .utils.file_utils import read_text_file, write_csv_file
from .utils.text_utils import (
    extract_date_from_line, extract_time_from_line, extract_sender_from_line,
    extract_message_content, convert_korean_date_to_iso
)
from .config.settings import CSV_COLUMNS

class BaseProcessor(ABC):
    """카카오톡 처리 기본 클래스"""
    
    def __init__(self, input_file: str, output_file: str):
        self.input_file = input_file
        self.output_file = output_file
        self.processed_data = []
        self.current_date = None
        self.current_time = None
        self.current_sender = None
    
    def load_data(self) -> List[str]:
        """입력 파일에서 데이터를 로드합니다."""
        return read_text_file(self.input_file)
    
    def save_data(self) -> None:
        """처리된 데이터를 CSV 파일로 저장합니다."""
        write_csv_file(self.processed_data, self.output_file, CSV_COLUMNS)
    
    def process_line(self, line: str) -> Optional[Dict[str, Any]]:
        """단일 라인을 처리합니다."""
        line = line.strip()
        if not line:
            return None
        
        # 날짜 라인 처리
        date = extract_date_from_line(line)
        if date:
            self.current_date = date
            return None
        
        # 메시지 라인 처리
        time = extract_time_from_line(line)
        sender = extract_sender_from_line(line)
        
        if time and sender:
            self.current_time = time
            self.current_sender = sender
            message_content = extract_message_content(line)
            
            # 날짜와 시간을 YYYY-MM-DD HH:MM:SS 형식으로 변환
            datetime_combined = convert_korean_date_to_iso(self.current_date, self.current_time)
            
            if datetime_combined:
                self.processed_data.append({
                    'date': datetime_combined,
                    'user': sender,
                    'message': message_content
                })
        
        return None
    
    def process_data(self) -> None:
        """전체 데이터를 처리합니다."""
        lines = self.load_data()
        for line in lines:
            processed_line = self.process_line(line)
            if processed_line:
                self.processed_data.append(processed_line)
    
    def get_statistics(self) -> Dict[str, Any]:
        """처리된 데이터의 통계 정보를 반환합니다."""
        if not self.processed_data:
            return {}
        
        total_messages = len(self.processed_data)
        senders = set(item['user'] for item in self.processed_data if item['user'])
        
        return {
            'total_messages': total_messages,
            'unique_senders': len(senders),
            'senders': list(senders)
        }
    
    @abstractmethod
    def run(self) -> None:
        """처리를 실행합니다. 하위 클래스에서 구현해야 합니다."""
        pass
