from abc import ABC, abstractmethod
import os

class BaseCrawler(ABC):
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        # 저장 폴더 없으면 자동 생성
        os.makedirs(self.output_dir, exist_ok=True)

    @abstractmethod
    def start_browser(self):
        """브라우저 실행 및 초기화"""
        pass

    @abstractmethod
    def scrape_data(self):
        """데이터 크롤링 로직"""
        pass

    @abstractmethod
    def save_to_database(self, data):
        """DB나 CSV 저장 로직"""
        pass

    def close_browser(self):
        """브라우저 종료 (선택 사항)"""
        if hasattr(self, 'driver') and self.driver:
            self.driver.quit()