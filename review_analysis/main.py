# -*- coding: utf-8 -*-
import os
from kakaogift_crawler import KakaoGiftCrawler

if __name__ == "__main__":
    output_dir = os.path.join(os.getcwd(), "data")  
    crawler = KakaoGiftCrawler(
        output_dir=output_dir,
        output_filename="kakao_gifts.csv",
        headless=False,
        items_per_subcat=300,
        start_url=None,
        top_filter=None
    )
    crawler.run()