# -*- coding: utf-8 -*-
import os
import re
import time
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException, WebDriverException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from base_crawler import BaseCrawler


class KakaoGiftLiquorCrawler(BaseCrawler):
    """
    와인/양주/전통주 카테고리 전용 크롤러
    - 상위 카테고리: 와인/양주/전통주
    - 하위 카테고리: 와인(75개씩), 양주(60개씩), 맥주/기타(75개씩)
    - 흐름: 상위 카테고리 → 하위 카테고리 → 가격대별 탭 → 더보기 버튼으로 수집
    - 카드 필드: brand, product_name, price, satisfaction_pct, review_count,
                wish_count, tags, product_url, price_range
    """

    BASE = "https://gift.kakao.com"
    DEFAULT_START = "https://gift.kakao.com/home?categoryLayer=OPEN"

    # 동작 파라미터
    SCROLL_PAUSE = 1.0
    CLICK_PAUSE = 0.8
    LOAD_PAUSE = 2.0
    IMPLICIT_WAIT = 10
    EXPLICIT_WAIT = 15

    # 하위 카테고리별 수집 개수
    SUB_CATEGORY_TARGETS = {
        "와인": 75,
        "양주": 60,
        "맥주/기타": 75
    }

    # 가격대 탭 선택자들 (가격대별 필터 탭)
    PRICE_TAB_SELECTORS = [
        ".module_tab.module_tab_bubble .rail_cate a.link_tab",
        ".module_tab .rail_cate a.link_tab",
        ".rail_cate a.link_tab",
        ".module_tab_wrapper .link_tab",
        ".module_tab.module_tab_keyword .link_tab"
    ]
    
    # 가격대 탭 텍스트 패턴 (가격대만 필터링)
    PRICE_RANGE_PATTERNS = [
        r"~?\d+만?원대?",
        r"\d+~?\d*만?원",
        r"\d+만?원\s*이상",
        r"\d+만?원\s*~",
        r"~\d+만?원"
    ]

    # 더보기 버튼 선택자들
    MORE_BUTTON_SELECTORS = [
        "button.btn_more",
        ".module_bottom_btn button.btn_more",
        "button[data-tiara-copy*='더보기']",
        ".btn_more"
    ]

    def __init__(
        self,
        output_dir: str,
        *,
        output_filename: str = "kakao_gifts_liquor.csv",
        headless: bool = False,
        start_url: str | None = None,
    ):
        super().__init__(output_dir=output_dir)
        self.output_path = os.path.join(output_dir, output_filename)

        self.headless = headless
        self.start_url = start_url or self.DEFAULT_START

        self.driver = None
        self.wait = None
        self.all_rows = []

    # -------------------- 1) 브라우저 --------------------
    def start_browser(self):
        opts = Options()
        if self.headless:
            opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1500,1100")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--lang=ko-KR")
        opts.add_argument("--disable-dev-shm-usage")
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=opts)
        self.driver.implicitly_wait(self.IMPLICIT_WAIT)
        self.wait = WebDriverWait(self.driver, self.EXPLICIT_WAIT)

    # -------------------- 유틸 --------------------
    def sleep(self, t=None):
        time.sleep(t if t is not None else self.SCROLL_PAUSE)

    def js_click(self, el):
        try:
            self.driver.execute_script("arguments[0].click();", el)
        except WebDriverException:
            try:
                el.click()
            except Exception:
                pass
        self.sleep(self.CLICK_PAUSE)

    @staticmethod
    def safe_text(el):
        try:
            return el.text.strip()
        except Exception:
            return ""

    @staticmethod
    def to_int(text):
        if not text:
            return None
        s = re.sub(r"[^\d만\+,\s]", "", text).replace(",", "").replace(" ", "")
        if "만+" in s:
            try:
                return int(s.replace("만+", "")) * 10000
            except Exception:
                return None
        if "만" in s:
            try:
                return int(s.replace("만", "")) * 10000
            except Exception:
                pass
        try:
            return int(re.sub(r"[^\d]", "", s))
        except Exception:
            return None

    # -------------------- 카테고리 네비게이션 --------------------
    def category_panel_open(self) -> bool:
        """카테고리 패널(좌측 레이어)이 열려 있는지 확인"""
        try:
            self.driver.find_element(By.CSS_SELECTOR, ".category_layer .list_ctgmenu")
            return True
        except NoSuchElementException:
            return False

    def click_header_category_button(self) -> bool:
        """헤더의 '카테고리' 버튼을 눌러 패널을 연다."""
        selectors = [
            "button.btn_cate",
            "div.group_head button.btn_cate",
            "button[aria-label='카테고리']",
            "a[aria-label='카테고리']",
            "a[href*='categoryLayer']",
        ]
        for sel in selectors:
            try:
                btn = self.driver.find_element(By.CSS_SELECTOR, sel)
                self.js_click(btn)
                self.sleep(0.8)
                if self.category_panel_open():
                    return True
            except (NoSuchElementException, StaleElementReferenceException):
                continue
        return False

    def ensure_category_panel(self):
        """카테고리 패널을 반드시 연다."""
        if self.category_panel_open():
            return
        if not self.click_header_category_button():
            self.driver.get(self.DEFAULT_START)
            self.sleep(1.5)

    def click_top_category(self, name):
        """상위 카테고리 클릭"""
        els = self.driver.find_elements(
            By.CSS_SELECTOR, ".category_layer .list_ctgmenu a.link_menu"
        )
        for a in els:
            txt = self.safe_text(a.find_element(By.CSS_SELECTOR, "span.txt_menu"))
            if txt == name:
                self.js_click(a)
                self.sleep(1.0)
                return True
        return False

    def click_sub_category(self, name):
        """하위 카테고리 클릭"""
        els = self.driver.find_elements(
            By.CSS_SELECTOR, ".category_layer .list_ctgsub a.link_menu"
        )
        for a in els:
            txt = self.safe_text(a.find_element(By.CSS_SELECTOR, "span.txt_menu"))
            if txt == name:
                self.js_click(a)
                self.sleep(1.5)
                return True
        return False

    # -------------------- 가격대 탭 제어 --------------------
    def get_price_tabs(self):
        """가격대 탭들을 찾아서 텍스트와 요소를 반환"""
        price_tabs = []
        
        for selector in self.PRICE_TAB_SELECTORS:
            try:
                tabs = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if tabs:
                    for tab in tabs:
                        try:
                            text = self.safe_text(tab.find_element(By.CSS_SELECTOR, "span.txt_tab"))
                        except NoSuchElementException:
                            # span.txt_tab이 없는 경우 직접 텍스트 확인
                            text = self.safe_text(tab)
                        
                        # 가격대 패턴에 맞는 텍스트만 필터링
                        if text and self.is_price_range_tab(text) and text not in [t[0] for t in price_tabs]:
                            price_tabs.append((text, tab))
                    
                    if price_tabs:
                        break
            except Exception:
                continue
        
        return price_tabs
    
    def is_price_range_tab(self, text):
        """텍스트가 가격대 탭인지 확인"""
        import re
        for pattern in self.PRICE_RANGE_PATTERNS:
            if re.search(pattern, text):
                return True
        return False

    def click_price_tab(self, tab_element):
        """가격대 탭 클릭"""
        try:
            self.js_click(tab_element)
            self.sleep(self.LOAD_PAUSE)
            return True
        except Exception as e:
            print(f"        ! 가격대 탭 클릭 실패: {e}")
            return False

    # -------------------- 더보기 버튼 제어 --------------------
    def find_more_button(self):
        """더보기 버튼 찾기"""
        for selector in self.MORE_BUTTON_SELECTORS:
            try:
                button = self.driver.find_element(By.CSS_SELECTOR, selector)
                if button.is_displayed():
                    return button
            except NoSuchElementException:
                continue
        return None

    def click_more_button(self):
        """더보기 버튼 클릭"""
        button = self.find_more_button()
        if button:
            try:
                # 버튼이 클릭 가능한 상태인지 확인
                if not button.is_enabled() or not button.is_displayed():
                    print(f"        ! 더보기 버튼이 비활성화되어 있음")
                    return False
                
                # 현재 상품 개수 저장
                before_count = len(self.driver.find_elements(By.CSS_SELECTOR, "ul.list_prd > li"))
                
                # 버튼이 보이도록 스크롤
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                self.sleep(0.5)
                
                # 클릭 시도
                self.js_click(button)
                
                # 로딩 대기 (더 길게)
                self.sleep(self.LOAD_PAUSE + 2.0)
                
                # 상품 개수가 증가했는지 확인
                after_count = len(self.driver.find_elements(By.CSS_SELECTOR, "ul.list_prd > li"))
                if before_count == after_count:
                    print(f"        ! 더보기 버튼 클릭 후 상품 개수 변화 없음 (이전: {before_count}, 현재: {after_count})")
                    return False
                else:
                    print(f"        더보기 버튼 클릭 성공: {before_count}개 → {after_count}개 (+{after_count - before_count}개)")
                
                return True
            except Exception as e:
                print(f"        ! 더보기 버튼 클릭 실패: {e}")
                return False
        return False

    # -------------------- 상품 카드 파싱 --------------------
    def parse_card(self, card):
        """상품 카드에서 정보 추출"""
        data = {
            "brand": None,
            "product_name": None,
            "price": None,
            "satisfaction_pct": None,
            "review_count": None,
            "wish_count": None,
            "tags": None,
            "product_url": None,
        }
        
        # 링크
        for sel in ["div.thumb_prd a.link_thumb[href]", "gc-link a.link_info[href]"]:
            try:
                href = card.find_element(By.CSS_SELECTOR, sel).get_attribute("href")
                if href:
                    data["product_url"] = urljoin(self.BASE, href)
                    break
            except NoSuchElementException:
                pass
        
        # 텍스트들
        try:
            data["brand"] = self.safe_text(card.find_element(By.CSS_SELECTOR, "span.brand_prd"))
        except NoSuchElementException:
            pass
        
        try:
            data["product_name"] = self.safe_text(card.find_element(By.CSS_SELECTOR, "strong.txt_prdname"))
        except NoSuchElementException:
            pass
        
        # 가격
        for sel in ["em.num_price", "span.price_info em.num_price"]:
            try:
                data["price"] = self.to_int(self.safe_text(card.find_element(By.CSS_SELECTOR, sel)))
                if data["price"] is not None:
                    break
            except NoSuchElementException:
                pass
        
        # 만족도/리뷰
        roots = [card]
        try:
            roots.insert(0, card.find_element(By.CSS_SELECTOR, "div.info_prd"))
        except NoSuchElementException:
            pass
        
        for r in roots:
            if data["satisfaction_pct"] is None:
                try:
                    s = self.safe_text(r.find_element(By.CSS_SELECTOR, "span.txt_star"))
                    m = re.search(r"(\d+)\s*%", s)
                    data["satisfaction_pct"] = int(m.group(1)) if m else None
                except NoSuchElementException:
                    pass
            
            if data["review_count"] is None:
                try:
                    rv = self.safe_text(r.find_element(By.CSS_SELECTOR, "span.txt_review"))
                    data["review_count"] = self.to_int(rv)
                except NoSuchElementException:
                    pass
        
        # 위시/태그
        try:
            data["wish_count"] = self.to_int(self.safe_text(card.find_element(By.CSS_SELECTOR, "span.num_wsh")))
        except NoSuchElementException:
            pass
        
        try:
            tags = [self.safe_text(t) for t in card.find_elements(By.CSS_SELECTOR, "span.tag_info span.tag_g")]
            data["tags"] = "|".join([t for t in tags if t]) if tags else None
        except NoSuchElementException:
            pass
        
        return data

    def parse_card_from_html(self, soup):
        """BeautifulSoup으로 HTML 스냅샷에서 카드 정보 파싱"""
        data = {
            "brand": None,
            "product_name": None,
            "price": None,
            "satisfaction_pct": None,
            "review_count": None,
            "wish_count": None,
            "tags": None,
            "product_url": None,
        }
        
        # 링크
        for sel in ["div.thumb_prd a.link_thumb[href]", "gc-link a.link_info[href]"]:
            link_elem = soup.select_one(sel)
            if link_elem and link_elem.get("href"):
                data["product_url"] = urljoin(self.BASE, link_elem.get("href"))
                break
        
        # 텍스트들
        brand_elem = soup.select_one("span.brand_prd")
        if brand_elem:
            data["brand"] = brand_elem.get_text(strip=True)
        
        name_elem = soup.select_one("strong.txt_prdname")
        if name_elem:
            data["product_name"] = name_elem.get_text(strip=True)
        
        # 가격
        for sel in ["em.num_price", "span.price_info em.num_price"]:
            price_elem = soup.select_one(sel)
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                data["price"] = self.to_int(price_text)
                if data["price"] is not None:
                    break
        
        # 만족도/리뷰
        info_prd = soup.select_one("div.info_prd")
        roots = [soup]
        if info_prd:
            roots.insert(0, info_prd)
        
        for root in roots:
            if data["satisfaction_pct"] is None:
                star_elem = root.select_one("span.txt_star")
                if star_elem:
                    star_text = star_elem.get_text(strip=True)
                    m = re.search(r"(\d+)\s*%", star_text)
                    data["satisfaction_pct"] = int(m.group(1)) if m else None
            
            if data["review_count"] is None:
                review_elem = root.select_one("span.txt_review")
                if review_elem:
                    review_text = review_elem.get_text(strip=True)
                    data["review_count"] = self.to_int(review_text)
        
        # 위시/태그
        wish_elem = soup.select_one("span.num_wsh")
        if wish_elem:
            data["wish_count"] = self.to_int(wish_elem.get_text(strip=True))
        
        tag_elems = soup.select("span.tag_info span.tag_g")
        if tag_elems:
            tags = [tag.get_text(strip=True) for tag in tag_elems if tag.get_text(strip=True)]
            data["tags"] = "|".join(tags) if tags else None
        
        return data

    # -------------------- 상품 수집 --------------------
    def collect_products_in_price_range(self, target_count, price_range, meta, seen_urls):
        """특정 가격대에서 목표 개수만큼 상품 수집 (목표 미달성 시에도 수집 중단)"""
        print(f"        가격대 '{price_range}'에서 {target_count}개 수집 시작...")
        
        collected_count = 0
        more_clicks = 0
        max_more_clicks = 20  # 무한 루프 방지
        consecutive_no_new_items = 0  # 연속으로 새 아이템이 없는 횟수
        
        while collected_count < target_count and more_clicks < max_more_clicks:
            # 현재 페이지의 상품 카드 찾기
            cards = self.driver.find_elements(By.CSS_SELECTOR, "ul.list_prd > li")
            
            if len(cards) == 0:
                # 다른 선택자 시도
                alternative_selectors = [
                    ".list_prd li",
                    ".list_product li", 
                    ".product_list li",
                    ".item_list li",
                    "[class*='product'] li",
                    "[class*='item'] li"
                ]
                for selector in alternative_selectors:
                    cards = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if len(cards) > 0:
                        print(f"        대체 선택자 '{selector}'로 {len(cards)}개 발견")
                        break
            
            if len(cards) == 0:
                print(f"        ! 상품 카드를 찾을 수 없음")
                break
            
            # HTML 스냅샷 수집
            html_snaps = []
            for i, c in enumerate(cards):
                try:
                    html_snaps.append(c.get_attribute("outerHTML"))
                except Exception as e:
                    print(f"          ! 카드 {i} HTML 추출 실패: {str(e)[:30]}...")
                    continue
            
            # 스냅샷 파싱
            new_items = []
            for i, html in enumerate(html_snaps):
                try:
                    soup = BeautifulSoup(html, "html.parser")
                    item = self.parse_card_from_html(soup)
                    url = item.get("product_url")
                    if not url or url in seen_urls:
                        continue
                    
                    # 메타데이터 추가
                    item.update(meta)
                    item["price_range"] = price_range
                    
                    new_items.append(item)
                    seen_urls.add(url)
                    
                    if collected_count + len(new_items) >= target_count:
                        break
                        
                except Exception as e:
                    print(f"          ! 스냅샷 {i} 파싱 실패: {str(e)[:50]}...")
                    continue
            
            # 수집된 아이템 추가
            self.all_rows.extend(new_items)
            collected_count += len(new_items)
            
            print(f"        진행: {collected_count}/{target_count}개 수집됨")
            
            # 목표 달성했으면 종료
            if collected_count >= target_count:
                break
            
            # 새 아이템이 없으면 카운트 증가
            if len(new_items) == 0:
                consecutive_no_new_items += 1
                if consecutive_no_new_items >= 3:  # 3번 연속으로 새 아이템이 없으면 중단
                    print(f"        연속으로 새 아이템이 없어서 수집 중단")
                    break
            else:
                consecutive_no_new_items = 0  # 새 아이템이 있으면 카운트 리셋
            
            # 더보기 버튼 클릭 시도
            if self.click_more_button():
                more_clicks += 1
                print(f"        더보기 버튼 클릭 ({more_clicks}번째)")
            else:
                print(f"        더보기 버튼이 없거나 클릭할 수 없음")
                break
        
        print(f"        ✓ 가격대 '{price_range}' 완료: {collected_count}개 수집")
        return collected_count

    def crawl_sub_category(self, top_category, sub_category):
        """하위 카테고리별 수집"""
        print(f"    [{top_category} > {sub_category}] 수집 시작")
        
        target_count = self.SUB_CATEGORY_TARGETS.get(sub_category, 75)
        print(f"    목표 수집 개수: {target_count}개")
        
        # 가격대 탭들 찾기
        price_tabs = self.get_price_tabs()
        if not price_tabs:
            print(f"    ! 가격대 탭을 찾을 수 없음")
            return
        
        print(f"    발견된 가격대 탭: {[tab[0] for tab in price_tabs]}")
        
        total_collected = 0
        seen_urls = set()
        
        # 각 가격대별로 수집
        for price_range, tab_element in price_tabs:
            print(f"      가격대 '{price_range}' 탭 클릭...")
            
            if not self.click_price_tab(tab_element):
                print(f"      ! 가격대 탭 클릭 실패: {price_range}")
                continue
            
            # 해당 가격대에서 수집
            meta = {
                "top_category": top_category,
                "sub_category": sub_category,
                "sub_tab": None
            }
            
            collected = self.collect_products_in_price_range(
                target_count, price_range, meta, seen_urls
            )
            total_collected += collected
            
            # 즉시 저장
            if collected > 0:
                recent_items = [item for item in self.all_rows[-collected:] if item.get("price_range") == price_range]
                self.save_to_database(recent_items, append_mode=True)
                print(f"      💾 {price_range} 저장 완료 ({collected}개)")
        
        print(f"    ✓ {sub_category} 완료: 총 {total_collected}개 수집")
        return total_collected

    # -------------------- 2) 크롤링 파이프라인 --------------------
    def scrape_data(self):
        self.driver.get(self.start_url)
        self.sleep(1.5)
        self.ensure_category_panel()
        self.all_rows = []

        # 와인/양주/전통주 상위 카테고리 클릭
        print("=== 와인/양주/전통주 카테고리 시작 ===")
        if not self.click_top_category("와인/양주/전통주"):
            print("! 와인/양주/전통주 상위 카테고리 클릭 실패")
            return []

        # 하위 카테고리들 순회
        sub_categories = ["와인", "양주", "맥주/기타"]
        
        for sub_category in sub_categories:
            print(f"\n--- {sub_category} 하위 카테고리 시작 ---")
            
            # 매 하위 카테고리 시작 시 메인 페이지로 리셋
            self.driver.get(self.DEFAULT_START)
            self.sleep(1.5)
            self.ensure_category_panel()
            
            # 상위 카테고리 클릭
            if not self.click_top_category("와인/양주/전통주"):
                print(f"! 상위 카테고리 클릭 실패")
                continue
            
            # 하위 카테고리 클릭
            if not self.click_sub_category(sub_category):
                print(f"! 하위 카테고리 클릭 실패: {sub_category}")
                continue
            
            # 해당 하위 카테고리 수집
            try:
                self.crawl_sub_category("와인/양주/전통주", sub_category)
            except Exception as e:
                print(f"! {sub_category} 수집 중 오류: {e}")

        print(f"\n=== 전체 크롤링 완료: {len(self.all_rows)}개 수집 ===")
        return self.all_rows

    # -------------------- 3) 저장 --------------------
    def save_to_database(self, data, append_mode=False):
        df = pd.DataFrame(data)
        if df.empty:
            print("수집 결과가 비었습니다.")
            return
        
        df.drop_duplicates(subset=["product_url"], inplace=True)
        cols = [
            "top_category", "sub_category", "sub_tab", "price_range",
            "brand", "product_name", "price",
            "satisfaction_pct", "review_count", "wish_count",
            "tags", "product_url",
        ]
        df = df[[c for c in cols if c in df.columns]]
        
        if append_mode and os.path.exists(self.output_path):
            # 기존 파일이 있으면 추가 모드로 저장
            df.to_csv(self.output_path, mode='a', header=False, index=False, encoding="utf-8-sig")
            print(f"추가 저장 완료: {self.output_path} (+{len(df)}개)")
        else:
            # 새 파일로 저장
            df.to_csv(self.output_path, index=False, encoding="utf-8-sig")
            print(f"저장 완료: {self.output_path} (총 {len(df)}개)")

    # -------------------- 파이프라인 --------------------
    def run(self):
        self.start_browser()
        try:
            data = self.scrape_data()
            # 마지막에 전체 데이터도 저장 (중복 제거 포함)
            if data:
                print(f"\n=== 전체 크롤링 완료: {len(data)}개 ===")
                self.save_to_database(data, append_mode=False)
        finally:
            self.close_browser()


# 직접 실행 시 사용할 메인 함수
def main():
    """와인/양주/전통주 카테고리 크롤링 실행"""
    
    # 출력 디렉토리 설정 (raw_data 폴더)
    current_dir = Path(__file__).parent
    output_dir = current_dir.parent / "raw_data"
    output_dir.mkdir(exist_ok=True)
    
    print("=== 카카오 기프트 와인/양주/전통주 크롤러 시작 ===")
    print(f"출력 디렉토리: {output_dir}")
    
    # 크롤러 초기화
    crawler = KakaoGiftLiquorCrawler(
        output_dir=str(output_dir),
        output_filename="kakao_gifts_liquor.csv",
        headless=False,  # 브라우저 창을 보면서 실행하려면 False
    )
    
    try:
        # 크롤링 실행
        crawler.run()
        print("\n=== 크롤링 완료 ===")
        
        # 결과 확인
        output_file = output_dir / "kakao_gifts_liquor.csv"
        if output_file.exists():
            import pandas as pd
            df = pd.read_csv(output_file)
            print(f"수집된 총 상품 수: {len(df)}개")
            print(f"파일 위치: {output_file}")
            
            # 카테고리별 통계
            if 'sub_category' in df.columns:
                print("\n카테고리별 수집 현황:")
                category_stats = df['sub_category'].value_counts()
                for category, count in category_stats.items():
                    print(f"  {category}: {count}개")
            
            if 'price_range' in df.columns:
                print("\n가격대별 수집 현황:")
                price_stats = df['price_range'].value_counts()
                for price_range, count in price_stats.items():
                    print(f"  {price_range}: {count}개")
        else:
            print("! 출력 파일이 생성되지 않았습니다.")
            
    except KeyboardInterrupt:
        print("\n! 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n! 크롤링 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n=== 크롤러 종료 ===")


if __name__ == "__main__":
    from pathlib import Path
    main()
