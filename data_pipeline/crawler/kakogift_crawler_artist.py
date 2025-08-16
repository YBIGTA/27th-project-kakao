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
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

from base_crawler import BaseCrawler


class KakaoGiftArtistCrawler(BaseCrawler):
    """
    아티스트/캐릭터 카테고리 전용 크롤러
    - 상위 카테고리: 아티스트/캐릭터
    - 하위 카테고리: 스타앨범, 애니메이션 캐릭터, 인디작가, 애니멀캐릭터, 웹소설, 게임
    - 흐름: 상위 카테고리 → 하위 카테고리 → 전체 탭 → 카드 목록 수집
    - 카드 필드: brand, product_name, price, satisfaction_pct, review_count,
                wish_count, tags, product_url
    """

    BASE = "https://gift.kakao.com"
    DEFAULT_START = "https://gift.kakao.com/home?categoryLayer=OPEN"

    # 동작 파라미터
    SCROLL_PAUSE = 0.8
    CLICK_PAUSE = 0.5
    MAX_SCROLL_TRIES = 100  # 300개가 모일 때까지 충분히 스크롤
    IMPLICIT_WAIT = 5

    # 아티스트/캐릭터 하위 카테고리 목록
    ARTIST_SUBCATEGORIES = [
        "스타앨범",
        "애니메이션 캐릭터", 
        "인디작가",
        "애니멀캐릭터",
        "웹소설",
        "게임"
    ]

    def __init__(
        self,
        output_dir: str,
        *,
        output_filename: str = "kakao_gifts_artist.csv",
        headless: bool = False,
        items_per_subcat: int = 300,
        start_url: str | None = None,
    ):
        super().__init__(output_dir=output_dir)
        self.output_path = os.path.join(output_dir, output_filename)

        self.headless = headless
        self.items_per_subcat = items_per_subcat
        self.start_url = start_url or self.DEFAULT_START

        self.driver = None
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

    # -------------------- 유틸 --------------------
    def wait(self, t=None):
        time.sleep(t if t is not None else self.SCROLL_PAUSE)

    def js_click(self, el):
        try:
            self.driver.execute_script("arguments[0].click();", el)
        except WebDriverException:
            try:
                el.click()
            except Exception:
                pass
        self.wait(self.CLICK_PAUSE)

    def safe_text(self, el):
        try:
            return el.text.strip()
        except Exception:
            return ""

    def to_int(self, text):
        if not text:
            return None
        try:
            return int(re.sub(r"[^\d]", "", text))
        except ValueError:
            return None

    # -------------------- 카테고리 네비게이션 --------------------
    def ensure_category_panel(self):
        """카테고리 패널이 열려있는지 확인하고, 안 열려있으면 열기"""
        try:
            # 이미 열려있는지 확인
            panel = self.driver.find_element(By.CSS_SELECTOR, ".category_layer")
            if panel.is_displayed():
                return True
        except NoSuchElementException:
            pass
        
        # 패널 열기 시도
        try:
            menu_btn = self.driver.find_element(By.CSS_SELECTOR, ".btn_menu, .link_menu, .btn_category")
            self.js_click(menu_btn)
            self.wait(1.0)
            return True
        except NoSuchElementException:
            # URL에 categoryLayer=OPEN이 있으면 이미 열려있는 상태
            if "categoryLayer=OPEN" in self.driver.current_url:
                print("    카테고리 패널이 이미 열려있는 상태입니다.")
                return True
            print("    ! 카테고리 메뉴 버튼을 찾을 수 없습니다.")
            return False

    def click_top_category(self, name):
        """상위 카테고리 클릭 (아티스트/캐릭터)"""
        try:
            # 여러 선택자 시도
            selectors = [
                ".category_layer .list_ctgmain a.link_ctg",
                ".category_layer a.link_ctg",
                ".list_ctgmain a.link_ctg",
                ".category_layer a[data-tiara-copy]"
            ]
            
            for selector in selectors:
                els = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for a in els:
                    try:
                        # data-tiara-copy 속성 확인
                        tiara_copy = a.get_attribute("data-tiara-copy")
                        if tiara_copy == name:
                            self.js_click(a)
                            self.wait(1.0)
                            return True
                        
                        # 텍스트 확인
                        txt = self.safe_text(a.find_element(By.CSS_SELECTOR, "span.txt_ctg"))
                        if txt == name:
                            self.js_click(a)
                            self.wait(1.0)
                            return True
                    except NoSuchElementException:
                        # 직접 텍스트 확인
                        if self.safe_text(a) == name:
                            self.js_click(a)
                            self.wait(1.0)
                            return True
        except Exception as e:
            print(f"    ! 상위 카테고리 클릭 중 오류: {e}")
        return False

    def click_sub_category(self, name):
        """하위 카테고리 클릭 (패널에서)"""
        try:
            els = self.driver.find_elements(
                By.CSS_SELECTOR, ".category_layer .list_ctgsub a.link_menu"
            )
            for a in els:
                txt = self.safe_text(a.find_element(By.CSS_SELECTOR, "span.txt_menu"))
                if txt == name:
                    self.js_click(a)
                    self.wait(1.0)
                    return True
        except Exception as e:
            print(f"    ! 하위 카테고리 클릭 중 오류: {e}")
        return False

    def click_all_tab(self):
        """전체 탭 클릭"""
        try:
            # 탭 버튼들 찾기
            tab_buttons = self.driver.find_elements(
                By.CSS_SELECTOR, 
                ".module_tab a.link_tab, .rail_cate a.link_tab, a[role='tab']"
            )
            
            for tab in tab_buttons:
                try:
                    # 텍스트 확인
                    text_elem = tab.find_element(By.CSS_SELECTOR, "span.txt_tab, span")
                    if self.safe_text(text_elem) == "전체":
                        self.js_click(tab)
                        self.wait(1.0)
                        return True
                except NoSuchElementException:
                    # 직접 텍스트 확인
                    if self.safe_text(tab) == "전체":
                        self.js_click(tab)
                        self.wait(1.0)
                        return True
        except Exception as e:
            print(f"    ! 전체 탭 클릭 중 오류: {e}")
        return False

    def click_more_button(self):
        """더보기 버튼 클릭"""
        try:
            # 여러 선택자 시도
            selectors = [
                "button.btn_more",
                ".btn_more", 
                "button[data-tiara-copy*='더보기']",
                "button[data-tiara-copy*='상품 더보기']",
                ".button.btn_more",
                "button[class*='btn_more']"
            ]
            
            for selector in selectors:
                try:
                    more_btn = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if more_btn.is_displayed():
                        print(f"        더보기 버튼 발견: {selector}")
                        self.js_click(more_btn)
                        self.wait(1.5)  # 더보기 버튼 클릭 후 대기 시간 증가
                        return True
                except NoSuchElementException:
                    continue
        except Exception as e:
            print(f"        ! 더보기 버튼 클릭 중 오류: {e}")
        return False

    def scroll_to_top(self):
        """페이지 맨 위로 스크롤"""
        self.driver.execute_script("window.scrollTo(0, 0);")
        self.wait(0.5)

    # -------------------- 스크롤/카드 파싱 --------------------
    def scroll_until_cards(self, min_cards, has_more_button=False, subcategory=None):
        print(f"        카드 로딩 대기 중...")
        for i in range(6):
            cards = self.driver.find_elements(By.CSS_SELECTOR, "ul.list_prd > li")
            if len(cards) > 0:
                print(f"        ✓ 카드 발견: {len(cards)}개")
                break
            print(f"        대기 {i+1}/6...")
            self.wait(0.6)

        # 스타앨범은 이전 방식 (더보기 버튼 여러 번 클릭)
        if has_more_button and subcategory == "스타앨범":
            print(f"        스타앨범: 더보기 버튼 여러 번 클릭 방식")
            last_len, still = 0, 0
            tries = 0
            more_clicks = 0
            
            while True:
                cards = self.driver.find_elements(By.CSS_SELECTOR, "ul.list_prd > li")
                if len(cards) >= min_cards:
                    print(f"        스크롤 완료: {len(cards)}개 (목표: {min_cards}개 달성)")
                    break
                if tries >= self.MAX_SCROLL_TRIES:
                    print(f"        스크롤 완료: {len(cards)}개 (최대 시도 횟수 도달)")
                    break
                    
                # 더보기 버튼이 있는 경우 클릭
                if more_clicks < 20:  # 더보기 버튼 클릭 제한
                    if self.click_more_button():
                        more_clicks += 1
                        print(f"        더보기 버튼 클릭 {more_clicks}회")
                        self.wait(1.0)
                        continue
                
                # 스크롤
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                self.wait(self.SCROLL_PAUSE)
                
                cards2 = self.driver.find_elements(By.CSS_SELECTOR, "ul.list_prd > li")
                still = still + 1 if len(cards2) == last_len else 0
                last_len = len(cards2)
                tries += 1
                
                if tries % 5 == 0:
                    print(f"        스크롤 {tries}: {len(cards2)}개 (목표: {min_cards}개)")
                if still >= 6:
                    print(f"        더 이상 새로운 카드가 로드되지 않음")
                    break
        
        # 게임은 현재 방식 (더보기 버튼 한 번만 클릭)
        elif has_more_button and subcategory == "게임":
            print(f"        게임: 더보기 버튼 한 번만 클릭 방식")
            initial_cards = len(self.driver.find_elements(By.CSS_SELECTOR, "ul.list_prd > li"))
            if self.click_more_button():
                print(f"        ✓ 더보기 버튼 클릭 성공, 추가 로딩 대기...")
                self.wait(3.0)  # 더보기 버튼 클릭 후 충분히 대기
                
                # 더보기 버튼 클릭 후 스크롤을 몇 번 더 해서 모든 콘텐츠 로드
                print(f"        더보기 버튼 클릭 후 추가 스크롤...")
                for i in range(5):
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    self.wait(1.0)
                    current_cards = len(self.driver.find_elements(By.CSS_SELECTOR, "ul.list_prd > li"))
                    print(f"        추가 스크롤 {i+1}: {current_cards}개")
                
                # 최종 카드 수 확인
                final_cards = len(self.driver.find_elements(By.CSS_SELECTOR, "ul.list_prd > li"))
                print(f"        더보기 버튼 클릭 후 최종: {initial_cards}개 → {final_cards}개")
            else:
                print(f"        ! 더보기 버튼 클릭 실패")
            
            # 스크롤하면서 모든 카드 로드
            last_len, still = 0, 0
            tries = 0
            
            while True:
                cards = self.driver.find_elements(By.CSS_SELECTOR, "ul.list_prd > li")
                if len(cards) >= min_cards:
                    print(f"        스크롤 완료: {len(cards)}개 (목표: {min_cards}개 달성)")
                    break
                if tries >= self.MAX_SCROLL_TRIES:
                    print(f"        스크롤 완료: {len(cards)}개 (최대 시도 횟수 도달)")
                    break
                
                # 스크롤
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                self.wait(self.SCROLL_PAUSE)
                
                cards2 = self.driver.find_elements(By.CSS_SELECTOR, "ul.list_prd > li")
                still = still + 1 if len(cards2) == last_len else 0
                last_len = len(cards2)
                tries += 1
                
                if tries % 5 == 0:
                    print(f"        스크롤 {tries}: {len(cards2)}개 (목표: {min_cards}개)")
                if still >= 6:
                    print(f"        더 이상 새로운 카드가 로드되지 않음")
                    break
        
        # 다른 카테고리는 스크롤만
        else:
            print(f"        {subcategory}: 스크롤만 방식")
            last_len, still = 0, 0
            tries = 0
            
            while True:
                cards = self.driver.find_elements(By.CSS_SELECTOR, "ul.list_prd > li")
                if len(cards) >= min_cards:
                    print(f"        스크롤 완료: {len(cards)}개 (목표: {min_cards}개 달성)")
                    break
                if tries >= self.MAX_SCROLL_TRIES:
                    print(f"        스크롤 완료: {len(cards)}개 (최대 시도 횟수 도달)")
                    break
                
                # 스크롤
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                self.wait(self.SCROLL_PAUSE)
                
                cards2 = self.driver.find_elements(By.CSS_SELECTOR, "ul.list_prd > li")
                still = still + 1 if len(cards2) == last_len else 0
                last_len = len(cards2)
                tries += 1
                
                if tries % 5 == 0:
                    print(f"        스크롤 {tries}: {len(cards2)}개 (목표: {min_cards}개)")
                if still >= 6:
                    print(f"        더 이상 새로운 카드가 로드되지 않음")
                    break

    def parse_card(self, card):
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
        for sel in ["div.thumb_prd a.link_thumb[href]", "gc-link a.link_info[href]", "a[href]"]:
            try:
                href = card.find_element(By.CSS_SELECTOR, sel).get_attribute("href")
                if href and "/product/" in href:
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
        for sel in ["em.num_price", "span.price_info em.num_price", ".price em", ".price_info em"]:
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
        for sel in ["div.thumb_prd a.link_thumb[href]", "gc-link a.link_info[href]", "a[href]"]:
            link_elem = soup.select_one(sel)
            if link_elem and link_elem.get("href") and "/product/" in link_elem.get("href"):
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
        for sel in ["em.num_price", "span.price_info em.num_price", ".price em", ".price_info em"]:
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

    def collect_current_page(self, want_total, meta, seen_urls, has_more_button=False, already_collected=0):
        print(f"      스크롤 시작 (목표: {want_total}개, 더보기버튼: {'있음' if has_more_button else '없음'})...")
        subcategory = meta.get("sub_category")
        self.scroll_until_cards(max(already_collected, want_total), has_more_button, subcategory)
        
        # 상품 카드 찾기
        cards = self.driver.find_elements(By.CSS_SELECTOR, "ul.list_prd > li")
        print(f"      발견된 상품 카드: {len(cards)}개")
        
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
                    print(f"      대체 선택자 '{selector}'로 {len(cards)}개 발견")
                    break
        
        # 스냅샷 파싱으로 stale 근본 차단
        print(f"      HTML 스냅샷 수집 중...")
        html_snaps = []
        for i, c in enumerate(cards):
            try:
                html_snaps.append(c.get_attribute("outerHTML"))
            except Exception as e:
                print(f"        ! 카드 {i} HTML 추출 실패: {str(e)[:30]}...")
                continue
        
        print(f"      스냅샷 {len(html_snaps)}개 수집 완료, 파싱 시작...")
        
        rows, new_count = [], 0
        for i, html in enumerate(html_snaps):
            try:
                soup = BeautifulSoup(html, "html.parser")
                item = self.parse_card_from_html(soup)
                url = item.get("product_url")
                if not url or url in seen_urls:
                    continue
                rows.append({**item, **meta})
                seen_urls.add(url)
                new_count += 1
                if already_collected + new_count >= want_total:
                    break
                if new_count % 10 == 0:
                    print(f"        진행: {new_count}개 수집됨")
            except Exception as e:
                print(f"        ! 스냅샷 {i} 파싱 실패: {str(e)[:50]}...")
                continue
        
        print(f"      최종 수집: {new_count}개")
        return rows, new_count

    # -------------------- 수집기 --------------------
    def crawl_subcategory(self, subcategory, n):
        """하위 카테고리 수집"""
        print(f"[아티스트/캐릭터 > {subcategory}] 수집 시작")
        
        # 전체 탭 활성화
        print(f"    전체 탭 활성화 시도...")
        if self.click_all_tab():
            print(f"    ✓ 전체 탭 활성화 성공")
        else:
            print(f"    ! 전체 탭 활성화 실패, 현재 탭으로 진행")
        
        # 현재 페이지 URL 확인
        current_url = self.driver.current_url
        print(f"    현재 URL: {current_url}")
        
        # 더보기 버튼 유무 확인
        has_more_button = False
        try:
            more_btn = self.driver.find_element(By.CSS_SELECTOR, "button.btn_more, .btn_more")
            has_more_button = more_btn.is_displayed()
        except NoSuchElementException:
            pass
        
        print(f"    더보기 버튼: {'있음' if has_more_button else '없음'}")
        
        # 상품 카드 수집
        print(f"    상품 카드 수집 시작 (목표: {n}개)...")
        rows, collected = self.collect_current_page(
            n,
            {"top_category": "아티스트/캐릭터", "sub_category": subcategory, "sub_tab": "전체"},
            set(),
            has_more_button,
            0,
        )
        print(f"    ✓ 수집 완료: {collected}개")
        return rows

    # -------------------- 2) 크롤링 파이프라인 --------------------
    def scrape_data(self):
        self.driver.get(self.start_url)
        self.wait(1.2)
        self.all_rows = []

        for subcategory in self.ARTIST_SUBCATEGORIES:
            print(f"\n=== 하위 카테고리 시작: {subcategory} ===")
            
            # 매 하위 카테고리 진입 시 항상 OPEN 페이지로 리셋
            self.driver.get(self.DEFAULT_START)
            self.wait(1.2)
            
            # 패널 열기 보장
            self.ensure_category_panel()
            
            # 아티스트/캐릭터 상위 카테고리 클릭
            print(f"  상위 카테고리 '아티스트/캐릭터' 클릭 시도...")
            if not self.click_top_category("아티스트/캐릭터"):
                print(f"  ! 상위 카테고리 클릭 실패: 아티스트/캐릭터")
                continue
            print(f"  ✓ 상위 카테고리 '아티스트/캐릭터' 클릭 성공")

            # 하위 카테고리 클릭
            print(f"  하위 카테고리 '{subcategory}' 클릭 시도...")
            if not self.click_sub_category(subcategory):
                print(f"    ! 하위 카테고리 클릭 실패: {subcategory}")
                continue
            print(f"    ✓ 하위 카테고리 '{subcategory}' 클릭 성공")

            # 하위 카테고리 수집
            try:
                rows = self.crawl_subcategory(subcategory, self.items_per_subcat)
                self.all_rows.extend(rows)
                print(f"    ✓ {subcategory}: {len(rows)}개 수집")
                # 각 하위 카테고리 즉시 저장
                if rows:
                    self.save_to_database(rows, append_mode=True)
                    print(f"    💾 {subcategory} 저장 완료")
            except Exception as e:
                print(f"    ! {subcategory} 수집 실패: {e}")

            print(f"\n=== {subcategory} 카테고리 완료: {len([r for r in self.all_rows if r.get('sub_category') == subcategory])}개 수집 ===")

        return self.all_rows

    # -------------------- 3) 저장 --------------------
    def save_to_database(self, data, append_mode=False):
        df = pd.DataFrame(data)
        if df.empty:
            print("수집 결과가 비었습니다.")
            return
        
        df.drop_duplicates(subset=["product_url"], inplace=True)
        cols = [
            "top_category", "sub_category", "sub_tab",
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


# 직접 실행 코드
if __name__ == "__main__":
    import os
    output_dir = os.path.join(os.getcwd(), "..", "raw_data")
    crawler = KakaoGiftArtistCrawler(
        output_dir=output_dir,
        output_filename="kakao_gifts_artist.csv",
        headless=False,
        items_per_subcat=300,
        start_url=None
    )
    crawler.run()
