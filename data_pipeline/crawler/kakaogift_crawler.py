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

from .base_crawler import BaseCrawler


class KakaoGiftCrawler(BaseCrawler):
    """
    (예외 전부 제외) 일반 카테고리 전용 크롤러
    - 상위 카테고리: 지정 목록 중 '와인/양주/전통주', '아티스트/캐릭터'는 건너뜀
    - 흐름: 상위 카테고리 → 하위 카테고리 → (상품 탭 활성화) → 카드 목록 수집
    - 카드 필드: brand, product_name, price, satisfaction_pct, review_count,
                wish_count, tags, product_url
    """

    BASE = "https://gift.kakao.com"
    DEFAULT_START = "https://gift.kakao.com/home?categoryLayer=OPEN"

    # 동작 파라미터
    SCROLL_PAUSE = 0.8
    CLICK_PAUSE = 0.5
    MAX_SCROLL_TRIES = 50  # 300개가 모일 때까지 충분히 스크롤
    IMPLICIT_WAIT = 5

    # 상위 카테고리 화이트리스트(요청 목록)
    TOP_WHITELIST = [
        "교환권", "상품권", "뷰티", "패션", "식품",
        "와인/양주/전통주", "리빙/도서", "레저/스포츠",
        "아티스트/캐릭터", "유아동/반려", "디지털/가전", "카카오프렌즈",
    ]
    # 이번 라운드에서는 전부 제외
    TOP_EXCLUDE_FOR_NOW = {"와인/양주/전통주", "아티스트/캐릭터"}

    # 일반적으로 숨길 서브카테고리
    EXCLUDE_SUBCATS = {
        "요즘 뷰티", "포인트적립 브랜드", "신규입점 브랜드", "MD추천", "신규입점",
        "추천", "계절가전", "전체", "와인/양주/맥주"
    }

    def __init__(
        self,
        output_dir: str,
        *,
        output_filename: str = "kakao_gifts.csv",
        headless: bool = False,
        items_per_subcat: int = 300,
        start_url: str | None = None,
        top_filter: str | None = None,
    ):
        super().__init__(output_dir=output_dir)
        self.output_path = os.path.join(output_dir, output_filename)

        self.headless = headless
        self.items_per_subcat = items_per_subcat
        self.start_url = start_url or self.DEFAULT_START
        self.top_filter = top_filter

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

    # -------------------- 카테고리 패널 / 헤더 버튼 --------------------
    def category_panel_open(self) -> bool:
        """카테고리 패널(좌측 레이어)이 열려 있는지 확인"""
        try:
            self.driver.find_element(By.CSS_SELECTOR, ".category_layer .list_ctgmenu")
            return True
        except NoSuchElementException:
            return False

    def click_header_category_button(self) -> bool:
        """
        헤더의 '카테고리' 버튼을 눌러 패널을 연다.
        다양한 마크업 케이스를 대비해 여러 셀렉터를 시도.
        """
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
                self.wait(0.6)
                if self.category_panel_open():
                    return True
            except (NoSuchElementException, StaleElementReferenceException):
                continue
        return False

    def ensure_category_panel(self):
        """
        카테고리 패널을 반드시 연다.
        - 헤더 버튼 클릭 시도
        - 그래도 실패하면 OPEN 파라미터 페이지로 강복귀
        """
        if self.category_panel_open():
            return
        if not self.click_header_category_button():
            self.driver.get(self.DEFAULT_START)
            self.wait(1.0)

    # -------------------- 카테고리/탭 핸들러 --------------------
    def list_top_categories(self):
        els = self.driver.find_elements(
            By.CSS_SELECTOR,
            ".category_layer .list_ctgmenu a.link_menu span.txt_menu",
        )
        return [self.safe_text(e) for e in els if self.safe_text(e)]

    def click_top_category(self, name):
        els = self.driver.find_elements(
            By.CSS_SELECTOR, ".category_layer .list_ctgmenu a.link_menu"
        )
        for a in els:
            txt = self.safe_text(a.find_element(By.CSS_SELECTOR, "span.txt_menu"))
            if txt == name:
                self.js_click(a)
                self.wait(0.8)
                return True
        return False

    def list_sub_categories(self):
        els = self.driver.find_elements(
            By.CSS_SELECTOR, ".category_layer .list_ctgsub a.link_menu span.txt_menu"
        )
        return [self.safe_text(e) for e in els if self.safe_text(e)]

    def click_sub_category(self, name):
        els = self.driver.find_elements(
            By.CSS_SELECTOR, ".category_layer .list_ctgsub a.link_menu"
        )
        for a in els:
            txt = self.safe_text(a.find_element(By.CSS_SELECTOR, "span.txt_menu"))
            if txt == name:
                self.js_click(a)
                self.wait(1.0)
                return True
        return False

    # -------------------- 페이지 내 하위 카테고리 버튼 제어 --------------------
    def list_page_sub_categories(self):
        """페이지 내 하위 카테고리 버튼들을 찾아서 텍스트 리스트 반환"""
        els = self.driver.find_elements(
            By.CSS_SELECTOR, 
            ".list_ctgmain li a.link_ctg, .wrap_depth1 a.link_ctg, .area_ctglist a.link_ctg"
        )
        subcats = []
        for el in els:
            try:
                # data-tiara-copy 속성에서 카테고리명 가져오기
                text = el.get_attribute("data-tiara-copy")
                if not text:
                    # 직접 텍스트에서 가져오기
                    text = self.safe_text(el)
                if text and text not in subcats:
                    subcats.append(text)
            except Exception:
                continue
        return subcats

    def click_page_sub_category(self, name):
        """페이지 내 하위 카테고리 버튼 클릭"""
        els = self.driver.find_elements(
            By.CSS_SELECTOR, 
            ".list_ctgmain li a.link_ctg, .wrap_depth1 a.link_ctg, .area_ctglist a.link_ctg"
        )
        for el in els:
            try:
                # data-tiara-copy 속성 확인
                text = el.get_attribute("data-tiara-copy")
                if not text:
                    # 직접 텍스트 확인
                    text = self.safe_text(el)
                if text == name:
                    self.js_click(el)
                    self.wait(1.0)
                    return True
            except Exception:
                continue
        return False

    def scroll_to_top(self):
        """페이지 맨 위로 스크롤"""
        self.driver.execute_script("window.scrollTo(0, 0);")
        self.wait(0.5)

    # --- 페이지 내 '상품/브랜드' 탭 제어 ---
    def ensure_product_tab(self):
        """
        일반 카테고리에서 '상품' 탭을 활성화한다.
        """
        candidate_a = self.driver.find_elements(
            By.CSS_SELECTOR,
            ".wrap_srchtab a.link_tab, "
            ".module_wrapper .module_tab .rail_cate a.link_tab, "
            ".rail_cate a.link_tab"
        )
        for a in candidate_a:
            try:
                label = self.safe_text(
                    a.find_element(By.CSS_SELECTOR, "span.txt_tab, span.txt_g, span")
                )
            except NoSuchElementException:
                label = self.safe_text(a)
            if label == "상품":
                self.js_click(a)
                return True
        spans = self.driver.find_elements(
            By.CSS_SELECTOR, "span.txt_tab, span.txt_g, .tablist span"
        )
        for s in spans:
            if self.safe_text(s) == "상품":
                self.js_click(s)
                return True
        return False

    # -------------------- 스크롤/카드 파싱 --------------------
    def scroll_until_cards(self, min_cards, list_selector="ul.list_prd > li"):
        print(f"        카드 로딩 대기 중...")
        for i in range(6):
            cards = self.driver.find_elements(By.CSS_SELECTOR, list_selector)
            if len(cards) > 0:
                print(f"        ✓ 카드 발견: {len(cards)}개")
                break
            print(f"        대기 {i+1}/6...")
            self.wait(0.6)

        last_len, still = 0, 0
        tries = 0
        while True:
            cards = self.driver.find_elements(By.CSS_SELECTOR, list_selector)
            if len(cards) >= min_cards:
                print(f"        스크롤 완료: {len(cards)}개 (목표: {min_cards}개 달성)")
                break
            if tries >= self.MAX_SCROLL_TRIES:
                print(f"        스크롤 완료: {len(cards)}개 (최대 시도 횟수 도달)")
                break
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            self.wait(self.SCROLL_PAUSE)
            cards2 = self.driver.find_elements(By.CSS_SELECTOR, list_selector)
            still = still + 1 if len(cards2) == last_len else 0
            last_len = len(cards2)
            tries += 1
            if tries % 3 == 0:
                print(f"        스크롤 {tries}: {len(cards2)}개 (목표: {min_cards}개)")
            if still >= 4:
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

    def collect_current_page(self, want_total, meta, seen_urls, already_collected=0):
        print(f"      스크롤 시작 (목표: {want_total}개)...")
        self.scroll_until_cards(max(already_collected, want_total))
        
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

    # -------------------- 수집기(일반만) --------------------
    def crawl_general(self, top, sub, n):
        """일반: (상품 탭으로 전환한 뒤) 카드에서 n개 목표(부족하면 있는 만큼)"""
        print(f"[{top} > {sub}] 일반 수집(상품 탭)")
        
        # 상품 탭 활성화 시도
        print(f"    상품 탭 활성화 시도...")
        if self.ensure_product_tab():
            print(f"    ✓ 상품 탭 활성화 성공")
        else:
            print(f"    ! 상품 탭 활성화 실패, 현재 탭으로 진행")
        
        # 현재 페이지 URL 확인
        current_url = self.driver.current_url
        print(f"    현재 URL: {current_url}")
        
        # 상품 카드 수집
        print(f"    상품 카드 수집 시작 (목표: {n}개)...")
        rows, collected = self.collect_current_page(
            n,
            {"top_category": top, "sub_category": sub, "sub_tab": None},
            set(),
            0,
        )
        print(f"    ✓ 수집 완료: {collected}개")
        return rows

    # -------------------- 2) 크롤링 파이프라인 --------------------
    def scrape_data(self):
        self.driver.get(self.start_url)
        self.wait(1.2)
        self.ensure_category_panel()
        self.all_rows = []

        avail_tops = self.list_top_categories()
        # 요청한 12개 중 현재 라운드에서 제외할 2개 제거
        avail_tops = [t for t in avail_tops if t in self.TOP_WHITELIST and t not in self.TOP_EXCLUDE_FOR_NOW]
        if self.top_filter:
            allow = [t.strip() for t in self.top_filter.split(",")]
            target_tops = [t for t in avail_tops if t in allow]
        else:
            target_tops = avail_tops

        print("대상 상위 카테고리(예외 제외):", target_tops)

        for top in target_tops:
            print(f"\n=== 상위 카테고리 시작: {top} ===")
            
            # 매 상위 카테고리 진입 시 항상 OPEN 페이지로 리셋
            self.driver.get(self.DEFAULT_START)
            self.wait(1.2)
            
            # 패널 열기 보장
            self.ensure_category_panel()
            print(f"  상위 카테고리 '{top}' 클릭 시도...")
            if not self.click_top_category(top):
                print(f"  ! 상위 카테고리 클릭 실패: {top}")
                continue
            print(f"  ✓ 상위 카테고리 '{top}' 클릭 성공")

            # 패널에서 하위 카테고리 목록 가져오기
            panel_subcats = self.list_sub_categories()
            if not panel_subcats:
                print(f"  ! 하위 카테고리 없음: {top}")
                continue

            # 첫 번째 하위 카테고리로 이동 (패널에서 클릭)
            first_sub = None
            for sub in panel_subcats:
                if sub not in self.EXCLUDE_SUBCATS:
                    first_sub = sub
                    break
            
            if not first_sub:
                print(f"  ! 유효한 하위 카테고리 없음: {top}")
                continue

            if not self.click_sub_category(first_sub):
                print(f"    ! 첫 번째 하위 카테고리 클릭 실패: {first_sub}")
                continue

            # 첫 번째 하위 카테고리 수집
            try:
                rows = self.crawl_general(top, first_sub, self.items_per_subcat)
                self.all_rows.extend(rows)
                print(f"    ✓ {first_sub}: {len(rows)}개 수집")
                # 첫 번째 하위 카테고리 즉시 저장
                if rows:
                    self.save_to_database(rows, append_mode=True)
                    print(f"    💾 {first_sub} 저장 완료")
            except Exception as e:
                print(f"    ! {first_sub} 수집 실패: {e}")

            # 페이지 내 하위 카테고리 버튼들로 나머지 순회
            page_subcats = self.list_page_sub_categories()
            print(f"    페이지 내 하위 카테고리 발견: {len(page_subcats)}개")
            
            for sub in page_subcats:
                if sub in self.EXCLUDE_SUBCATS or sub == first_sub:
                    continue
                
                # 페이지 위로 스크롤
                self.scroll_to_top()
                self.wait(0.5)
                
                # 페이지 내 버튼 클릭
                if not self.click_page_sub_category(sub):
                    print(f"      ! 페이지 내 하위 카테고리 클릭 실패: {sub}")
                    continue

                # 수집
                try:
                    rows = self.crawl_general(top, sub, self.items_per_subcat)
                    self.all_rows.extend(rows)
                    print(f"      ✓ {sub}: {len(rows)}개 수집")
                    # 각 하위 카테고리 즉시 저장
                    if rows:
                        self.save_to_database(rows, append_mode=True)
                        print(f"      💾 {sub} 저장 완료")
                except Exception as e:
                    print(f"      ! {sub} 수집 실패: {e}")

            print(f"\n=== {top} 카테고리 완료: {len([r for r in self.all_rows if r.get('top_category') == top])}개 수집 ===")
            print(f"=== {top} 카테고리 저장 완료 ===\n")

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
