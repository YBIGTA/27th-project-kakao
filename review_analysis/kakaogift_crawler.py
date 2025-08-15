# -*- coding: utf-8 -*-
import os
import re
import time
from urllib.parse import urljoin, urlsplit, urlunsplit, parse_qsl, urlencode

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

from base_crawler import BaseCrawler


class KakaoGiftCrawler(BaseCrawler):
    """
    일반: 서브카테고리 클릭 → (상품 탭 진입) → 카드에서 데이터 수집
    예외1: 와인/양주/전통주: (가격 등) 탭을 '전체' 제외하고 균등 분배 + 부족분 재분배
    예외2: 아티스트/캐릭터: 서브카테고리별로 '전체'(+일부는 '주간 베스트') 제외 후
          균등 분배 + 부족분 재분배
    카드에서 수집: brand, product_name, price, satisfaction_pct, review_count, wish_count, tags, product_url
    """

    BASE = "https://gift.kakao.com"
    DEFAULT_START = (
        "https://gift.kakao.com/home?"
        "targetType=ALL&rankType=MANY_RECEIVE&priceRange=20000_29999"
    )

    SCROLL_PAUSE = 0.8
    CLICK_PAUSE = 0.5
    MAX_SCROLL_TRIES = 18
    IMPLICIT_WAIT = 5

    # 상위 카테고리 화이트리스트 (이 순서대로만 순회)
    TOP_WHITELIST = [
        "교환권",
        "상품권",
        "뷰티",
        "패션",
        "식품",
        "와인/양주/전통주",
        "리빙/도서",
        "레저/스포츠",
        "아티스트/캐릭터",
        "유아동/반려",
        "디지털/가전",
        "카카오프렌즈",
    ]

    # 숨길 서브카테고리(특수 영역)
    EXCLUDE_SUBCATS = {
        "요즘 뷰티", "포인트적립 브랜드", "신규입점 브랜드", "MD추천", "신규입점"
    }

    # 항상 제외할 일반 탭명
    EXCLUDE_TABS_GENERIC = {"전체"}

    # 아티스트/캐릭터 하위별 제외 탭
    EXCLUDE_TABS_BY_SUBCAT = {
        "스타앨범": {"전체"},
        "애니메이션 캐릭터": {"전체", "주간 베스트"},
        "인디 작가": {"전체", "주간 베스트"},
        "애니멀 캐릭터": {"전체", "주간 베스트"},
        "웹소설": {"전체", "주간 베스트"},
        "게임": {"전체"},
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
        self.all_rows: list[dict] = []

    # -------------------- 1) 브라우저 --------------------
    def start_browser(self):
        opts = Options()
        if self.headless:
            opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1440,1000")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--lang=ko-KR")
        # 자동화 배너 숨김 최소화
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option('useAutomationExtension', False)

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=opts)
        self.driver.implicitly_wait(self.IMPLICIT_WAIT)

    # -------------------- 유틸 --------------------
    def wait(self, t=None): time.sleep(t if t is not None else self.SCROLL_PAUSE)
    def js_click(self, el):
        self.driver.execute_script("arguments[0].click();", el)
        self.wait(self.CLICK_PAUSE)
    @staticmethod
    def safe_text(el):
        try: return el.text.strip()
        except Exception: return ""

    @staticmethod
    def to_int(text):
        if not text:
            return None
        s = re.sub(r"[^\d만\+,\s]", "", text).replace(",", "").replace(" ", "")
        if "만+" in s:
            try: return int(s.replace("만+", "")) * 10000
            except Exception: return None
        if "만" in s:
            try: return int(s.replace("만", "")) * 10000
            except Exception: pass
        try: return int(re.sub(r"[^\d]", "", s))
        except Exception: return None

    # -------------------- 카테고리 패널 --------------------
    def category_panel_visible(self) -> bool:
        try:
            self.driver.find_element(By.CSS_SELECTOR, ".category_layer .list_ctgmenu")
            return True
        except NoSuchElementException:
            return False

    def click_header_category_button(self) -> bool:
        """헤더의 '카테고리' 버튼 클릭 시도"""
        self.driver.execute_script("window.scrollTo(0,0);")
        self.wait(0.2)
        selectors = [
            "button.btn_cate",                 # 버튼
            "a[aria-label='카테고리']",
            "button[aria-label='카테고리']",
            "a[href*='category']",
        ]
        for sel in selectors:
            try:
                btn = self.driver.find_element(By.CSS_SELECTOR, sel)
                self.js_click(btn)
                self.wait(0.8)
                if self.category_panel_visible():
                    return True
            except NoSuchElementException:
                continue
        return False

    def open_category_overlay(self) -> bool:
        """
        1) 카테고리 버튼 클릭으로 열기 시도
        2) 실패 시 현재 URL에 categoryLayer=OPEN 파라미터를 강제 부여해 진입
        """
        if self.category_panel_visible():
            return True

        # 1차: 버튼 클릭
        if self.click_header_category_button():
            return True

        # 2차: URL 파라미터로 강제 오픈
        cur = self.driver.current_url
        parts = urlsplit(cur)
        qs = dict(parse_qsl(parts.query))
        if qs.get("categoryLayer") != "OPEN":
            qs["categoryLayer"] = "OPEN"
            new_url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(qs), parts.fragment))
            self.driver.get(new_url)
            self.wait(1.0)
            if self.category_panel_visible():
                return True

        # 3차: 홈의 카테고리 오버레이 고정 엔드포인트 시도 (fallback)
        try:
            self.driver.get("https://gift.kakao.com/page/19976?categoryLayer=OPEN")
            self.wait(1.0)
            return self.category_panel_visible()
        except Exception:
            return False

    # -------------------- 카테고리/탭 조작 --------------------
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

    # --- 페이지 내 '상품/브랜드' 탭 제어(일반 수집용) ---
    def ensure_product_tab(self):
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

    # --- 탭 ---
    def list_tabs(self):
        try:
            tabs = self.driver.find_elements(
                By.CSS_SELECTOR,
                ".module_wrapper .module_tab .rail_cate a.link_tab span.txt_tab",
            )
            return [self.safe_text(t) for t in tabs if self.safe_text(t)]
        except NoSuchElementException:
            return []

    def click_tab(self, name):
        tabs = self.driver.find_elements(
            By.CSS_SELECTOR, ".module_wrapper .module_tab .rail_cate a.link_tab"
        )
        for a in tabs:
            try:
                txt = self.safe_text(a.find_element(By.CSS_SELECTOR, "span.txt_tab"))
            except NoSuchElementException:
                txt = self.safe_text(a)
            if txt == name:
                self.js_click(a)
                self.wait(0.9)
                return True
        return False

    # -------------------- 스크롤/카드 파싱 --------------------
    def scroll_until_cards(self, min_cards, list_selector="ul.list_prd > li"):
        for _ in range(6):
            if self.driver.find_elements(By.CSS_SELECTOR, list_selector):
                break
            self.wait(0.6)

        last_len, still = 0, 0
        while True:
            cards = self.driver.find_elements(By.CSS_SELECTOR, list_selector)
            if len(cards) >= min_cards:
                break
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            self.wait(self.SCROLL_PAUSE)
            cards2 = self.driver.find_elements(By.CSS_SELECTOR, list_selector)
            still = still + 1 if len(cards2) == last_len else 0
            last_len = len(cards2)
            if still >= self.MAX_SCROLL_TRIES:
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
        # 링크 (여러 구조 대응)
        link_selectors = [
            "a.link_info[href]",
            "div.thumb_prd a.link_thumb[href]",
            "a.link_prd[href]",
        ]
        for sel in link_selectors:
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

    def collect_current_page(self, want_total, meta, seen_urls, already_collected=0):
        self.scroll_until_cards(max(already_collected, want_total))
        rows, new_count = [], 0
        cards = self.driver.find_elements(By.CSS_SELECTOR, "ul.list_prd > li")
        for c in cards:
            item = self.parse_card(c)
            url = item.get("product_url")
            if not url or url in seen_urls:
                continue
            rows.append({**item, **meta})
            seen_urls.add(url)
            new_count += 1
            if already_collected + new_count >= want_total:
                break
        return rows, new_count

    # -------------------- 수집기 --------------------
    def crawl_general(self, top, sub, n):
        print(f"[{top} > {sub}] 일반 수집(상품 탭)")
        self.ensure_product_tab()
        rows, _ = self.collect_current_page(
            n,
            {"top_category": top, "sub_category": sub, "sub_tab": None},
            set(),
            0,
        )
        return rows

    def crawl_tabs_balanced(self, top, sub, total, exclude_tabs=None):
        tabs_all = self.list_tabs()
        excluding = set(exclude_tabs or set())
        tab_names = [t for t in tabs_all if t and t not in excluding]
        if not tab_names:
            return self.crawl_general(top, sub, total)

        print(f"[{top} > {sub}] 탭 균등 분배 수집 (총 {total}) / 사용탭: {tab_names}")
        k = len(tab_names)
        base = total // k
        rem = total % k
        plan = {t: base + (1 if i < rem else 0) for i, t in enumerate(tab_names)}

        collected = {t: 0 for t in tab_names}
        seen = set()
        all_rows = []

        # 1차 라운드
        for t in tab_names:
            if not self.click_tab(t):
                continue
            want = plan[t]
            rows, got = self.collect_current_page(
                want,
                {"top_category": top, "sub_category": sub, "sub_tab": t},
                seen,
                collected[t],
            )
            all_rows.extend(rows)
            collected[t] += got

        # 부족분 재분배
        deficit = sum(max(0, plan[t] - collected[t]) for t in tab_names)
        if deficit <= 0:
            return all_rows[:total]

        MAX_PER_HOP = 30
        safety = 0
        while deficit > 0 and safety < 8:
            progressed = 0
            for t in tab_names:
                if deficit <= 0:
                    break
                if not self.click_tab(t):
                    continue
                extra = min(MAX_PER_HOP, deficit)
                want = collected[t] + extra
                rows, got = self.collect_current_page(
                    want,
                    {"top_category": top, "sub_category": sub, "sub_tab": t},
                    seen,
                    collected[t],
                )
                if got > 0:
                    all_rows.extend(rows)
                    collected[t] += got
                    deficit -= got
                    progressed += got
            if progressed == 0:
                break
            safety += 1

        return all_rows[:total]

    def crawl_price_tabs_balanced(self, top, sub, total):
        return self.crawl_tabs_balanced(top, sub, total, exclude_tabs=self.EXCLUDE_TABS_GENERIC)

    def crawl_artist_tabs(self, top, sub, total):
        exclude = self.EXCLUDE_TABS_BY_SUBCAT.get(sub, {"전체"})
        return self.crawl_tabs_balanced(top, sub, total, exclude_tabs=exclude)

    # -------------------- 2) 크롤링 파이프라인 --------------------
    def scrape_data(self):
        self.driver.get(self.start_url)
        self.wait(1.2)

        # 항상 카테고리 패널을 띄운 상태에서 시작
        self.open_category_overlay()
        self.all_rows = []

        avail_tops = self.list_top_categories()
        # 화이트리스트 교집합 + 순서
        desired = [t for t in self.TOP_WHITELIST if t in avail_tops]
        if self.top_filter:
            # top_filter 지정 시, 화이트리스트 안에서만 필터링
            wanted = [t.strip() for t in self.top_filter.split(",")]
            target_tops = [t for t in desired if t in wanted]
        else:
            target_tops = desired

        print("대상 상위 카테고리:", target_tops)

        for top in target_tops:
            if not self.click_top_category(top):
                print(f"  ! 상위 카테고리 클릭 실패: {top}")
                self.open_category_overlay()
                continue

            subcats = self.list_sub_categories()
            if not subcats:
                print(f"  ! 하위 카테고리 없음: {top}")
                self.open_category_overlay()
                continue

            for sub in subcats:
                if sub in self.EXCLUDE_SUBCATS:
                    print(f"  - 제외: {top} > {sub}")
                    continue
                if not self.click_sub_category(sub):
                    print(f"    ! 하위 카테고리 클릭 실패: {sub}")
                    self.open_category_overlay()
                    continue

                try:
                    if top == "와인/양주/전통주":
                        rows = self.crawl_price_tabs_balanced(top, sub, self.items_per_subcat)
                    elif top == "아티스트/캐릭터":
                        rows = self.crawl_artist_tabs(top, sub, self.items_per_subcat)
                    else:
                        rows = self.crawl_general(top, sub, self.items_per_subcat)
                    self.all_rows.extend(rows)
                finally:
                    # 반드시 카테고리 패널로 복귀
                    self.open_category_overlay()

        return self.all_rows

    # -------------------- 3) 저장 --------------------
    def save_to_database(self, data):
        df

