"""
하위 카테고리 노드용 LLM 프롬프트 (lower_category_prompts.py)

각 상위 카테고리별 하위 카테고리 confidence를 계산하는 프롬프트들
"""

import json
import re
from typing import Dict, Any

class LowerCategoryPrompts:
    def __init__(self):
        self.system_prompt = """너는 "카카오톡 선물하기 하위 카테고리 매퍼"다.
확장 루브릭(A~H), 문장 라우팅, ownership/배타성 보정을 적용하여 A식으로 confidence를 계산한다.
명시적 거부(NEG)는 즉시 0.00으로 결정한다. 내부 CoT는 수행하되 출력은 JSON만 제공한다."""

    def create_user_prompt(
        self, 
        conversation_text: str, 
        user_profile: dict,
        top_category_list: list,
        sub_category_list: list,
        upper_probabilities: dict = None,
        weights: dict = None,
        tau_days: int = 30,
        exclusivity_mode: str = "mul",
        exclusivity_scope: str = "sibling",
        ownership_rules: dict = None,
        use_parent_prior: bool = False,
        return_min_conf: float = 0.00,
        max_evidence: int = 2
    ) -> str:
        """사용자 프롬프트를 생성합니다."""
        if weights is None:
            weights = {"d": 0.30, "f": 0.15, "r": 0.15, "c": 0.20, "s": 0.20, "lambda": 0.15}
        
        if ownership_rules is None:
            ownership_rules = {
                "consumable_refill": 1.15,
                "refill_needed": 1.25,
                "stock_plenty": 0.85,
                "ecosystem_accessory_if_base_owned": 1.15,
                "collection_hobby": 1.20,
                "single_install_durable": 0.60,
                "durable_upgrade_fault_dissatisfied": 1.20,
                "recently_bought_satisfied_no_duplicate": 0.50
            }
        
        parent_priors = {}
        if use_parent_prior and upper_probabilities:
            for sub_cat in sub_category_list:
                if "/" in sub_cat:
                    parent = sub_cat.split("/")[0]
                    parent_priors[sub_cat] = upper_probabilities.get(parent, 1.0)
        
        return f"""
대화: {conversation_text}
상위: {top_category_list}
하위: {sub_category_list}
설정:
- unit=sentence
- mode=free
- weights={weights}
- recency_tau={tau_days}
- exclusivity_mode={exclusivity_mode}
- exclusivity_scope={exclusivity_scope}
- ownership_rules={ownership_rules}
- use_parent_prior={use_parent_prior}
- parent_priors={parent_priors}
- min_conf={return_min_conf}
- max_evidence={max_evidence}
요구:
1) 연관 문장만 집계 → d,f,r,g,s,특정성,ownership,배타성 산정(약한 관련은 d를 낮게 반영)
2) NEG 즉시 처리 → A식 → 배타성 → ownership → (옵션)parent_prior → min(1.0, …)
3) 모든 하위 카테고리를 rank와 함께 JSON 스키마로 반환

[하위 카테고리 확장 루브릭 — A~H]
A. 직접성(Directness)
  - 상위/대표 활동 직접 언급은 강함, 하위만 언급 시 상위 일반화 보수적.
  - 추상/다의적 표현은 중간 이하.
  - (하위 전용 보정) **구체성·구매신호·문제상황**을 직접성에 가점으로 반영:
    · 구체성(specificity): 브랜드/모델/속성(무알콜·무향·사이즈 등) 명시 시 +0.05
    · 구매신호(intent): "사야겠다/사볼까/검색했다/장바구니" 등 +0.15
    · 필요/문제상황(need/problem): "뻐근/건조/배터리 빨리 닳음/헤졌음" 등 +0.05
    (합산 상한: d ≤ 1.00)
B. 빈도(Frequency)
  - 최근 3개월 내 반복 언급일수록 가산(문장 단위 라우팅에서 연관 문장만 집계).
C. 최근성(Recency)
  - 최신 발화가 지지 시 가산, 오래된 언급만 존재 시 감산(지수 감쇠).
D. 상황합치(Context Fit)
  - 대상자의 생활 습관/환경/제약/예산/취향과 하위 카테고리의 적합도 평가.
E. 감정(Sentiment)
  - 긍정 강할수록 가산, 부정 강할수록 감산. 하위 전반 거부는 0에 수렴.
F. **구체성(Specificity; 하위 전용 강조)**
  - 브랜드/모델/속성(무알콜/무향/사이즈/재질/호환규격 등)의 명시가 있을수록 상향.
G. 대화-수준 배타성(Exclusivity)
  - 동일 상위 내 **형제 하위 간** 충돌/동시강세를 우선 고려(EXCLUSIVITY_SCOPE="sibling").
  - level∈{{high,mid,low}} → 계수 {{1.00,0.90,0.75}}(mul) 또는 e∈[0,1] 감산(sub).
H. **보유/재고/에코시스템 보정(Ownership) — multiplicative**
  - ownership_coef 기본 1.00, 다음 규칙 매칭 시 곱셈 적용(중복 매칭 시 가장 **강한 단일 규칙만** 적용 권장):
    1) 소모성/리필형(스낵·커피캡슐·화장솜/클렌저 등)           → ×1.15
    2) "자주 먹음/거의 떨어짐/리필 필요"                           → ×1.25
    3) "재고 충분/많이 보유/여유 있음"                              → ×0.85
    4) 에코시스템 액세서리(폰케이스/팁/케이블, 머신용 캡슐 등)
       - 본체 보유가 문맥상 확인될 때                            → ×1.15
    5) 컬렉션/취미형(향수·피규어·문구·도서·카드·키링 수집 등)     → ×1.20 (예: "모으는 중/시리즈 완성하고 싶다")
    6) 단일 설치 내구재(대형가전·가구·본체 디바이스)              → ×0.60
       - 단, 업그레이드/고장/불만 언급 시                          → ×1.20 (0.60 대신)
    7) 방금 구매·만족/중복 원치 않음                                → ×0.50
    8) **명시적 거부("이제 안 씀/싫다/그만둠")**                    → 하드 NEG (conf=0.00)

[문장 라우팅(연관 문장만 집계)]
- is_relevant(sentence, subcategory) 기준:
  1) 하위명/대표 키워드/동의어/브랜드·모델·속성 일치
  2) 기능·효용·호환규격 언급("보습/무향/USB-C/호환 팁" 등)
  3) 구매행동/의향, 필요/문제, 재고·보유·리필·업그레이드·고장
  4) 부정/금기/중단 선언도 라우팅하되 s<0로 반영
  5) 다의어는 인접 문맥으로 해소. 불가 시 **약한 관련(0.3)**으로 간주하며, 이 경우 directness는 낮은 구간(예: 0.30~0.40)으로 처리
- False인 문장은 집계 제외

[수치화 가이드(0~1)]
- directness d:
  · 하위 직접 언급+의향/요구: 0.85~1.00
  · 하위 직접 언급만: 0.60~0.80
  · 추상/다의적/약한 신호: 0.30~0.55
  · 무관/부정: 0.00~0.25
  (보정) 구체성 +0.05, intent +0.15, need/problem +0.05 (상한 1.00)
- frequency f:  f = min(1, sqrt(count_relevant(subcat)/count_p95))
- recency r:    r = exp(-Δdays/τ), τ={tau_days} (문장별 r 최대값 추천)
- context_fit g: 강 0.75~1.00 / 보통 0.40~0.74 / 약·불일치 0.00~0.39
- sentiment s:  [-1,1] → (s+1)/2 (연관 문장만)
- ownership_coef o: RULES_JSON에 따라 ×계수
- exclusivity:  level∈{{high,mid,low}}→coef∈{{1.00,0.90,0.75}} (mul) 또는 e∈[0,1] (sub)

[산출식 A - 자유 가중합(연속)]
# NEG 우선 처리
if NEG_HARD_ZERO and explicit_rejection: conf_i = 0.00  # 이후 보정 스킵
else:
  z_i = w_d*d + w_f*f + w_r*r + w_c*g + w_s*s        # (sub 모드면 -λ*e 추가)
  conf_raw = σ(z_i)
  # 배타성 보정
  if {exclusivity_mode}=="mul": conf_tmp = conf_raw * coef(level_i)
  else:                        conf_tmp = max(0, conf_raw - λ*e)
  # 소유 보정
  conf_tmp2 = conf_tmp * ownership_coef
  # 상위 prior(선택)
  if {use_parent_prior}: conf_tmp2 = conf_tmp2 * parent_prior
  conf_i = min(1.0, conf_tmp2)

[출력 스키마(JSON) — 모든 하위 카테고리 반환]
{{
  "unit": "sentence",
  "mode": "free",
  "exclusivity_mode": "{exclusivity_mode}",
  "exclusivity_scope": "{exclusivity_scope}",
  "min_conf": {return_min_conf},
  "use_parent_prior": {use_parent_prior},
  "categories": [
    {{
      "path": "<상위/하위>",
      "parent": "<상위>",
      "name": "<하위>",
      "rank": 0,
      "scores": {{
        "directness": 0.00,
        "frequency": 0.00,
        "recency": 0.00,
        "context_fit": 0.00,
        "sentiment": 0.00
      }},
      "specificity": {{
        "brand": false,
        "model": false,
        "attributes": []
      }},
      "ownership": {{
        "applied_rule": "<rule_key or none>",
        "coef": 1.00,
        "evidence": [{{"text":"<원문 일부>","date":"YYYY-MM-DD"}}]
      }},
      "exclusivity": {{"scope":"sibling|global","level":"high|mid|low","coef":1.00}},
      "parent_prior": 1.00,
      "explicit_rejection": false,
      "confidence_raw": 0.00,
      "confidence": 0.00,
      "evidence": [
        {{"text": "<대표 단서(최신|강한|구체적)>", "date": "YYYY-MM-DD"}}
      ],
      "reason": "<한두 문장 요약 근거>"
    }}
  ]
}}
"""

    def parse_response(self, response_text: str) -> tuple[dict, str]:
        """LLM 응답을 파싱합니다."""
        try:
            # JSON 추출
            json_data = self._extract_json(response_text)
            
            # 필수 필드 검증
            if "categories" not in json_data:
                raise ValueError("categories 필드가 없습니다")
            
            # 카테고리별 confidence 추출
            confidence_data = {}
            for category in json_data["categories"]:
                if "path" in category and "confidence" in category:
                    confidence_data[category["path"]] = {
                        "confidence": float(category["confidence"]),
                        "rank": category.get("rank", 0),
                        "reason": category.get("reason", ""),
                        "evidence": category.get("evidence", []),
                        "parent": category.get("parent", ""),
                        "name": category.get("name", "")
                    }
            
            # 추론 과정 생성
            reasoning = "하위 카테고리 분석 결과:\n"
            
            # 상위 카테고리별로 정렬
            parent_groups = {}
            for path, data in confidence_data.items():
                parent = data.get("parent", "")
                if parent not in parent_groups:
                    parent_groups[parent] = []
                parent_groups[parent].append((path, data))
            
            for parent in sorted(parent_groups.keys()):
                parent_data = parent_groups[parent]
                # confidence 순으로 정렬
                parent_data.sort(key=lambda x: x[1]["confidence"], reverse=True)
                
                reasoning += f"\n{parent}:\n"
                for i, (path, data) in enumerate(parent_data[:3], 1):  # 상위 3개만
                    child_name = data.get("name", path.split("/")[-1])
                    reasoning += f"  {i}. {child_name}: {data['confidence']:.4f}\n"
                    if data.get("reason"):
                        reasoning += f"     근거: {data['reason']}\n"
            
            return confidence_data, reasoning
            
        except Exception as e:
            print(f"LLM 응답 파싱 실패: {e}")
            return {}, f"파싱 오류: {str(e)}"
    
    def _extract_json(self, content: str) -> Dict[str, Any]:
        """JSON을 추출합니다."""
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            # JSON 블록 찾기
            json_match = re.search(r'```json\s*(.*?)\s*```', content, re.DOTALL)
            if json_match:
                return json.loads(json_match.group(1))
            
            # 중괄호로 둘러싸인 JSON 찾기
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                return json.loads(json_match.group(0))
            
            raise ValueError("JSON을 찾을 수 없습니다.")
