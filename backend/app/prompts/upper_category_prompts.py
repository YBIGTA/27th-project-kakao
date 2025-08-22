"""
상위 카테고리 노드용 LLM 프롬프트 (upper_category_prompts.py)

모든 상위 카테고리에 대한 confidence를 계산하는 프롬프트들
"""

import json
import re
from typing import Dict, Any

class UpperCategoryPrompts:
    def __init__(self):
        self.system_prompt = """너는 "카카오톡 선물하기 상위 카테고리 매퍼"다.
확장 루브릭(A~G)과 문장 라우팅 규정을 적용해 각 카테고리의 신호(d,f,r,g,s,exclusivity)를 0~1로 산출하고,
A식으로 confidence를 계산한다. 내부 단계적 추론(CoT)은 수행하되, 출력은 JSON만 제공한다."""

    def create_user_prompt(
        self, 
        conversation_text: str, 
        top_category_list: list,
        weights: dict = None,
        tau_days: int = 30,
        exclusivity_mode: str = "mul",
        return_min_conf: float = 0.00,
        max_evidence: int = 2
    ) -> str:
        """사용자 프롬프트를 생성합니다."""
        if weights is None:
            weights = {"d": 0.30, "f": 0.15, "r": 0.15, "c": 0.20, "s": 0.20, "lambda": 0.15}
        
        return f"""
대화: {conversation_text}
카테고리: {top_category_list}
설정:
- unit=sentence
- mode=free
- weights={weights}
- recency_tau={tau_days}
- exclusivity_mode={exclusivity_mode}
- min_conf={return_min_conf}
- max_evidence={max_evidence}
요구:
1) 연관 문장만 집계(is_relevant 규정 준수)
2) d,f,r,g,s와 배타성(level/e) 산정 → A식 적용 → 최종 confidence 계산
3) 모든 카테고리를 confidence 내림차순으로 rank 지정하여 JSON 스키마로 반환

[확장 루브릭 A~G (서술 규정)]
A. 직접성(Directness)
  - 상위 개념/대표 활동의 직접 언급(화장품/옷/과자/캠핑/집 꾸미기/가전 등)은 강한 근거.
  - 특정 하위(립밤/초콜릿/운동화 등)만 언급 시 상위 일반화는 보수적으로.
  - 추상/다의적 표현("피부 관리 필요", "새 장비 사고 싶다")은 중간 이하.
B. 빈도(Frequency)
  - 최근 3개월 내 반복 언급 가산, 단발·희박 감산.
C. 최근성(Recency)
  - 최신 발화가 지지하면 가산, 오래된 언급만 있으면 감산.
D. 상황합치(Context Fit; 대상자 맥락)
  - 생활 습관/환경/제약/예산과의 적합(다이어트→건강식, 기숙사→소형/간편, 피부 건조→보습, 무알콜 등).
  - 어긋남(금기/알레르기/중단 선언 등)은 감산.
E. 감정(Sentiment)
  - 긍정(좋아/탐난다/필요/쓰고 싶다) 강할수록 가산.
  - 부정(싫다/필요없다/끊었다) 강할수록 감산. 카테고리 전반 거부면 0에 수렴.
G. 대화-수준 배타성(Exclusivity)
  - 의미: 단서가 특정 상위에 독점적으로 모였는가(높음) vs 여러 상위로 겹치는가(낮음).
  - 다중 후보 예: "향 좋아"(뷰티/리빙), "집 꾸미기"(리빙/디지털/가전), "장비"(가전/레저).
  - 등급 {높음/중간/낮음} → 계수 {1.00/0.90/0.75} (mul) 또는 e∈[0,1] 감산(sub).

[문장 단위 라우팅 규정 — "연관 문장만 집계"]
- 문장을 카테고리에 배정하는 is_relevant(sentence, category) 판단 규칙:
  1) 직접 키워드: 카테고리명/대표 하위/동의어/행동(예: "집 꾸미기"→리빙, "캠핑 가고 싶다"→레저/캠핑)
  2) 속성/효용 언급: 기능·속성이 명백히 해당 카테고리를 지시("보습/향/메이크업"→뷰티)
  3) 구매행동/의향: "사다/장바구니/추천해줘/고민 중"
  4) 부정/금기/문제: "알콜 못 마셔/유당불내증/허리 아픔/건조함"
  5) 모호성 처리: 다의어는 문맥(이전/다음 발화, 대상 소유/장소/행위)로 해소. 해소 불가 시 약한 관련(0.3)로 표기.
  6) 부정 라우팅: "XX 싫다/안 산다/그만뒀다"도 해당 카테고리로 라우팅하되 s<0로 반영.

[수치화 가이드(0~1)]
- directness d:
  · 상위 직접 언급+의향/요구: 0.90~1.00
  · 하위만 언급(상위 일반화 가능): 0.55~0.75
  · 추상/다의적·약한 신호: 0.30~0.50
  · 무관/부정 명시: 0.00~0.20
- frequency f:  f = min(1, sqrt(count_relevant(cat)/count_p95))
- recency r:    r = exp(-Δdays/τ), τ={tau_days}  (문장별 r의 최대값 사용 권장)
- context_fit g:
  · 강한 적합: 0.75~1.00 / 보통: 0.40~0.74 / 어긋남: 0.00~0.39
- sentiment s:  감성[-1,1] → (s+1)/2  (해당 카테고리 연관 문장만)
- exclusivity:
  · level ∈ {high, mid, low} → coef ∈ {1.00, 0.90, 0.75} (mul)
  · 또는 e∈[0,1] 산정 후 λ*e 감산(sub)

[산출식 A - 자유 가중합(연속)]
z_i = w_d*d + w_f*f + w_r*r + w_c*g + w_s*s   # (sub 모드면 - λ*e 추가)
conf_raw = σ(z_i)
배타성 보정:
  if {exclusivity_mode}=="mul": conf_i = conf_raw * coef(level_i)
  else:                        conf_i = max(0, conf_raw - λ*e)

[출력 스키마(JSON) — 모든 카테고리 반환]
반드시 아래 스키마로만 출력한다. categories 배열에는 {top_category_list}의 모든 항목을 포함한다.
{{
  "unit": "sentence",
  "mode": "free",
  "exclusivity_mode": "{exclusivity_mode}",
  "min_conf": {return_min_conf},
  "categories": [
    {{
      "name": "<상위 카테고리>",
      "rank": 0,                      // confidence 내림차순 순위(0부터, 연속 번호)
      "scores": {{
        "directness": 0.00,
        "frequency": 0.00,
        "recency": 0.00,
        "context_fit": 0.00,
        "sentiment": 0.00
      }},
      "exclusivity": {{"level": "high|mid|low", "coef": 1.00}},
      "confidence_raw": 0.00,         // 보정 전
      "confidence": 0.00,             // 최종
      "evidence": [
        {{"text": "<원문 일부>", "date": "YYYY-MM-DD"}}
      ],
      "reason": "<짧은 요약 근거>"
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
                if "name" in category and "confidence" in category:
                    confidence_data[category["name"]] = {
                        "confidence": float(category["confidence"]),
                        "rank": category.get("rank", 0),
                        "reason": category.get("reason", ""),
                        "evidence": category.get("evidence", [])
                    }
            
            # 추론 과정 생성
            reasoning = "상위 카테고리 분석 결과:\n"
            sorted_categories = sorted(
                confidence_data.items(), 
                key=lambda x: x[1]["confidence"], 
                reverse=True
            )[:3]
            
            for i, (cat, data) in enumerate(sorted_categories, 1):
                reasoning += f"{i}. {cat}: {data['confidence']:.4f}\n"
                if data.get("reason"):
                    reasoning += f"   근거: {data['reason']}\n"
            
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
