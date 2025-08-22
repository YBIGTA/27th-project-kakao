"""통합 프롬프트 관리
- 모든 노드에서 사용하는 프롬프트를 한 곳에서 관리
- 프롬프트 버전 관리 및 템플릿화
"""
from typing import List, Dict, Any
import json

# ===== 기본 프롬프트 =====

PARENT_PROMPT = """\
<SYSTEM>
너는 "카카오톡 선물하기 상위 카테고리 판단기"다.
수학적 계산이나 정규화는 하지 말고, 각 카테고리에 대해
(1) 연관성 점수(0~100), (2) 구매의도 점수(0~100),
(3) 감정 극성(pos/neg/neutral), (4) 배타성 힌트(high/mid/low),
(5) 근거 문장 인덱스(최대 2개)만 JSON으로 반환하라.
카테고리/문장 외의 정보나 설명 문장은 절대 출력하지 마라.
</SYSTEM>
<USER>
입력:
- sentences: [{{idx:int, date:YYYY-MM-DD, text:str}}, ...]
- top_categories: [str, ...]   # 제공 목록 이외의 카테고리는 절대 생성 금지

출력 스키마:
{{
  "unit":"sentence",
  "categories":[
    {{
      "name": "<top_category>",
      "relevance_raw": 0..100,
      "intent_raw": 0..100,
      "polarity": "pos|neg|neutral",
      "exclusivity_hint": "high|mid|low",
      "form_signals": {{
        "giftcard_voucher": 0|1,
        "physical_goods": 0|1,
        "digital_content": 0|1
      }},
      "evidence_idx": [int, ...]  # 최대 2개, 없으면 []
    }}, ...
  ]
}}
데이터:
- sentences = {{sentences}}
- top_categories = {{parent_list}}
"""

# ===== 배치 처리용 프롬프트 =====

PARENT_BATCH_PROMPT = """\
<SYSTEM>
너는 "카카오톡 선물하기 상위 카테고리 배치 판단기"다.
여러 문장을 한 번에 분석하여 각 문장마다 상위 카테고리 점수를 매겨주세요.
수학적 계산이나 정규화는 하지 말고, 각 문장과 카테고리 조합에 대해
(1) 연관성 점수(0~100), (2) 구매의도 점수(0~100),
(3) 감정 극성(pos/neg/neutral), (4) 배타성 힌트(high/mid/low),
(5) 근거 문장 인덱스(최대 2개)만 JSON으로 반환하라.
</SYSTEM>
<USER>
입력:
- sentences: [{{idx:int, date:YYYY-MM-DD, text:str}}, ...]
- top_categories: [str, ...]   # 제공 목록 이외의 카테고리는 절대 생성 금지

출력 스키마:
{{
  "unit":"batch",
  "results": [
    {{
      "sentence_idx": int,
      "categories":[
        {{
          "name": "<top_category>",
          "relevance_raw": 0..100,
          "intent_raw": 0..100,
          "polarity": "pos|neg|neutral",
          "exclusivity_hint": "high|mid|low",
          "form_signals": {{
            "giftcard_voucher": 0|1,
            "physical_goods": 0|1,
            "digital_content": 0|1
          }},
          "evidence_idx": [int, ...]  # 최대 2개, 없으면 []
        }}, ...
      ]
    }}, ...
  ]
}}
데이터:
- sentences = {{sentences}}
- top_categories = {{parent_list}}
"""

CHILD_BATCH_PROMPT = """\
<SYSTEM>
너는 "카카오톡 선물하기 하위 카테고리 배치 판단기"다.
여러 문장을 한 번에 분석하여 각 문장마다 하위 카테고리 점수를 매겨주세요.
수학적 계산/정규화는 하지 말고, 각 문장과 하위 카테고리 조합에 대해
(1) 연관성 0~100, (2) 구매의도 0~100, (3) 구체성(brand/model/attributes),
(4) ownership 힌트(룰키), (5) 명시적 거부 여부, (6) 근거 문장 인덱스(≤2)만
JSON으로 반환하라. 제공 리스트 외의 카테고리는 생성 금지.
</SYSTEM>
<USER>
입력:
- sentences: [{{idx:int, date:str, text:str}}, ...]
- sub_categories: ["상위/하위", ...]  # 풀패스

출력 스키마:
{{
  "unit":"batch",
  "results": [
    {{
      "sentence_idx": int,
      "subcategories":[
        {{
          "path":"<상위/하위>",
          "relevance_raw": 0..100,
          "intent_raw": 0..100,
          "specificity": {{"brand":true|false,"model":true|false,"attributes":[str,...]}},
          "ownership_hint":"consumable_refill|refill_needed|stock_plenty|ecosystem_accessory_if_base_owned|collection_hobby|single_install_durable|durable_upgrade_fault_dissatisfied|recently_bought_satisfied_no_duplicate|none",
          "explicit_rejection": true|false,
          "evidence_idx":[int,...]
        }}, ...
      ]
    }}, ...
  ]
}}
데이터:
- sentences = {{sentences}}
- sub_categories = {{child_list}}
"""

CHILD_PROMPT = """\
<SYSTEM>
너는 "카카오톡 선물하기 하위 카테고리 판단기"다.
수학적 계산/정규화는 하지 말고, 각 하위 카테고리에 대해
(1) 연관성 0~100, (2) 구매의도 0~100, (3) 구체성(brand/model/attributes),
(4) ownership 힌트(룰키), (5) 명시적 거부 여부, (6) 근거 문장 인덱스(≤2)만
JSON으로 반환하라. 제공 리스트 외의 카테고리는 생성 금지.
</SYSTEM>
<USER>
입력:
- sentences: [{{idx:int, date:str, text:str}}, ...]
- sub_categories: ["상위/하위", ...]  # 풀패스

출력 스키마:
{{
  "unit":"sentence",
  "subcategories":[
    {{
      "path":"<상위/하위>",
      "relevance_raw": 0..100,
      "intent_raw": 0..100,
      "specificity": {{"brand":true|false,"model":true|false,"attributes":[str,...]}},
      "ownership_hint":"consumable_refill|refill_needed|stock_plenty|ecosystem_accessory_if_base_owned|collection_hobby|single_install_durable|durable_upgrade_fault_dissatisfied|recently_bought_satisfied_no_duplicate|none",
      "explicit_rejection": true|false,
      "evidence_idx":[int,...]
    }}, ...
  ]
}}
데이터:
- sentences = {{sentences}}
- sub_categories = {{child_list}}
"""

# ===== 최종 상품 선택 프롬프트 =====

FINAL_SELECTION_PROMPT = """\
System:
너는 카카오톡 대화를 기반으로 가장 적합한 선물 5개를 선택하는 전문가다.
카테고리 신호, 근거 문장, evidence를 종합적으로 고려하여 선택하라.

User:
[대상자 정보]
- 나이: {age}세
- 성별: {gender}
- 관계: {relation}
- 예산: {budget_min:,}원 ~ {budget_max:,}원

[상위 카테고리 신호 및 근거]
{parent_scores_info}

[상위 카테고리 Evidence]
{parent_evidence_info}

[하위 카테고리 신호 (상위 3개)]
{child_scores_info}

[하위 카테고리 Evidence]
{child_evidence_info}

[후보 상품들]
{candidate_products_info}

위 정보를 종합하여 가장 적합한 선물 5개를 선택하라.
선택 기준:
1. 카테고리 신호 강도 (높은 점수 우선)
2. Evidence 문장과의 일치도
3. 사용자 프로필 적합성 (나이, 성별, 관계)
4. 예산 범위 준수
5. 상품 품질 지표 (만족도, 리뷰수)

출력 (JSON):
{{
  "selected": ["상품ID1", "상품ID2", "상품ID3", "상품ID4", "상품ID5"],
  "rationale": {{
    "상품ID1": "선택 이유 (카테고리 신호와 근거를 포함한 구체적 설명)",
    "상품ID2": "선택 이유 (카테고리 신호와 근거를 포함한 구체적 설명)",
    "상품ID3": "선택 이유 (카테고리 신호와 근거를 포함한 구체적 설명)",
    "상품ID4": "선택 이유 (카테고리 신호와 근거를 포함한 구체적 설명)",
    "상품ID5": "선택 이유 (카테고리 신호와 근거를 포함한 구체적 설명)"
  }}
}}"""

# ===== 프롬프트 헬퍼 함수 =====

def format_parent_prompt(sentences: List[Dict], parent_labels: List[str]) -> str:
    """상위 카테고리 프롬프트 포맷팅"""
    sentences_json = json.dumps(sentences, ensure_ascii=False)
    parent_list = json.dumps(parent_labels, ensure_ascii=False)
    
    return PARENT_PROMPT.format(
        sentences=sentences_json,
        parent_list=parent_list
    )

def format_parent_batch_prompt(sentences: List[Dict], parent_labels: List[str]) -> str:
    """상위 카테고리 배치 프롬프트 포맷팅"""
    sentences_json = json.dumps(sentences, ensure_ascii=False)
    parent_list = json.dumps(parent_labels, ensure_ascii=False)
    
    return PARENT_BATCH_PROMPT.format(
        sentences=sentences_json,
        parent_list=parent_list
    )

def format_parent_batch_template(parent_labels: List[str]) -> str:
    """상위 카테고리 배치 프롬프트 템플릿 생성 (문장 없이)"""
    # placeholder로 템플릿 생성
    placeholder_sentences = [{"idx": 0, "date": "2024-01-01", "text": "PLACEHOLDER_TEXT"}]
    sentences_json = json.dumps(placeholder_sentences, ensure_ascii=False)
    parent_list = json.dumps(parent_labels, ensure_ascii=False)
    
    template = PARENT_BATCH_PROMPT.format(
        sentences=sentences_json,
        parent_list=parent_list
    )
    
    # placeholder를 실제 교체 가능한 형태로 변환
    return template.replace(json.dumps(placeholder_sentences, ensure_ascii=False), "{{sentences}}")

def format_child_prompt(sentences: List[Dict], child_labels: List[str]) -> str:
    """하위 카테고리 프롬프트 포맷팅"""
    sentences_json = json.dumps(sentences, ensure_ascii=False)
    child_list = json.dumps(child_labels, ensure_ascii=False)
    
    return CHILD_PROMPT.format(
        sentences=sentences_json,
        child_list=child_list
    )

def format_child_batch_prompt(sentences: List[Dict], child_labels: List[str]) -> str:
    """하위 카테고리 배치 프롬프트 포맷팅"""
    sentences_json = json.dumps(sentences, ensure_ascii=False)
    child_list = json.dumps(child_labels, ensure_ascii=False)
    
    return CHILD_BATCH_PROMPT.format(
        sentences=sentences_json,
        child_list=child_list
    )

def format_child_batch_template(child_labels: List[str]) -> str:
    """하위 카테고리 배치 프롬프트 템플릿 생성 (문장 없이)"""
    # placeholder로 템플릿 생성
    placeholder_sentences = [{"idx": 0, "date": "2024-01-01", "text": "PLACEHOLDER_TEXT"}]
    sentences_json = json.dumps(placeholder_sentences, ensure_ascii=False)
    child_list = json.dumps(child_labels, ensure_ascii=False)
    
    template = CHILD_BATCH_PROMPT.format(
        sentences=sentences_json,
        child_list=child_list
    )
    
    # placeholder를 실제 교체 가능한 형태로 변환
    return template.replace(json.dumps(placeholder_sentences, ensure_ascii=False), "{{sentences}}")

def format_final_selection_prompt(state, candidate_products):
    """최종 선택 프롬프트 포맷팅"""

    # 상위 카테고리 정보
    parent_scores_info = "\n".join(
        f"- {cat}: {state.parent_scores.get(cat, 0):.3f} (이유: {', '.join(state.parent_reasoning.get(cat, ['없음'])[:2])})"
        for cat in state.parent_labels if state.parent_scores.get(cat, 0) > 0.01
    )

    # 상위 카테고리 Evidence
    parent_evidence_info = "\n".join(
        f"- {cat}: {', '.join(ev[:2])}"
        for cat, ev in _get_parent_evidence_texts(state).items() if ev
    )

    # 하위 카테고리 정보
    child_scores_info = "\n".join(
        f"- {cat}: {state.final_child_scores.get(cat, 0):.3f} (이유: {', '.join(state.child_reasoning.get(cat, ['없음'])[:2])})"
        for cat in state.top3_children
    )

    # 하위 카테고리 Evidence
    child_evidence_info = "\n".join(
        f"- {cat}: {', '.join(ev[:2])}"
        for cat, ev in _get_child_evidence_texts(state).items() if ev
    )

    # 후보 상품 정보
    candidate_products_info = "\n".join(
        f"- {p['id']}: {p['title']} ({p['brand']}, {p['price']:,}원, {p['category_child']})"
        for p in candidate_products[:20]
    )

    return FINAL_SELECTION_PROMPT.format(
        age=state.ctx.age,
        gender=state.ctx.gender,
        relation=state.ctx.relation,
        budget_min=state.ctx.budget_min,
        budget_max=state.ctx.budget_max,
        parent_scores_info=parent_scores_info,
        parent_evidence_info=parent_evidence_info,
        child_scores_info=child_scores_info,
        child_evidence_info=child_evidence_info,
        candidate_products_info=candidate_products_info
    )
def _get_parent_evidence_texts(state):
    """상위 카테고리 evidence 텍스트 추출"""
    evidence_texts = {}
    for p in state.parent_labels:
        evidence_texts[p] = []
        for idx in state.parent_evidence_idx.get(p, [])[:3]:
            if 0 <= idx < len(state.rows):
                evidence_texts[p].append(state.rows[idx].text)
    return evidence_texts

def _get_child_evidence_texts(state):
    """하위 카테고리 evidence 텍스트 추출"""
    evidence_texts = {}
    for s in state.top3_children:
        evidence_texts[s] = []
        for idx in state.child_evidence_idx.get(s, [])[:3]:
            if 0 <= idx < len(state.rows):
                evidence_texts[s].append(state.rows[idx].text)
    return evidence_texts

