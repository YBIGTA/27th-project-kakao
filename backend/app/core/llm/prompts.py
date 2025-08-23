"""
통합 프롬프트 관리
- 모든 노드에서 사용하는 프롬프트를 한 곳에서 관리
- 프롬프트 버전 관리 및 템플릿화
"""
from typing import List, Dict, Any
import json

# ===== 기본 프롬프트 =====
PARENT_PROMPT = """\
<SYSTEM>
너는 "카카오톡 선물하기 상위 카테고리 판단기"다.
각 문장을 개별적으로 분석하여, 해당 문장에서 각 카테고리의 중요도를 평가하라.

핵심 원칙:
1. 해당 문장에서 가장 관련성이 높은 카테고리: 1.0
2. 관련성이 없는 카테고리: 0.0
3. 나머지는 0.0~1.0 사이에서 상대적으로 점수 부여
4. 동일한 의미를 가진 표현에는 반드시 같은 점수 부여
   - 의미의 동일성은 카테고리와 의도를 종합하여 판단
   - 문장 구조나 표현 방식이 달라도 의미가 같으면 동일 점수
5. 절대적 점수보다 상대적 순위가 중요

점수 기준:
(1) 연관성 점수(0.0~1.0)
   - 1.0: 가장 관련성 높음 (직접 언급 + 구체적 내용)
   - 0.8-0.9: 높은 관련성 (직접 언급)
   - 0.5-0.7: 보통 관련성 (간접적 언급)
   - 0.2-0.4: 약한 관련성 (약간 언급)
   - 0.0: 무관

(2) 구매의도 점수(0.0~1.0)
   - 1.0: 가장 강한 구매 의향
   - 0.8-0.9: 강한 구매 의향
   - 0.5-0.7: 보통 구매 의향
   - 0.2-0.4: 약한 구매 의향
   - 0.0: 구매 의향 없음

**일관성 체크**: 동일한 의미를 가진 표현에는 반드시 같은 점수 부여
</SYSTEM>
<USER>
입력:
- sentences: [{{idx:int, date:YYYY-MM-DD, text:str}}, ...]
- top_categories: [str, ...]

출력 스키마:
{{
  "unit":"sentence",
  "categories":[
    {{
      "name": "<top_category>",
      "relevance_raw": 0.0..1.0,  # 상대적 중요도 기준
      "intent_raw": 0.0..1.0,     # 상대적 중요도 기준
      "evidence_idx": [int, ...]
    }}, ...
  ]
}}
데이터:
- sentences = {{sentences}}
- top_categories = {{parent_list}}
</USER>
"""

CHILD_PROMPT = """\
<SYSTEM>
너는 "카카오톡 선물하기 하위 카테고리 판단기"다.
각 하위 카테고리에 대해 연관성과 구매의도를 0.0~1.0으로 평가하라.

핵심 원칙:
1. 각 하위 카테고리를 개별적으로 평가
2. 해당 문장에서 가장 관련성이 높은 하위 카테고리: 1.0
3. 관련성이 없는 하위 카테고리: 0.0
4. 나머지는 0.0~1.0 사이에서 상대적으로 점수 부여
5. 동일한 의미를 가진 표현에는 반드시 같은 점수 부여
   - 의미의 동일성은 카테고리와 의도를 종합하여 판단
   - 문장 구조나 표현 방식이 달라도 의미가 같으면 동일 점수
6. 절대적 점수보다 상대적 순위가 중요

점수 기준:
(1) 연관성 점수(0.0~1.0)
   - 1.0: 가장 관련성 높음 (직접 언급 + 구체적 내용)
   - 0.8-0.9: 높은 관련성 (직접 언급)
   - 0.5-0.7: 보통 관련성 (간접적 언급)
   - 0.2-0.4: 약한 관련성 (약간 언급)
   - 0.0: 무관

(2) 구매의도 점수(0.0~1.0)
   - 1.0: 가장 강한 구매 의향
   - 0.8-0.9: 강한 구매 의향
   - 0.5-0.7: 보통 구매 의향
   - 0.2-0.4: 약한 구매 의향
   - 0.0: 구매 의향 없음

**일관성 체크**: 동일한 의미를 가진 표현에는 반드시 같은 점수 부여
</SYSTEM>
"""

# ===== 배치 처리용 프롬프트 =====
PARENT_BATCH_PROMPT = """\
<SYSTEM>
너는 "카카오톡 선물하기 상위 카테고리 배치 판단기"다.
여러 문장을 한 번에 분석하여 각 문장마다 **상대적 중요도**를 기준으로 일관되게 점수를 부여하라.

핵심 원칙:
1. 각 문장을 개별적으로 분석하여, 해당 문장에서 각 카테고리의 중요도를 평가하라
2. 관련성이 없는 카테고리: 0.0
3. 나머지는 0.0~1.0 사이에서 상대적으로 점수 부여
4. 동일한 의미를 가진 표현에는 반드시 같은 점수 부여
   - 의미의 동일성은 카테고리와 의도를 종합하여 판단
   - 문장 구조나 표현 방식이 달라도 의미가 같으면 동일 점수
5. 절대적 점수보다 상대적 순위가 중요
6. **배치 내 모든 문장에서 일관된 기준 적용**

점수 기준:
(1) 연관성 점수(0.0~1.0)
   - 1.0: 가장 관련성 높음 (직접 언급 + 구체적 내용)
   - 0.8-0.9: 높은 관련성 (직접 언급)
   - 0.5-0.7: 보통 관련성 (간접적 언급)
   - 0.2-0.4: 약한 관련성 (약간 언급)
   - 0.0: 무관

(2) 구매의도 점수(0.0~1.0)
   - 1.0: 가장 강한 구매 의향
   - 0.8-0.9: 강한 구매 의향
   - 0.5-0.7: 보통 구매 의향
   - 0.2-0.4: 약한 구매 의향
   - 0.0: 구매 의향 없음

**일관성 체크**: 
- 동일한 의미를 가진 표현에는 반드시 같은 점수 부여
- 배치 내 모든 문장에서 동일한 기준 적용
- 상대적 중요도는 배치 전체를 고려하여 결정
</SYSTEM>
<USER>
입력:
- sentences: [{{idx:int, date:YYYY-MM-DD, text:str}}, ...]
- top_categories: [str, ...]

출력 스키마:
{{
  "unit":"batch",
  "results": [
    {{
      "sentence_idx": 0,  # 문장 인덱스
      "categories":[
        {{
          "name": "<top_category>",
          "relevance_raw": 0.0..1.0,  # 상대적 중요도 기준
          "intent_raw": 0.0..1.0,     # 상대적 중요도 기준
          "evidence_idx": [int, ...]
        }}, ...
      ]
    }}, ...
  ]
}}
데이터:
- sentences = {{sentences}}
- top_categories = {{parent_list}}
</USER>
"""

CHILD_BATCH_PROMPT = """\
<SYSTEM>
너는 "카카오톡 선물하기 하위 카테고리 배치 판단기"다.
여러 문장을 한 번에 분석하여 각 문장마다 각 하위 카테고리에 대해 연관성과 구매의도를 0.0~1.0으로 평가하라.

핵심 원칙:
1. 각 문장을 개별적으로 분석하여, 해당 문장에서 각 하위 카테고리의 중요도를 평가하라
2. 관련성이 없는 하위 카테고리: 0.0
3. 나머지는 0.0~1.0 사이에서 상대적으로 점수 부여
4. 동일한 의미를 가진 표현에는 반드시 같은 점수 부여
   - 의미의 동일성은 카테고리와 의도를 종합하여 판단
   - 문장 구조나 표현 방식이 달라도 의미가 같으면 동일 점수
5. 절대적 점수보다 상대적 순위가 중요
6. **배치 내 모든 문장에서 일관된 기준 적용**

점수 기준:
(1) 연관성 점수(0.0~1.0)
   - 1.0: 가장 관련성 높음 (직접 언급 + 구체적 내용)
   - 0.8-0.9: 높은 관련성 (직접 언급)
   - 0.5-0.7: 보통 관련성 (간접적 언급)
   - 0.2-0.4: 약한 관련성 (약간 언급)
   - 0.0: 무관

(2) 구매의도 점수(0.0~1.0)
   - 1.0: 가장 강한 구매 의향
   - 0.8-0.9: 강한 구매 의향
   - 0.5-0.7: 보통 구매 의향
   - 0.2-0.4: 약한 구매 의향
   - 0.0: 구매 의향 없음

**일관성 체크**: 
- 동일한 의미를 가진 표현에는 반드시 같은 점수 부여
- 배치 내 모든 문장에서 동일한 기준 적용
</SYSTEM>
<USER>
입력:
- sentences: [{{idx:int, date:str, text:str}}, ...]
- sub_categories: ["과일", "케이크", "의류", "신발", ...]

출력 스키마:
{{
  "unit":"batch",
  "results": [
    {{
      "sentence_idx": 0,  # 문장 인덱스
      "subcategories":[
        {{
          "name": "<sub_category>",
          "relevance_raw": 0.0..1.0,  # 상대적 중요도 기준
          "intent_raw": 0.0..1.0,     # 상대적 중요도 기준
          "evidence_idx": [int, ...]
        }}, ...
      ]
    }}, ...
  ]
}}
데이터:
- sentences = {{sentences}}
- sub_categories = {{child_list}}
</USER>
"""

# ===== 최종 상품 선택 프롬프트 =====
FINAL_SELECTION_PROMPT = """\
<SYSTEM>
너는 카카오톡 대화를 기반으로 가장 적합한 선물 5개를 추천하는 전문가다.

**추천 방식:**
- 제공된 Top-3 카테고리 정보를 우선 활용
- Evidence 문장과의 일치도를 중점적으로 고려
- 대상자 프로필(나이/성별/관계)을 항상 고려하여 맞춤형 추천
- 예산 범위 내에서 최적의 상품 선택

**핵심 원칙:**
- Top-3에 포함된 정보는 이미 의미있는 것
- 카테고리 점수보다 evidence 내용의 품질에 집중
- 프로필 정보를 활용하여 개인화된 추천
</SYSTEM>
<USER>
[대상자 정보]
- 나이: {age}세
- 성별: {gender}
- 관계: {relation}
- 예산: {budget_min:,}원 ~ {budget_max:,}원

[상위 카테고리 신호 및 근거]
{parent_scores_info}

[상위 카테고리 Evidence]
{parent_evidence_info}

[하위 카테고리 신호 (Top-3)]
{child_scores_info}

[하위 카테고리 Evidence]
{child_evidence_info}

[후보 상품들]
{candidate_products_info}

위 정보를 기반으로 가장 적합한 선물 5개를 구체적으로 추천하라.
특히 Top-3 카테고리 점수와 evidence 문장을 중점적으로 고려하라.

출력 (JSON):
{{
  "recommendations": [
    {{
      "product_name": "구체적 상품명",
      "category": "카테고리",
      "price_range": "가격대",
      "rationale": "추천 이유 (Top-3 카테고리 점수, evidence 매칭, 프로필 적합성 포함)"
    }},
    {{
      "product_name": "구체적 상품명", 
      "category": "카테고리",
      "price_range": "가격대",
      "rationale": "추천 이유 (Top-3 카테고리 점수, evidence 매칭, 프로필 적합성 포함)"
    }},
    {{
      "product_name": "구체적 상품명",
      "category": "카테고리", 
      "price_range": "가격대",
      "rationale": "추천 이유 (Top-3 카테고리 점수, evidence 매칭, 프로필 적합성 포함)"
    }},
    {{
      "product_name": "구체적 상품명",
      "category": "카테고리", 
      "price_range": "가격대",
      "rationale": "추천 이유 (Top-3 카테고리 점수, evidence 매칭, 프로필 적합성 포함)"
    }},
    {{
      "product_name": "구체적 상품명",
      "category": "카테고리", 
      "price_range": "가격대",
      "rationale": "추천 이유 (Top-3 카테고리 점수, evidence 매칭, 프로필 적합성 포함)"
    }}
  ]
}}
</USER>
"""

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
