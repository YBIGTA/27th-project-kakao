import json
from typing import Dict, Any, List
from ..state import GraphState
from ..llm.client import llm_client
from ..llm.prompts import format_final_selection_prompt
from ...config.settings import PRODUCT_SELECTION_WEIGHTS

async def product_node(state: GraphState) -> GraphState:
    """
    최종 상품 선택 노드: LLM 기반 스마트 선택 + 룰 기반 폴백
    
    Args:
        state: GraphState 객체
        
    Returns:
        GraphState: selected_products와 rationales가 업데이트된 상태
    """
    candidates = state.candidate_products
    if not candidates:
        state.selected_products = []
        state.rationales = {}
        return state
    
    print(f"Product Node 시작: {len(candidates)}개 후보 상품")
    
    # 1차: LLM 기반 스마트 선택 시도
    try:
        print("LLM 기반 상품 선택 시도 중...")
        llm_selections = await _llm_smart_selection(state, candidates)
        
        if llm_selections and len(llm_selections) >= 3:  # 최소 3개 이상
            print(f"LLM 선택 성공: {len(llm_selections)}개 상품")
            state.selected_products = llm_selections[:5]
            state.rationales = {
                p["id"]: p.get("rationale", "LLM 기반 맥락적 선택")
                for p in state.selected_products
            }
            state.debug["selection_method"] = "llm_smart"
            return state
        else:
            print(f"LLM 선택 결과 부족: {len(llm_selections) if llm_selections else 0}개")
            
    except Exception as e:
        print(f"LLM 선택 실패: {e}")
        state.debug["llm_selection_error"] = str(e)
    
    # 2차: 향상된 룰 기반 폴백
    print("🔧 룰 기반 폴백 선택 시작...")
    fallback_selections = _enhanced_rule_based_selection(state, candidates)
    state.selected_products = fallback_selections[:5]
    state.rationales = {
        p["id"]: p.get("reason", "룰 기반 선택")
        for p in state.selected_products
    }
    state.debug["selection_method"] = "enhanced_rule"
    
    print(f"최종 선택 완료: {len(state.selected_products)}개 상품")
    return state

async def _llm_smart_selection(
    state: GraphState, 
    candidates: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """LLM을 사용한 스마트 상품 선택 (기존 FINAL_SELECTION_PROMPT 사용)"""
    
    try:
        # 🚀 기존 FINAL_SELECTION_PROMPT 사용
        prompt = format_final_selection_prompt(state, candidates)
        print(f"프롬프트 생성 완료 (길이: {len(prompt)})")
        
        # LLM 클라이언트로 호출
        response = await llm_client._call_upstage_api(prompt, llm_client._get_next_api_key())
        
        content = response["choices"][0]["message"]["content"]
        print(f"LLM 응답: {content[:200]}...")
        
        result = json.loads(content)
        return _parse_llm_final_response(result, candidates)
        
    except Exception as e:
        print(f"LLM 호출 실패: {e}")
        import traceback
        traceback.print_exc()
        raise

def _parse_llm_final_response(
    response: Dict[str, Any], 
    candidates: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """LLM 최종 응답 파싱 및 검증"""
    
    product_map = {p["id"]: p for p in candidates}
    selected_ids = response.get("selected", [])
    rationales = response.get("rationale", {})
    
    result: List[Dict[str, Any]] = []
    
    for product_id in selected_ids:
        if product_id in product_map:
            product = product_map[product_id].copy()
            product["rationale"] = rationales.get(str(product_id), "LLM 선택")
            result.append(product)
            
            if len(result) >= 5:
                break
    
    print(f"LLM 응답 파싱 완료: {len(result)}개 상품")
    return result

def _enhanced_rule_based_selection(
    state: GraphState, 
    candidates: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """향상된 룰 기반 선택 (프로필 보너스 + 브랜드 중복 제거 + 카테고리 다양성)"""
    
    ctx = state.ctx
    
    def calculate_enhanced_score(product: Dict[str, Any]) -> float:
        # 기본 점수: 카테고리 신호 + 인기도
        category_score = state.final_child_scores.get(product.get("category_child", ""), 0.0)
        popularity_score = product.get("popularity_score", 0.0)
        
        weights = PRODUCT_SELECTION_WEIGHTS
        base_score = weights["category"] * category_score + weights["popularity"] * popularity_score
        
        # 프로필 기반 보너스
        bonus = 0.0
        
        # 연령 보너스
        if ctx.age < 30:
            young_brands = {"nike", "adidas", "uniqlo", "zara", "스타벅스", "투썸플레이스"}
            if any(brand in product.get("brand", "").lower() for brand in young_brands):
                bonus += 0.05
        elif ctx.age > 50:
            premium_brands = {"샤넬", "디올", "에르메스", "롤렉스"}
            if any(brand in product.get("brand", "") for brand in premium_brands):
                bonus += 0.05
        
        # 성별 보너스
        if ctx.gender == "F":
            female_categories = {"향수", "화장품", "스킨케어", "메이크업", "주얼리"}
            if product.get("category_child") in female_categories:
                bonus += 0.03
        elif ctx.gender == "M":
            male_categories = {"디지털/가전", "스포츠", "게임", "전자기기"}
            if any(cat in product.get("category_child", "") for cat in male_categories):
                bonus += 0.03
        
        # 관계 보너스
        if ctx.relation in ["연인", "남자친구", "여자친구"]:
            romantic_categories = {"향수", "주얼리", "케이크", "디저트", "꽃"}
            if any(cat in product.get("category_child", "") for cat in romantic_categories):
                bonus += 0.04
        elif ctx.relation in ["가족", "부모님"]:
            family_categories = {"건강식품", "생활용품", "의류"}
            if any(cat in product.get("category_child", "") for cat in family_categories):
                bonus += 0.04
        
        return base_score + bonus
    
    # 모든 후보에 점수 계산
    scored_products = []
    for product in candidates:
        score = calculate_enhanced_score(product)
        scored_products.append({
            **product,
            "enhanced_score": score
        })
    
    # 점수 순으로 정렬
    scored_products.sort(key=lambda x: x["enhanced_score"], reverse=True)
    
    # 브랜드 중복 제거 + 카테고리 다양성 보장
    selected: List[Dict[str, Any]] = []
    seen_brands = set()
    seen_categories = set()
    
    # 1차: 다양성 우선 선택
    for product in scored_products:
        if len(selected) >= 5:
            break
            
        brand = str(product.get("brand", "")).strip().lower()
        category = product.get("sub_category", "")
        title = str(product.get("product_name", "")).strip().lower()
        
        # 브랜드 중복 체크
        if brand in seen_brands:
            continue
            
        # 제목 유사도 체크 (간단한 문자열 포함 관계)
        too_similar = any(
            title in s.get("product_name", "").lower() or s.get("product_name", "").lower() in title
            for s in selected
        )
        if too_similar:
            continue
        
        # 카테고리 다양성 체크 (초기 3개까지는 엄격)
        if len(selected) < 3 and category in seen_categories:
            continue
        
        # 선택
        product["reason"] = f"{category} 카테고리 신호({state.final_child_scores.get(category, 0):.3f}) + 프로필 적합성"
        selected.append(product)
        seen_brands.add(brand)
        seen_categories.add(category)
    
    # 2차: 부족한 경우 조건 완화하여 추가 선택
    if len(selected) < 5:
        for product in scored_products:
            if len(selected) >= 5:
                break
                
            brand = str(product.get("brand", "")).strip().lower()
            
            # 이미 선택된 상품 스킵
            if any(p["id"] == product["id"] for p in selected):
                continue
                
            # 브랜드 중복만 체크 (카테고리 다양성 완화)
            if brand in seen_brands:
                continue
            
            product["reason"] = f"보완 선택: {product.get('category_child', '')} 카테고리"
            selected.append(product)
            seen_brands.add(brand)
    
    return selected[:5]