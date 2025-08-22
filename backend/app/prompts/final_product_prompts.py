"""
최종 상품 선택용 LLM 프롬프트 (final_product_prompts.py)

후보 상품들 중에서 최종 상품을 선택하는 프롬프트들
"""

class FinalProductPrompts:
    def __init__(self):
        self.system_prompt = """당신은 선물 추천 전문가입니다. 
제공된 후보 상품 목록에서 사용자에게 가장 적합한 상품들을 선택해주세요.

다음 규칙을 엄격히 준수하세요:
1. 제공된 후보 상품 목록에서만 선택하세요 (후보 밖 금지)
2. 각 카테고리별로 정확히 1개의 상품을 선택하세요
3. 사용자의 예산, 연령, 관계를 반드시 고려하세요
4. JSON 형식으로만 응답하세요
5. 각 상품에 대해 선택 이유를 명시하세요

응답 형식:
{
  "selections": [
    {
      "product_name": "상품명",
      "brand": "브랜드",
      "price": 가격,
      "product_url": "URL",
      "reason": "선택 이유"
    }
  ]
}"""

    def create_user_prompt(
        self, 
        candidates: list, 
        user_profile: dict,
        rag_context: dict = None,
        sentence_context: list = None
    ) -> str:
        """사용자 프롬프트를 생성합니다."""
        # 카테고리별로 그룹화
        candidates_by_category = {}
        for product in candidates:
            category = product.get('sub_category', 'unknown')
            if category not in candidates_by_category:
                candidates_by_category[category] = []
            candidates_by_category[category].append(product)
        
        prompt = f"""
사용자 프로필:
- 연령: {user_profile.get('age', 'N/A')}세
- 성별: {user_profile.get('gender', 'N/A')}
- 관계: {user_profile.get('relation', 'N/A')}
- 예산: {user_profile.get('budget_min', 'N/A')}원 ~ {user_profile.get('budget_max', 'N/A')}원

후보 상품 목록 (카테고리별):
"""
        
        for category, category_products in candidates_by_category.items():
            prompt += f"\n[{category} 카테고리]:"
            for i, product in enumerate(category_products, 1):
                prompt += f"""
{i}. {product.get('product_name', 'N/A')}
   - 브랜드: {product.get('brand', 'N/A')}
   - 가격: {product.get('price', 'N/A')}원
   - 리뷰 수: {product.get('review_count', 'N/A')}
   - 위시 수: {product.get('wish_count', 'N/A')}
   - 만족도: {product.get('satisfaction_pct', 'N/A')}%
   - URL: {product.get('product_url', 'N/A')}
"""
        
        if rag_context:
            prompt += f"\n추가 컨텍스트: {rag_context}"
        
        if sentence_context:
            prompt += f"\n관련 문장들: {' '.join(sentence_context)}"
        
        prompt += "\n\n위 정보를 바탕으로 각 카테고리별로 가장 적합한 1개씩 상품을 선택해주세요."
        
        return prompt

    def parse_response(self, response_text: str, candidates: list) -> list:
        """LLM 응답을 파싱하고 검증합니다."""
        try:
            # JSON 추출
            data = self._extract_json(response_text)
            selections = data.get("selections", [])
            
            if not isinstance(selections, list):
                return []
            
            # 후보 검증 및 매칭
            validated_selections = []
            candidate_names = {self._normalize_name(p.get("product_name", "")) for p in candidates}
            
            for selection in selections:
                if not isinstance(selection, dict):
                    continue
                
                product_name = selection.get("product_name", "")
                if not product_name:
                    continue
                
                # 후보 목록에 있는지 확인
                if self._normalize_name(product_name) in candidate_names:
                    # 원본 후보에서 정보 가져오기
                    original_candidate = next(
                        (c for c in candidates 
                         if self._normalize_name(c.get("product_name", "")) == self._normalize_name(product_name)),
                        None
                    )
                    
                    if original_candidate:
                        validated_selections.append({
                            "product_name": original_candidate.get("product_name"),
                            "brand": original_candidate.get("brand"),
                            "price": original_candidate.get("price"),
                            "product_url": original_candidate.get("product_url"),
                            "reason": selection.get("reason", "LLM 추천")
                        })
            
            return validated_selections
            
        except Exception as e:
            print(f"LLM 응답 파싱 실패: {e}")
            return []
    
    def _extract_json(self, content: str) -> dict:
        """JSON을 추출합니다."""
        import json
        import re
        
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            # 정규식으로 JSON 추출 시도
            match = re.search(r"\{.*\}", content, re.DOTALL)
            if match:
                return json.loads(match.group(0))
            raise
    
    def _normalize_name(self, name: str) -> str:
        """상품명을 정규화합니다."""
        return (name or "").strip().lower()
