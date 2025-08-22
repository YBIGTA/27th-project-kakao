"""
상품 랭킹/가드레일 노드 (product_node.py)

Input: candidates, profile, 상품 별 RAG 컨텍스트 + 문장 라우팅 연관 문장 모음
Process (택1):
    - LLM STRICT 선택 (후보 밖 금지 / 5개 / 예산·연령·관계 준수 / JSON only)
    - 실패 시 룰/임베딩 랭크 폴백 (리뷰수→위시수→평점 + 프로필 보너스)
Output: final_products (Top-5 + 이유)
"""

import os
import json
import re
from typing import Dict, Any, List, Optional
from ..prompts.final_product_prompts import FinalProductPrompts

class ProductNode:
    def __init__(self, max_products: int = 5):
        """
        Args:
            max_products: 최종 선택할 상품 수 (기본값: 5)
        """
        self.max_products = max_products
        self.llm_provider = os.getenv("LLM_PROVIDER", "upstage")
        self.llm_model = os.getenv("LLM_MODEL", "solar-1-mini-chat")
        self.prompts = FinalProductPrompts()
    
    async def select_final_products(
        self,
        candidates: List[Dict[str, Any]],
        profile: Dict[str, Any],
        rag_context: Dict[str, Any] = None,
        sentence_context: List[str] = None
    ) -> List[Dict[str, Any]]:
        """
        최종 상품을 선택합니다. 카테고리별로 1개씩 선택합니다.
        
        Args:
            candidates: 후보 상품 목록
            profile: 사용자 프로필
            rag_context: 상품별 RAG 컨텍스트 (선택사항)
            sentence_context: 문장 라우팅 연관 문장 모음 (선택사항)
            
        Returns:
            List[Dict]: 최종 선택된 상품 목록 (카테고리별 1개씩 + 이유)
        """
        if not candidates:
            return []
        
        # 1. LLM STRICT 선택 시도 (카테고리별 1개씩)
        try:
            llm_selections = await self._llm_strict_selection(
                candidates, profile, rag_context, sentence_context
            )
            if llm_selections:
                return llm_selections
        except Exception as e:
            print(f"LLM STRICT 선택 실패: {e}")
        
        # 2. 폴백: 룰/임베딩 랭크 (카테고리별 1개씩)
        return self._rule_embedding_rank_fallback_by_category(candidates, profile)
    
    async def _llm_strict_selection(
        self,
        candidates: List[Dict[str, Any]],
        profile: Dict[str, Any],
        rag_context: Dict[str, Any] = None,
        sentence_context: List[str] = None
    ) -> List[Dict[str, Any]]:
        """
        LLM STRICT 선택을 수행합니다.
        - 후보 밖 금지
        - 카테고리별 1개씩 선택
        - 예산·연령·관계 준수
        - JSON only 응답
        """
        system_prompt = self.prompts.system_prompt
        user_prompt = self.prompts.create_user_prompt(
            candidates, profile, rag_context, sentence_context
        )
        
        try:
            if self.llm_provider == "openai":
                return await self._call_openai_llm(system_prompt, user_prompt, candidates)
            elif self.llm_provider == "upstage":
                return await self._call_upstage_llm(system_prompt, user_prompt, candidates)
            else:
                raise RuntimeError(f"지원하지 않는 LLM provider: {self.llm_provider}")
        except Exception as e:
            print(f"LLM 호출 실패: {e}")
            return []
    

    
    async def _call_openai_llm(
        self, 
        system_prompt: str, 
        user_prompt: str, 
        candidates: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """OpenAI LLM을 호출합니다."""
        try:
            from openai import OpenAI
            client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            
            response = client.chat.completions.create(
                model=self.llm_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.2,
                response_format={"type": "json_object"},
                max_tokens=1000
            )
            
            content = response.choices[0].message.content
            return self.prompts.parse_response(content, candidates)
            
        except Exception as e:
            print(f"OpenAI LLM 호출 실패: {e}")
            return []
    
    async def _call_upstage_llm(
        self, 
        system_prompt: str, 
        user_prompt: str, 
        candidates: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Upstage LLM을 호출합니다."""
        try:
            import requests
            import time
            
            api_key = os.getenv("UPSTAGE_API_KEY")
            if not api_key:
                raise ValueError("UPSTAGE_API_KEY가 설정되지 않았습니다")
            
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "model": self.llm_model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                "temperature": 0.2,
                "max_tokens": 1000,
                "response_format": {"type": "json_object"}
            }
            
            # 재시도 로직
            for attempt in range(3):
                try:
                    response = requests.post(
                        "https://api.upstage.ai/v1/chat/completions",
                        headers=headers,
                        json=payload,
                        timeout=60
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        content = data["choices"][0]["message"]["content"]
                        return self.prompts.parse_response(content, candidates)
                    
                    if response.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                        time.sleep(1.5 ** attempt)
                        continue
                    
                    print(f"Upstage API 오류: {response.status_code}")
                    return []
                    
                except requests.Timeout:
                    if attempt < 2:
                        time.sleep(1.5 ** attempt)
                        continue
                    print("Upstage API 타임아웃")
                    return []
                    
        except Exception as e:
            print(f"Upstage LLM 호출 실패: {e}")
            return []
    

    

    
    def _rule_embedding_rank_fallback_by_category(
        self, 
        candidates: List[Dict[str, Any]], 
        profile: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        룰/임베딩 랭크 폴백을 수행합니다. 카테고리별로 1개씩 선택합니다.
        리뷰수 → 위시수 → 평점 + 프로필 보너스
        """
        # 카테고리별로 그룹화
        candidates_by_category = {}
        for candidate in candidates:
            category = candidate.get('sub_category', 'unknown')
            if category not in candidates_by_category:
                candidates_by_category[category] = []
            candidates_by_category[category].append(candidate)
        
        # 프로필 보너스 계산
        age = profile.get('age', 30)
        gender = profile.get('gender', 'unknown')
        relation = profile.get('relation', 'unknown')
        
        def calculate_profile_bonus(product: Dict[str, Any]) -> float:
            bonus = 0.0
            
            # 연령 관련 보너스 (예: 20대는 젊은 브랜드 선호)
            if age < 30:
                young_brands = {'nike', 'adidas', 'uniqlo', 'zara', 'h&m'}
                if product.get('brand', '').lower() in young_brands:
                    bonus += 0.1
            
            # 성별 관련 보너스 (예: 여성은 화장품/패션 선호)
            if gender == 'female':
                female_categories = {'화장품', '패션', '가방', '신발', '액세서리'}
                if any(cat in product.get('sub_category', '') for cat in female_categories):
                    bonus += 0.05
            
            # 관계 관련 보너스 (예: 연인은 로맨틱한 선물 선호)
            if relation in ['연인', '남자친구', '여자친구']:
                romantic_categories = {'화장품', '향수', '쥬얼리', '가방'}
                if any(cat in product.get('sub_category', '') for cat in romantic_categories):
                    bonus += 0.1
            
            return bonus
        
        # 랭킹 점수 계산
        def calculate_score(product: Dict[str, Any]) -> float:
            review_count = float(product.get('review_count', 0))
            wish_count = float(product.get('wish_count', 0))
            satisfaction = float(product.get('satisfaction_pct', 0))
            
            # 기본 점수: 리뷰수 + 위시수 + 만족도
            base_score = (
                min(review_count / 1000, 1.0) * 0.4 +  # 리뷰수 (최대 1.0)
                min(wish_count / 500, 1.0) * 0.3 +     # 위시수 (최대 1.0)
                (satisfaction / 100) * 0.3              # 만족도 (0-1)
            )
            
            # 프로필 보너스 추가
            profile_bonus = calculate_profile_bonus(product)
            
            return base_score + profile_bonus
        
        # 카테고리별로 최고 점수 상품 선택
        selected_products = []
        for category, category_candidates in candidates_by_category.items():
            if not category_candidates:
                continue
            
            # 점수 계산 및 정렬
            scored_candidates = []
            for candidate in category_candidates:
                score = calculate_score(candidate)
                scored_candidates.append({
                    **candidate,
                    'score': score,
                    'reason': f"리뷰수({candidate.get('review_count', 0)}) + 위시수({candidate.get('wish_count', 0)}) + 만족도({candidate.get('satisfaction_pct', 0)}%) + 프로필 보너스"
                })
            
            # 최고 점수 상품 선택
            best_candidate = max(scored_candidates, key=lambda x: x['score'])
            selected_products.append(best_candidate)
        
        return selected_products
