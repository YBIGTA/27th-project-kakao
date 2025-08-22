"""
상위 카테고리 노드 (uppercategory_node.py)

Input: preprocessed_data (CSV 파일 경로 + 사용자 프로필)
Process:
    - LLM을 통한 모든 상위 카테고리 confidence 계산
    - 안정화 softmax (max-shift) → P(p|x)
    - ε-floor
Output: probs_upper={parent: prob}, reasoning, confidence_data
"""

import os
import json
import re
import requests
import time
from typing import Dict, Any, List, Tuple
import numpy as np
from ..prompts.upper_category_prompts import UpperCategoryPrompts

class UpperCategoryNode:
    def __init__(self):
        self.llm_provider = os.getenv("LLM_PROVIDER", "upstage")
        self.llm_model = os.getenv("LLM_MODEL", "solar-1-mini-chat")
        self.prompts = UpperCategoryPrompts()
        
        # 카카오 선물하기 상위 카테고리
        self.top_categories = [
            "교환권", "상품권", "뷰티", "패션", "식품", "와인/양주/전통주",
            "리빙/도서", "레저/스포츠", "아티스트/캐릭터", "유아동/반려",
            "디지털/가전", "카카오프렌즈"
        ]
    
    def process(
        self, 
        preprocessed_data: Dict[str, Any]
    ) -> Tuple[Dict[str, float], str, Dict[str, Any]]:
        """
        상위 카테고리 확률을 계산합니다.
        
        Args:
            preprocessed_data: 전처리된 데이터
            
        Returns:
            Tuple[Dict[str, float], str, Dict[str, Any]]: (상위 카테고리 확률, 추론 과정, confidence 데이터)
        """
        try:
            # 1. 대화 텍스트 추출
            conversation_text = self._extract_conversation_text(preprocessed_data)
            
            # 2. LLM을 통한 상위 카테고리 confidence 계산
            llm_response = self._call_llm_for_upper_categories(
                conversation_text
            )
            
            # 3. LLM 응답 파싱
            confidence_data, reasoning = self.prompts.parse_response(llm_response)
            
            # 4. confidence를 확률로 변환 (안정화 softmax + ε-floor)
            probs_upper = self._convert_confidence_to_probabilities(confidence_data)
            
            # 5. 추론 과정 생성 (이미 파싱에서 받았으므로 그대로 사용)
            return probs_upper, reasoning, confidence_data
            
        except Exception as e:
            print(f"상위 카테고리 노드 오류: {e}")
            # 폴백: 균등 분포
            probs_upper = {cat: 1.0/len(self.top_categories) for cat in self.top_categories}
            reasoning = f"오류로 인한 균등 분포 적용: {str(e)}"
            confidence_data = {}
            return probs_upper, reasoning, confidence_data
    
    def _extract_conversation_text(self, preprocessed_data: Dict[str, Any]) -> str:
        """전처리된 데이터에서 대화 텍스트를 추출합니다."""
        # CSV 파일 경로가 있으면 파일을 읽어서 텍스트 추출
        if "csv_file_path" in preprocessed_data:
            try:
                import pandas as pd
                df = pd.read_csv(preprocessed_data["csv_file_path"])
                if "text" in df.columns:
                    return " ".join(df["text"].dropna().astype(str))
                elif "message" in df.columns:
                    return " ".join(df["message"].dropna().astype(str))
            except Exception as e:
                print(f"CSV 파일 읽기 실패: {e}")
        
        # 기본값
        return "대화 내용을 추출할 수 없습니다."
    
    def _call_llm_for_upper_categories(
        self, 
        conversation_text: str
    ) -> str:
        """LLM을 호출하여 상위 카테고리 confidence를 계산합니다."""
        system_prompt = self.prompts.system_prompt
        user_prompt = self.prompts.create_user_prompt(
            conversation_text=conversation_text,
            top_category_list=self.top_categories
        )
        
        try:
            return self._call_upstage_llm(system_prompt, user_prompt)
        except Exception as e:
            print(f"LLM 호출 실패: {e}")
            raise
    

    
    def _call_upstage_llm(self, system_prompt: str, user_prompt: str) -> str:
        """Upstage LLM을 호출합니다."""
        headers = {
            "Authorization": f"Bearer {os.getenv('UPSTAGE_API_KEY')}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": self.llm_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": 0.1,
            "max_tokens": 2000
        }
        
        response = requests.post(
            "https://api.upstage.ai/v1/chat/completions",
            headers=headers,
            json=data,
            timeout=30
        )
        
        if response.status_code != 200:
            raise RuntimeError(f"Upstage API 오류: {response.status_code}")
        
        return response.json()["choices"][0]["message"]["content"]
    
    def _convert_confidence_to_probabilities(self, confidence_data: Dict[str, float]) -> Dict[str, float]:
        """confidence를 확률로 변환합니다 (안정화 softmax + ε-floor)."""
        if not confidence_data:
            return {cat: 1.0/len(self.top_categories) for cat in self.top_categories}
        
        # 모든 카테고리에 대해 confidence 값 준비
        confidences = []
        categories = []
        
        for cat in self.top_categories:
            confidences.append(confidence_data.get(cat, 0.0))
            categories.append(cat)
        
        # 안정화 softmax 적용
        probs = self._stabilized_softmax(confidences)
        
        # ε-floor 적용
        probs = self._epsilon_floor(probs)
        
        # 결과 딕셔너리 생성
        return {cat: prob for cat, prob in zip(categories, probs)}
    
    def _stabilized_softmax(self, logits: List[float]) -> List[float]:
        """안정화된 softmax를 적용합니다."""
        if not logits:
            return []
        
        # 최대값 찾기
        max_logit = max(logits)
        
        # 안정화된 지수 계산
        exp_logits = [np.exp(logit - max_logit) for logit in logits]
        sum_exp = sum(exp_logits)
        
        # 확률 계산
        if sum_exp == 0:
            return [1.0/len(logits)] * len(logits)
        
        return [exp_logit / sum_exp for exp_logit in exp_logits]
    
    def _epsilon_floor(self, probs: List[float], epsilon: float = 0.01) -> List[float]:
        """ε-floor를 적용하여 최소 확률을 보장합니다."""
        # 최소값 적용
        probs = [max(prob, epsilon) for prob in probs]
        
        # 재정규화
        sum_probs = sum(probs)
        if sum_probs > 0:
            probs = [prob / sum_probs for prob in probs]
        
        return probs
