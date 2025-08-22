"""
하위 카테고리 노드 (lowercategory_node.py)

Input: preprocessed_data (CSV 파일 경로 + 사용자 프로필)
Process:
    - LLM을 통한 모든 하위 카테고리 confidence 계산
    - 부모별 안정화 softmax → P(c|p,x)
    - ε-floor & 재정규화 (부모별로 적용)
Output: probs_lower_by_parent={p:{c:prob}}, reasoning, confidence_data
"""

import os
import json
import re
import requests
import time
from typing import Dict, Any, List, Tuple
import numpy as np
from ..prompts.lower_category_prompts import LowerCategoryPrompts

class LowerCategoryNode:
    def __init__(self):
        self.llm_provider = os.getenv("LLM_PROVIDER", "upstage")
        self.llm_model = os.getenv("LLM_MODEL", "solar-1-mini-chat")
        self.prompts = LowerCategoryPrompts()
        
        # 실제 카카오 선물하기 하위 카테고리 매핑
        self.sub_categories_by_parent = {
            "교환권": ["베이커리/도넛/떡", "카페", "아이스크림/빙수", "치킨", "버거/피자", "편의점", "한식/중식/일식", "패밀리/호텔뷔페", "퓨전/외국/펍", "분식/죽/도시락"],
            "상품권": ["상품권/마트", "뷰티/패션/건강", "영화/OTT/게임", "전시/테마/체험", "생활/교육/취미", "종교/나눔"],
            "뷰티": ["명품화장품", "향수", "헤어/네일/스파", "바디", "스킨케어", "메이크업", "헤어/미용", "남성화장품"],
            "패션": ["명품브랜드", "쥬얼리", "파자마", "브랜드 가방/지갑", "브랜드 의류", "브랜드 신발", "언더웨어", "디자이너 브랜드", "브랜드 잡화", "브랜드 시계", "주문각인"],
            "식품": ["과일/견과/채소", "축산/수산", "쌀/반찬/김치", "건강식품", "다이어트/이너뷰티", "가공/보양식", "케이크", "디저트", "유제품/아이스크림", "커피/차/음료"],
            "와인/양주/전통주": ["와인", "양주", "전통주", "맥주/기타"],
            "리빙/도서": ["주방/수입주방", "캔들/디퓨저/인센스", "식물/꽃배달", "침대/패브릭", "조명/무드등", "인테리어", "생필품", "수납/생활", "가구/DIY", "팬시/캐릭터", "문구/취미", "도서", "명품리빙", "리빙편집샵"],
            "레저/스포츠": ["글로벌 브랜드본사", "스포츠 의류", "스포츠 슈즈", "스포츠 잡화", "요가/헬스/수영", "레저/캠핑", "등산/아웃도어", "차량용품", "여행용품", "차량용 방향제", "골프선물", "골프/테니스"],
            "아티스트/캐릭터": ["스타앨범", "애니메이션 캐릭터", "인디작가", "웹소설", "게임"],
            "유아동/반려": ["신생아선물세트", "베이비패션", "키즈패션", "애니멀캐릭터", "임신/출산/육아", "장난감/인형", "유아교육/도서", "기저귀/물티슈", "분유/간식/영양제", "강아지 간식/용품", "고양이 간식/용품", "기타 소동물용품"],
            "디지털/가전": ["프리미엄 가전", "케이스", "모바일 액세서리", "미니가전", "건강용품/가전", "디지털/음향기기", "생활가전", "주방가전", "미용가전", "카메라"],
            "카카오프렌즈": ["토이", "리빙", "테크", "문구", "패션", "푸드", "골프"]
        }
    
    def process(
        self, 
        preprocessed_data: Dict[str, Any]
    ) -> Tuple[Dict[str, Dict[str, float]], str, Dict[str, Any]]:
        """
        하위 카테고리 확률을 계산합니다.
        
        Args:
            preprocessed_data: 전처리된 데이터
            
        Returns:
            Tuple[Dict[str, Dict[str, float]], str, Dict[str, Any]]: (부모별 하위 카테고리 확률, 추론 과정, confidence 데이터)
        """
        try:
            # 1. 대화 텍스트 추출
            conversation_text = self._extract_conversation_text(preprocessed_data)
            
            # 2. 상위 카테고리 리스트 생성
            top_category_list = list(self.sub_categories_by_parent.keys())
            
            # 3. 하위 카테고리 리스트 생성 (상위/하위 형태)
            sub_category_list = []
            for parent, children in self.sub_categories_by_parent.items():
                for child in children:
                    sub_category_list.append(f"{parent}/{child}")
            
            # 4. LLM을 통한 하위 카테고리 confidence 계산
            llm_response = self._call_llm_for_lower_categories(
                conversation_text, top_category_list, sub_category_list
            )
            
            # 5. LLM 응답 파싱
            confidence_data, reasoning = self.prompts.parse_response(llm_response)
            
            # 6. confidence를 확률로 변환 (부모별 안정화 softmax + ε-floor)
            probs_lower_by_parent = self._convert_confidence_to_probabilities(
                confidence_data
            )
            
            # 7. 추론 과정 생성 (이미 파싱에서 받았으므로 그대로 사용)
            return probs_lower_by_parent, reasoning, confidence_data
            
        except Exception as e:
            print(f"하위 카테고리 노드 오류: {e}")
            # 폴백: 균등 분포
            probs_lower_by_parent = {}
            for parent, children in self.sub_categories_by_parent.items():
                probs_lower_by_parent[parent] = {
                    child: 1.0/len(children) for child in children
                }
            reasoning = f"오류로 인한 균등 분포 적용: {str(e)}"
            confidence_data = {}
            return probs_lower_by_parent, reasoning, confidence_data
    
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
    
    def _call_llm_for_lower_categories(
        self,
        conversation_text: str,
        top_category_list: List[str],
        sub_category_list: List[str]
    ) -> str:
        """LLM을 호출하여 하위 카테고리 confidence를 계산합니다."""
        system_prompt = self.prompts.system_prompt
        user_prompt = self.prompts.create_user_prompt(
            conversation_text=conversation_text,
            top_category_list=top_category_list,
            sub_category_list=sub_category_list
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
    
    def _convert_confidence_to_probabilities(
        self, 
        confidence_data: Dict[str, float]
    ) -> Dict[str, Dict[str, float]]:
        """confidence를 확률로 변환합니다 (부모별 안정화 softmax + ε-floor)."""
        probs_lower_by_parent = {}
        
        for parent, children in self.sub_categories_by_parent.items():
            
            # 해당 부모의 하위 카테고리 confidence 추출
            child_confidences = []
            child_names = []
            
            for child in children:
                sub_cat_key = f"{parent}/{child}"
                confidence = confidence_data.get(sub_cat_key, 0.0)
                child_confidences.append(confidence)
                child_names.append(child)
            
            # 부모별 안정화 softmax 적용
            if child_confidences:
                child_probs = self._parent_wise_stabilized_softmax(child_confidences)
                # ε-floor 및 재정규화
                child_probs = self._epsilon_floor_and_renormalize(child_probs)
                
                # 결과 딕셔너리 생성
                probs_lower_by_parent[parent] = {
                    child: prob for child, prob in zip(child_names, child_probs)
                }
        
        return probs_lower_by_parent
    
    def _parent_wise_stabilized_softmax(self, logits: List[float]) -> List[float]:
        """부모별 안정화 softmax를 적용합니다."""
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
    
    def _epsilon_floor_and_renormalize(self, probs: List[float], epsilon: float = 0.01) -> List[float]:
        """ε-floor를 적용하고 재정규화합니다."""
        # 최소값 적용
        probs = [max(prob, epsilon) for prob in probs]
        
        # 재정규화
        sum_probs = sum(probs)
        if sum_probs > 0:
            probs = [prob / sum_probs for prob in probs]
        
        return probs
