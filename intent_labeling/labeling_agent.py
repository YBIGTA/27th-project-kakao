# -*- coding: utf-8 -*-
from typing import Dict, List

class LabelingAgent:
    """라벨링을 위한 에이전트 클래스"""
    
    def __init__(self, few_shot_examples: Dict[str, List[str]] = None):
        """
        Args:
            few_shot_examples: 각 라벨별 few-shot 예시 딕셔너리
        """
        self.few_shot_examples = few_shot_examples or {}
        
    def build_prompt(self, text: str) -> str:
        """주어진 텍스트에 대한 라벨링 프롬프트 생성"""
        prompt = "아래 문장을 라벨링해줘.\n\n"
        
        # Few-shot 예시가 있으면 포함
        if self.few_shot_examples:
            prompt += "다음 예시들을 참고하여 라벨링해줘:\n\n"
            
            for label, examples in self.few_shot_examples.items():
                if examples:
                    prompt += f"라벨: {label}\n"
                    for example in examples:
                        prompt += f"문장: {example}\n"
                    prompt += "\n"
            
            prompt += "이제 아래 문장을 라벨링해줘:\n\n"
        
        prompt += f"문장: {text}"
        return prompt
    
    def get_few_shot_examples(self) -> Dict[str, List[str]]:
        """Few-shot 예시 반환"""
        return self.few_shot_examples
    
    def set_few_shot_examples(self, examples: Dict[str, List[str]]) -> None:
        """Few-shot 예시 설정"""
        self.few_shot_examples = examples
