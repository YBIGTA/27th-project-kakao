
# -*- coding: utf-8 -*-
import json
import time
import requests
from typing import Dict, Any, List
from config import GROQ_MODEL, API_KEYS, GROQ_TIMEOUT

SYSTEM_PROMPT = """너의 임무는 사용자가 한 문장에서 드러내는 **핵심 의도**를 다섯 가지 라벨 중 하나로 분류하는 것이다.  
출력은 JSON 형식으로 `{"label": <라벨명>, "confidence": <0.0~1.0>}`만 반환하라.  

### 라벨 정의 (겹치지 않도록 경계 명확히)

1. 필요  
- 정의: 지금 쓰는 물건에 문제·부족·고장이 있어서 교체/보충이 필요한 상황  
- 키워드: "망가졌어", "떨어졌어", "버벅거려", "없어져서 필요해"  
- 예: "휴대폰 케이스가 찢어졌어"

2. 구매  
- 정의: 앞으로 **살/구입할 의사나 계획**을 명확히 드러냄  
- 키워드: "살 거야", "사야겠다", "사고 싶어", "장만할까"  
- 예: "향수 새로 사야겠다"

3. 관심  
- 정의: 특정 상품/서비스에 대해 흥미·호기심은 표현했지만, **직접 사거나 필요하다는 말은 없음**  
- 키워드: "궁금", "예쁘다", "마음에 들어", "봤어?"  
- 예: "한솥에서 신메뉴 나왔다는데 궁금"

4. 부정  
- 정의: 상품/서비스에 대해 부정적 반응, 거부, 무관심 표현  
- 키워드: "싫어", "별로야", "안 좋아해", "전혀 흥미 없어"  
- 예: "나는 립스틱은 별로야"

5. 단순 언급  
- 정의: 일상 공유나 단순 서술. **구매·관심·필요·부정과 연결되지 않음**  
- 키워드: "먹었어", "봤어", "다녀왔어", "날씨"  
- 예: "한솥 시켜줘서 그거 먹음"

---

### 판단 절차
1. 먼저 **부정** 신호가 있으면 → `부정`  
2. 아니면 **현재 물건의 결핍/고장** 언급 → `필요`  
3. 아니면 **앞으로 살 의사** 언급 → `구매`  
4. 아니면 **상품에 대한 흥미·호기심** 표현 → `관심`  
5. 그 외는 전부 → `단순 언급`"""

def build_few_shot_prompt(text: str, few_shot_examples: Dict[str, List[str]]) -> str:
    """Few-shot 예시를 포함한 프롬프트 생성"""
    prompt = "다음 예시들을 참고하여 문장을 라벨링해주세요:\n\n"
    
    # 각 라벨별로 예시 추가
    for label, examples in few_shot_examples.items():
        if examples:
            prompt += f"**{label}** 예시:\n"
            for example in examples[:3]:  # 최대 3개 예시
                prompt += f"- {example}\n"
            prompt += "\n"
    
    prompt += f"이제 다음 문장을 라벨링해주세요:\n문장: \"{text}\"\n\n"
    prompt += "JSON 형식으로 응답해주세요:\n{\"label\": \"라벨명\", \"confidence\": 0.0~1.0, \"reason\": \"분류 이유\"}"
    
    return prompt

def call_groq(user_prompt: str, few_shot_examples: Dict[str, List[str]]) -> Dict[str, Any]:
    """Groq API 호출 (비활성화 시 OpenAI 사용)"""
    # Groq가 비활성화된 경우 OpenAI 사용
    if GROQ_MODEL is None:
        try:
            from openai_refiner import call_openai
            return call_openai(user_prompt)
        except Exception as e:
            print(f"OpenAI 호출 중 오류: {e}")
            return {
                "label": "단순 언급",
                "confidence": 0.0,
                "reason": f"OpenAI 호출 실패: {str(e)}"
            }
    
    try:
        # Few-shot 예시를 포함한 프롬프트 생성
        full_prompt = build_few_shot_prompt(user_prompt, few_shot_examples)
        
        headers = {
            "Authorization": f"Bearer {API_KEYS['groq']}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": GROQ_MODEL,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": full_prompt}
            ],
            "temperature": 0.1,  # 낮은 temperature로 일관성 향상
            "max_tokens": 200
        }
        
        response = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers=headers,
            json=data,
            timeout=GROQ_TIMEOUT
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result["choices"][0]["message"]["content"]
            
            try:
                # JSON 파싱 시도
                parsed = json.loads(content)
                return {
                    "label": parsed.get("label", "단순 언급"),
                    "confidence": float(parsed.get("confidence", 0.5)),
                    "reason": parsed.get("reason", "분류 완료")
                }
            except json.JSONDecodeError:
                # JSON 파싱 실패 시 텍스트에서 라벨 추출
                for label in ["필요", "구매", "관심", "부정", "단순 언급"]:
                    if label in content:
                        return {
                            "label": label,
                            "confidence": 0.6,
                            "reason": f"JSON 파싱 실패, 텍스트에서 {label} 추출"
                        }
                
                return {
                    "label": "단순 언급",
                    "confidence": 0.5,
                    "reason": "JSON 파싱 실패, 기본값 사용"
                }
        elif response.status_code == 429:
            print(f"Groq API Rate Limit (429) - 30초 대기 후 재시도")
            time.sleep(30)  # 30초 대기
            # 재시도 로직
            try:
                response = requests.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers=headers,
                    json=data,
                    timeout=GROQ_TIMEOUT
                )
                if response.status_code == 200:
                    result = response.json()
                    content = result["choices"][0]["message"]["content"]
                    # JSON 파싱 시도
                    try:
                        parsed = json.loads(content)
                        return {
                            "label": parsed.get("label", "단순 언급"),
                            "confidence": float(parsed.get("confidence", 0.5)),
                            "reason": parsed.get("reason", "재시도 성공")
                        }
                    except json.JSONDecodeError:
                        return {
                            "label": "단순 언급",
                            "confidence": 0.5,
                            "reason": "재시도 성공, JSON 파싱 실패"
                        }
            except Exception as e:
                print(f"재시도 실패: {e}")
        
        print(f"Groq API 호출 실패: {response.status_code} {response.reason}")
        return {
            "label": "단순 언급",
            "confidence": 0.0,
            "reason": f"API 오류: {response.status_code}"
        }
            
    except Exception as e:
        print(f"Groq API 호출 중 오류: {e}")
        return {
            "label": "단순 언급",
            "confidence": 0.0,
            "reason": f"예외 발생: {str(e)}"
        }
