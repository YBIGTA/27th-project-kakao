# -*- coding: utf-8 -*-
import json
import time
import requests
from typing import Dict, Any
from config import OPENAI_MODEL, API_KEYS, OPENAI_TIMEOUT

SYSTEM_PROMPT = """너의 임무는 사용자가 한 문장에서 드러내는 **핵심 의도**를 다섯 가지 라벨 중 하나로 정확하게 분류하는 것이다.  
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
5. 그 외는 전부 → `단순 언급`

**중요**: 판단 절차를 순서대로 따라가며 가장 적합한 라벨을 선택하라."""

def call_openai(user_prompt: str) -> Dict[str, Any]:
    """OpenAI API 호출하여 정확한 라벨링 수행"""
    try:
        headers = {
            "Authorization": f"Bearer {API_KEYS['openai']}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": OPENAI_MODEL,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"다음 문장을 정확하게 라벨링해주세요:\n\n문장: \"{user_prompt}\"\n\nJSON 형식으로 응답해주세요:\n{{\"label\": \"라벨명\", \"confidence\": 0.0~1.0, \"reason\": \"분류 이유\"}}"}
            ],
            "temperature": 0.1,  # 낮은 temperature로 일관성 향상
            "max_tokens": 300
        }
        
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json=data,
            timeout=OPENAI_TIMEOUT
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result["choices"][0]["message"]["content"]
            
            try:
                # JSON 파싱 시도
                parsed = json.loads(content)
                return {
                    "label": parsed.get("label", "단순 언급"),
                    "confidence": float(parsed.get("confidence", 0.8)),  # OpenAI는 더 높은 신뢰도
                    "reason": parsed.get("reason", "정확한 분류 완료")
                }
            except json.JSONDecodeError:
                # JSON 파싱 실패 시 텍스트에서 라벨 추출
                for label in ["필요", "구매", "관심", "부정", "단순 언급"]:
                    if label in content:
                        return {
                            "label": label,
                            "confidence": 0.7,
                            "reason": f"JSON 파싱 실패, 텍스트에서 {label} 추출"
                        }
                
                return {
                    "label": "단순 언급",
                    "confidence": 0.6,
                    "reason": "JSON 파싱 실패, 기본값 사용"
                }
        elif response.status_code == 429:
            print(f"OpenAI API Rate Limit (429) - 30초 대기 후 재시도")
            time.sleep(30)  # 30초 대기
            # 재시도 로직
            try:
                response = requests.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers=headers,
                    json=data,
                    timeout=OPENAI_TIMEOUT
                )
                if response.status_code == 200:
                    result = response.json()
                    content = result["choices"][0]["message"]["content"]
                    # JSON 파싱 시도
                    try:
                        parsed = json.loads(content)
                        return {
                            "label": parsed.get("label", "단순 언급"),
                            "confidence": float(parsed.get("confidence", 0.8)),
                            "reason": parsed.get("reason", "재시도 성공")
                        }
                    except json.JSONDecodeError:
                        return {
                            "label": "단순 언급",
                            "confidence": 0.7,
                            "reason": "재시도 성공, JSON 파싱 실패"
                        }
            except Exception as e:
                print(f"재시도 실패: {e}")
        
        print(f"OpenAI API 호출 실패: {response.status_code} {response.reason}")
        return {
            "label": "단순 언급",
            "confidence": 0.0,
            "reason": f"API 오류: {response.status_code}"
        }
            
    except Exception as e:
        print(f"OpenAI API 호출 중 오류: {e}")
        return {
            "label": "단순 언급",
            "confidence": 0.0,
            "reason": f"예외 발생: {str(e)}"
        }

def refine_label(text: str, original_label: str, original_confidence: float) -> Dict[str, Any]:
    """낮은 신뢰도의 라벨을 OpenAI로 재라벨링"""
    print(f"OpenAI 재라벨링: '{text[:50]}...' (원래: {original_label}, 신뢰도: {original_confidence:.3f})")
    
    try:
        result = call_openai(text)
        
        # OpenAI 결과의 신뢰도가 더 높으면 사용
        if result["confidence"] > original_confidence:
            print(f"  → OpenAI 결과 채택: {result['label']} (신뢰도: {result['confidence']:.3f})")
            return result
        else:
            print(f"  → 원래 라벨 유지: {original_label} (신뢰도: {original_confidence:.3f})")
            return {
                "label": original_label,
                "confidence": original_confidence,
                "reason": "OpenAI 재라벨링 완료, 원래 라벨이 더 높은 신뢰도"
            }
            
    except Exception as e:
        print(f"  → OpenAI 재라벨링 실패: {e}")
        return {
            "label": original_label,
            "confidence": original_confidence,
            "reason": f"OpenAI 재라벨링 실패: {str(e)}"
        }
