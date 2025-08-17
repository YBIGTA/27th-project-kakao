import os, json, re
from typing import List, Dict, Any

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "upstage")
LLM_MODEL = os.getenv("LLM_MODEL", "solar-1-mini-chat")

def _norm(s: str) -> str:
    """상품명 정규화 (대소문자/공백 차이 허용)"""
    return (s or "").strip().casefold()

def _load_json_strict(s: str) -> dict:
    """JSON 파싱 강화 (정규식으로 JSON 추출)"""
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", s, re.S)
        if m:
            return json.loads(m.group(0))
        raise

def _format(profile: Dict[str, Any], analysis: Dict[str, Any], grouped: Dict[str, List[Dict[str, Any]]]):
    sys = (
        "You are a gift recommender. For EACH category, choose EXACTLY ONE item "
        "ONLY from the provided list for that category. Respect budget/age/relation. "
        "Return STRICT JSON: {\"selections\":[{\"sub_category\":\"...\",\"product_name\":\"...\",\"reason\":\"...\"}, ...]}"
    )
    compact = {
        cat: [
            {k: v for k, v in it.items()
             if k in ("product_name","brand","price","product_url")}
            for it in items
        ] for cat, items in grouped.items()
    }
    user = {"profile": profile, "analysis": analysis, "candidates_by_category": compact}
    return sys, json.dumps(user, ensure_ascii=False)

def _fallback_first(grouped: Dict[str, List[Dict[str, Any]]]):
    out = []
    for cat, items in grouped.items():
        if not items: continue
        d = dict(items[0])
        out.append({
            "sub_category": cat,
            "product_name": d.get("product_name"),
            "brand": d.get("brand"),
            "price": d.get("price"),
            "product_url": d.get("product_url"),
            "reason": "기본 폴백"
        })
    return out

def choose_one_per_category(profile: Dict[str, Any], analysis: Dict[str, Any], grouped: Dict[str, List[Dict[str, Any]]]):
    try:
        if LLM_PROVIDER == "openai":
            from openai import OpenAI
            client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            system, user = _format(profile, analysis, grouped)
            resp = client.chat.completions.create(
                model=LLM_MODEL,
                messages=[{"role":"system","content":system},{"role":"user","content":user}],
                temperature=0.2,
                response_format={"type":"json_object"},
            )
            
            # JSON 파싱 및 검증 강화
            try:
                content = resp.choices[0].message.content
                data = json.loads(content)
            except (json.JSONDecodeError, AttributeError, IndexError) as e:
                print(f"LLM 응답 JSON 파싱 실패: {e}")
                return _fallback_first(grouped)
            
            sels = data.get("selections", [])
            if not isinstance(sels, list):
                print("LLM 응답에서 selections가 리스트가 아님")
                return _fallback_first(grouped)

            # 후보 밖 금지 + 카테고리당 1개 보장 (정규화 매칭)
            name_sets = {c: {_norm(it.get("product_name","")) for it in items} for c, items in grouped.items()}
            safe = []
            seen = set()
            for s in sels:
                if not isinstance(s, dict):
                    continue
                    
                cat, name = s.get("sub_category"), s.get("product_name")
                if not cat or not name or cat in seen: continue
                norm = _norm(name)
                if norm in name_sets.get(cat, set()):
                    item = next((it for it in grouped[cat] if _norm(it.get("product_name","")) == norm), {})
                    safe.append({
                        "sub_category": cat,
                        "product_name": item.get("product_name") or name,  # 원본 상품명 사용
                        "brand": item.get("brand"),
                        "price": item.get("price"),
                        "product_url": item.get("product_url"),
                        "reason": s.get("reason") or "대화 근거와 예산을 바탕으로 선정"
                    })
                    seen.add(cat)
            return safe if safe else _fallback_first(grouped)
            
        elif LLM_PROVIDER == "upstage":
            import requests
            import time
            
            api_key = os.getenv("UPSTAGE_API_KEY")
            if not api_key:
                print("UPSTAGE_API_KEY 미설정")
                return _fallback_first(grouped)
                
            system, user = _format(profile, analysis, grouped)
            headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
            payload = {
                "model": LLM_MODEL,
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
                "temperature": 0.2,
                "max_tokens": 800,
                "response_format": {"type": "json_object"},
            }
            
            # 재시도 로직 추가
            for attempt in range(3):
                try:
                    resp = requests.post("https://api.upstage.ai/v1/chat/completions", 
                                       headers=headers, json=payload, timeout=60)
                    
                    if resp.status_code == 200:
                        data = resp.json()
                        content = data["choices"][0]["message"]["content"]
                        
                        # 공통 JSON 파서 사용
                        parsed_data = _load_json_strict(content)
                        
                        sels = parsed_data.get("selections", [])
                        if not isinstance(sels, list):
                            print("업스테이지 응답에서 selections가 리스트가 아님")
                            return _fallback_first(grouped)

                        # 후보 밖 금지 + 카테고리당 1개 보장 (정규화 매칭)
                        name_sets = {c: {_norm(it.get("product_name","")) for it in items} for c, items in grouped.items()}
                        safe = []
                        seen = set()
                        for s in sels:
                            if not isinstance(s, dict):
                                continue
                                
                            cat, name = s.get("sub_category"), s.get("product_name")
                            if not cat or not name or cat in seen: continue
                            norm = _norm(name)
                            if norm in name_sets.get(cat, set()):
                                item = next((it for it in grouped[cat] if _norm(it.get("product_name","")) == norm), {})
                                safe.append({
                                    "sub_category": cat,
                                    "product_name": item.get("product_name") or name,  # 원본 상품명 사용
                                    "brand": item.get("brand"),
                                    "price": item.get("price"),
                                    "product_url": item.get("product_url"),
                                    "reason": s.get("reason") or "대화 근거와 예산을 바탕으로 선정"
                                })
                                seen.add(cat)
                        return safe if safe else _fallback_first(grouped)
                    
                    # 재시도 가능한 오류들
                    if resp.status_code in (429, 500, 502, 503, 504):
                        if attempt < 2:  # 마지막 시도가 아니면
                            time.sleep(1.5 ** attempt)  # 지수 백오프
                            continue
                    
                    print(f"업스테이지 API 오류: {resp.status_code} - {resp.text[:400]}")
                    return _fallback_first(grouped)
                    
                except requests.Timeout:
                    if attempt < 2:
                        time.sleep(1.5 ** attempt)
                        continue
                    print("업스테이지 API 타임아웃")
                    return _fallback_first(grouped)
                except requests.RequestException as e:
                    print(f"업스테이지 요청 예외: {e}")
                    return _fallback_first(grouped)
            
            return _fallback_first(grouped)
            
        else:
            raise RuntimeError(f"LLM provider '{LLM_PROVIDER}' not configured")
    except Exception as e:
        print(f"LLM 처리 중 오류 발생: {e}")
        return _fallback_first(grouped)
