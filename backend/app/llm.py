import os, json
from typing import List, Dict, Any

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")

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
            data = json.loads(resp.choices[0].message.content)
            sels = data.get("selections", [])

            # 후보 밖 금지 + 카테고리당 1개 보장
            name_sets = {c: {it["product_name"] for it in items} for c, items in grouped.items()}
            safe = []
            seen = set()
            for s in sels:
                cat, name = s.get("sub_category"), s.get("product_name")
                if not cat or not name or cat in seen: continue
                if name in name_sets.get(cat, set()):
                    item = next((it for it in grouped[cat] if it["product_name"] == name), {})
                    safe.append({
                        "sub_category": cat,
                        "product_name": name,
                        "brand": item.get("brand"),
                        "price": item.get("price"),
                        "product_url": item.get("product_url"),
                        "reason": s.get("reason") or "대화 근거와 예산을 바탕으로 선정"
                    })
                    seen.add(cat)
            return safe if safe else _fallback_first(grouped)
        else:
            raise RuntimeError("LLM provider not configured")
    except Exception:
        return _fallback_first(grouped)
