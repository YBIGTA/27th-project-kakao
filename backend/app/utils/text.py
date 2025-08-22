import re

def normalize_brand(s: str) -> str:
    s = s.lower()
    s = re.sub(r'[^a-z0-9가-힣]+', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()

def normalize_title_for_similarity(t: str) -> str:
    t = t.lower()
    t = re.sub(r'\b(\d+ml|\d+g|\d+kg|\d+개|세트|set|size\s*\d+)\b', ' ', t)
    t = re.sub(r'[^a-z0-9가-힣 ]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t

def jaccard_sim(a: str, b: str) -> float:
    A = set(a.split()); B = set(b.split())
    if not A or not B: return 0.0
    return len(A & B) / len(A | B)
