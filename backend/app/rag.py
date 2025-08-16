#mock 
from typing import Dict, Any, List, Optional
import os
from . import preprocess      # TODO
from . import embedder        # TODO
from . import sentiment       # TODO
from . import algorithm       # TODO
from .search import vector_search 
from . import llm             # TODO

RAG_TOP_M = int(os.getenv("RAG_TOP_M", "30"))
RAG_TOP_K = int(os.getenv("RAG_TOP_K", "3"))

class RAGEngine:
    def __init__(self, top_m: Optional[int] = None, top_k: Optional[int] = None):
        self.rag_top_m = int(top_m or RAG_TOP_M)
        self.top_k = int(top_k or RAG_TOP_K)
    async def run(
        self,
        raw_txt: str,
        age: int,
        relation: str,
        budget_min: int,
        budget_max: int,
    ) -> Dict[str, Any]:
        # 1) 전처리 — 구현은 preprocess.py 안에 
        sentences: List[str] = preprocess.preprocess_txt(raw_txt)
        if not sentences:
            raise ValueError("유효한 문장을 찾지 못했습니다.")

        # 2) 감정 — sentiment.py에 구현, 실패 시 None 허용
        try:
            sentiments = sentiment.analyze_sentences(sentences)
        except Exception:
            sentiments = None

        # 3) 임베딩 —embedder.py에 구현
        sent_vecs = embedder.embed_sentences(sentences)
        if not sent_vecs:
            raise ValueError("임베딩 실패")

        # 4) Gate — algorithm.py에 구현
        #    Gate는 "Top3 하위카테고리 + evidence 문장 3개"만 책임짐
        gate_out = algorithm.gate(sentences, sentiments)
        if not gate_out or not gate_out.get("evidence"):
            raise ValueError("Gate 산출 없음")
        top3 = gate_out.get("top3_subcats", [])[:3]
        evid_texts = [e["text"] for e in gate_out["evidence"][:3]]

        # 5) evidence → 쿼리벡터 (평균) — 계산식은 embedder.py에 
        evid_vecs = embedder.pick_vectors_for_evidence(sentences, sent_vecs, evid_texts)
        qvec = embedder.average_embedding(evid_vecs)

        # 6) Retrieval — (pgvector) search.py에 구
        primary = top3[0] if top3 else None
        rows = await vector_search(qvec, primary, budget_min, budget_max, self.rag_top_m)
        if not rows:
            rows = await vector_search(qvec, None, budget_min, budget_max, self.rag_top_m)
        if not rows:
            raise ValueError("RAG 후보 없음")

        candidates = []
        for r in rows:
            d = dict(r)
            candidates.append({
                "url_hash": d.get("url_hash"),
                "product_name": d.get("product_name"),
                "brand": d.get("brand"),
                "sub_category": d.get("sub_category"),
                "price": d.get("price"),
                "satisfaction_pct": d.get("satisfaction_pct"),  
                "review_count": d.get("review_count"),          
                "wish_count": d.get("wish_count"),              
                "product_url": d.get("product_url"),
                "sim": d.get("sim"),
            })
        # 7) LLM 재랭킹 — llm.py에 구현
        profile = {"age": age, "relation": relation, "budget_min": budget_min, "budget_max": budget_max}
        analysis = {"top3_subcats": top3, "evidence": evid_texts}
        try:
            final_items = llm.rerank_and_reason(profile, analysis, candidates, top_k=self.top_k)
        except Exception:
            final_items = candidates[:self.top_k]  # 폴백

        return {
            "profile": profile,
            "analysis": analysis,
            "results": final_items
        }