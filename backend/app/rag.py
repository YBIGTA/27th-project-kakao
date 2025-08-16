from typing import Dict, Any, List, Optional
import os
from . import preprocess      # TODO
from . import embedder        # TODO
from . import sentiment       # TODO
from . import algorithm       # TODO
from .search import vector_search
from . import llm             # TODO

RAG_TOP_M = int(os.getenv("RAG_TOP_M", "200"))
RETURN_TOP_K = int(os.getenv("RETURN_TOP_K", "8"))

class RAGEngine:
    def __init__(self, rag_top_m: Optional[int] = None, return_top_k: Optional[int] = None):
        self.rag_top_m = rag_top_m or RAG_TOP_M
        self.return_top_k = return_top_k or RETURN_TOP_K

    @staticmethod
    def _profile(age: int, relation: str, budget_min: int, budget_max: int) -> Dict[str, Any]:
        return {"age": age, "relation": relation, "budget_min": budget_min, "budget_max": budget_max}

    async def run(self, *, raw_txt: str, age: int, relation: str,
                  budget_min: int, budget_max: int) -> Dict[str, Any]:
        # 1) 전처리
        sentences = preprocess.preprocess_txt(raw_txt)
        if not sentences: raise ValueError("유효한 문장을 찾지 못했습니다.")

        # 2) 감정/임베딩
        try: sentiments_ = sentiment.analyze_sentences(sentences)
        except Exception: sentiments_ = None
        sent_vecs = embedder.embed_sentences(sentences)
        if not sent_vecs: raise ValueError("임베딩 실패")

        # 3) Gate: Top3 + evidence3
        gate_out = algorithm.gate(sentences, sentiments_)
        if not gate_out or not gate_out.get("evidence"):
            raise ValueError("Gate 산출 없음")
        top3 = gate_out.get("top3_subcats", [])[:3]
        evid_texts = gate_out["evidence"][:3]

        # evidence → qvec (임시: 마지막 N개 벡터 평균)
        evid_vecs = sent_vecs[-len(evid_texts):] if len(sent_vecs) >= len(evid_texts) else sent_vecs
        qvec = embedder.average_embedding(evid_vecs)

        # 4) Retrieval: 예산/카테고리 + pgvector TopM
        primary = top3[0] if top3 else None
        rows = await vector_search(qvec, primary, budget_min, budget_max, self.rag_top_m) or \
               await vector_search(qvec, None,    budget_min, budget_max, self.rag_top_m)
        if not rows: raise ValueError("RAG 후보 없음")

        candidates = [{
            "url_hash": r.get("url_hash"),
            "product_name": r.get("product_name"),
            "brand": r.get("brand"),
            "sub_category": r.get("sub_category"),
            "price": int(r.get("price")) if r.get("price") is not None else None,
            "image_url": r.get("image_url"),
            "product_url": r.get("product_url"),
            "sim": float(r.get("sim")) if r.get("sim") is not None else None,
        } for r in rows]

        profile = self._profile(age, relation, budget_min, budget_max)
        analysis = {"top3_subcats": top3, "evidence": evid_texts}

        # 5) Generation(LLM 재랭킹) — 후보 밖 금지
        try:
            final_items = llm.rerank_and_reason(profile, analysis, candidates)
        except Exception:
            final_items = [{
                "product_name": c["product_name"],
                "product_url": c.get("product_url"),
                "image_url": c.get("image_url"),
                "price": c.get("price"),
                "brand": c.get("brand"),
                "reason": "LLM 실패로 유사도 상위 결과 제공",
            } for c in candidates[: self.return_top_k]]

        return {"profile": profile, "analysis": analysis, "results": final_items}