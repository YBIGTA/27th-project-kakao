from typing import Dict, Any, List
from .preprocess_adapter import preprocess_any
from .gpu_client import get_gpu_client
from .algorithm import decide_categories_and_evidence
from .search import metadata_search_grouped
from . import llm

class PipelineEngine:
    def __init__(self):
        self.gpu = get_gpu_client()

    async def run(
        self,
        file_bytes: bytes,
        filename: str,
        age: int,
        gender: str,
        relation: str,
        budget_min: int,
        budget_max: int,
    ) -> Dict[str, Any]:
        # 1) 전처리: CSV/TXT 자동 분기 (preprocess import)
        sentences: List[str] = preprocess_any(file_bytes, filename)
        if not sentences:
            raise ValueError("유효한 문장을 찾지 못했습니다.")

        # 2) GPU: 문장만 전달 → 문장별 원천 스코어(per_sentence) 수신
        gpu_out = await self.gpu.infer(sentences)
        if not gpu_out:
            raise ValueError("GPU 결과 없음")

        # 3) Gate: 1~3개 sub_category + 카테고리별 근거문장 3개
        gate = decide_categories_and_evidence(gpu_out)
        subcats = gate.get("subcats", [])
        evidence_by_cat = gate.get("evidence_by_cat", {})
        if not subcats:
            raise ValueError("카테고리 산출 실패")

        # 4) SQL 메타 필터(카테고리/예산) → 카테고리별 후보 목록
        grouped = await metadata_search_grouped(
            sub_categories=subcats,
            budget_min=budget_min,
            budget_max=budget_max,
        )
        if not grouped or not any(grouped.get(c) for c in subcats):
            raise ValueError("상품 후보 없음")

        # 5) LLM: 각 카테고리에서 정확히 1개 선택
        profile = {
            "age": age,
            "gender": gender,
            "relation": relation,
            "budget_min": budget_min,
            "budget_max": budget_max,
        }
        analysis = {"subcats": subcats, "evidence_by_cat": evidence_by_cat}

        try:
            selections = llm.choose_one_per_category(profile, analysis, grouped)
        except Exception:
            # 폴백: 각 카테고리 첫 번째
            selections = []
            for cat in subcats:
                items = grouped.get(cat, [])
                if items:
                    # asyncpg.Record를 dict로 변환하는 올바른 방법
                    d = dict(items[0])
                    selections.append({
                        "sub_category": cat,
                        "product_name": d.get("product_name"),
                        "brand": d.get("brand"),
                        "price": d.get("price"),
                        "product_url": d.get("product_url"),
                        "reason": "기본 폴백"
                    })

        return {"analysis": analysis, "selections": selections}
