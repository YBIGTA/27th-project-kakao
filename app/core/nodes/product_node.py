#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Dict, List, Any
import logging
import json
from app.core.state import PipelineState
from app.services.llm.client import LLMClient

logger = logging.getLogger(__name__)

def _fmt_parent_scores(state: PipelineState) -> str:
    """상위 카테고리 점수 정보 포맷팅"""
    try:
        scores = state.get("parent_scores_prob", {})
        if not scores:
            return "상위 카테고리 점수 없음"
        
        summary = []
        for name, score in scores.items():
            summary.append({
                "name": name,
                "score": f"{score:.4f}"
            })
        return json.dumps(summary, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error formatting parent scores: {e}")
        return "상위 카테고리 점수 포맷팅 오류"

def _fmt_parent_evidence(state: PipelineState) -> str:
    """상위 카테고리 증거 정보 포맷팅"""
    try:
        reasoning = state.get("parent_reasoning", {})
        
        if not reasoning:
            return "상위 카테고리 증거 없음"
        
        summary = []
        for name, reason in reasoning.items():
            summary.append({
                "name": name,
                "reasoning": reason
            })
        return json.dumps(summary, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error formatting parent evidence: {e}")
        return "상위 카테고리 증거 포맷팅 오류"

def _fmt_child_scores(state: PipelineState) -> str:
    """하위 카테고리 점수 정보 포맷팅"""
    try:
        scores = state.get("final_child_scores", {})
        if not scores:
            return "하위 카테고리 점수 없음"
        
        summary = []
        for name, score in scores.items():
            summary.append({
                "name": name,
                "score": f"{score:.4f}"
            })
        return json.dumps(summary, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error formatting child scores: {e}")
        return "하위 카테고리 점수 포맷팅 오류"

def _fmt_child_evidence(state: PipelineState) -> str:
    """하위 카테고리 증거 정보 포맷팅"""
    try:
        top3_children = state.get("top3_children", [])
        top3_reasoning = state.get("top3_children_reasoning", {})
        
        if not top3_children:
            return "하위 카테고리 증거 없음"
        
        summary = []
        for name in top3_children:
            summary.append({
                "name": name,
                "reasoning": top3_reasoning.get(name, "이유 없음")
            })
        return json.dumps(summary, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error formatting child evidence: {e}")
        return "하위 카테고리 증거 포맷팅 오류"

def _fmt_candidates(state: PipelineState) -> str:
    """후보 상품들을 포맷팅"""
    try:
        cands = state.get("candidate_products", [])
        if not cands:
            return "후보 상품 없음"
        
        # 상품 정보를 간단하게 요약 (CSV 필드와 매핑)
        summary = []
        for i, cand in enumerate(cands[:100]):  # 최대 100개만
            # URL 필드 매핑 강화
            url = cand.get("product_url", "") or cand.get("url", "") or cand.get("link", "") or cand.get("gift_link", "")
            
            summary.append({
                "index": i,
                "name": cand.get("product_name", cand.get("name", "이름 없음")),
                "category": cand.get("sub_category", cand.get("child_category", "카테고리 없음")),
                "price": cand.get("price", 0),
                "brand": cand.get("brand", "브랜드 없음"),
                "url": url,
                "top_category": cand.get("top_category", cand.get("parent_category", ""))
            })
        return json.dumps(summary, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error formatting candidates: {e}")
        return "후보 상품 포맷팅 오류"

def _ensure_category_coverage(selections: List[Dict], top3_categories: List[str], candidates: List[Dict], budget_min: int, budget_max: int) -> List[Dict]:
    """Top-3 카테고리 각각에서 최소 1개씩 상품이 포함되도록 보장"""
    logger.info(f"Ensuring coverage for top3 categories: {top3_categories}")
    
    # 현재 선택된 상품들의 카테고리 확인
    covered_categories = set()
    for selection in selections:
        child_cat = selection.get("child_category", "")
        if child_cat:
            covered_categories.add(child_cat)
    
    # 누락된 카테고리 찾기
    missing_categories = [cat for cat in top3_categories if cat not in covered_categories]
    logger.info(f"Missing categories for coverage: {missing_categories}")
    
    # 누락된 카테고리에서 상품 추가
    for missing_cat in missing_categories:
        # 해당 카테고리의 예산 범위 내 상품들 찾기
        pool = [c for c in candidates if c.get("child_category") == missing_cat and 
               budget_min <= c.get("price", 0) <= budget_max]
        
        if pool:
            # 가격 순으로 정렬하여 적절한 상품 선택
            pool.sort(key=lambda x: x.get("price", 0))
            selected_candidate = pool[0]
            
            # 추천 상품 형태로 변환
            reco = {
                "product_name": selected_candidate.get("product_name", ""),
                "price": selected_candidate.get("price", 0),
                "product_url": selected_candidate.get("product_url", ""),
                "parent_category": selected_candidate.get("parent_category", ""),
                "child_category": selected_candidate.get("child_category", ""),
                "id": selected_candidate.get("id", ""),
                "rationale": f"카테고리 '{missing_cat}' 커버리지 보장을 위해 자동 선정됨"
            }
            
            selections.append(reco)
            logger.info(f"Added coverage product for category '{missing_cat}': {reco['product_name']}")
    
    return selections

def _validate_and_sync_recommendations(recos: List[Dict], candidates: List[Dict], budget_min: int, budget_max: int) -> List[Dict]:
    """LLM 추천 상품들을 검증하고 후보 상품 데이터와 동기화"""
    logger.info(f"Validating {len(recos)} recommendations against {len(candidates)} candidates")
    
    # 후보 상품을 ID/상품명으로 인덱싱
    cand_by_id = {c.get("id", c.get("product_name", "")): c for c in candidates}
    valid_selections = []
    
    for reco in recos:
        # ID 또는 상품명으로 후보 찾기
        cand_id = reco.get("id", reco.get("product_name", ""))
        candidate = cand_by_id.get(cand_id)
        
        if not candidate:
            logger.warning(f"Candidate not found for recommendation: {cand_id}")
            continue
            
        if not (budget_min <= candidate.get("price", 0) <= budget_max):
            logger.warning(f"Product {cand_id} price {candidate.get('price')} out of budget range [{budget_min}, {budget_max}]")
            continue
            
        # 동기화: 후보 상품의 실제 데이터로 재설정 (LLM 변조 방지)
        synced_reco = {
            "product_name": candidate.get("product_name", ""),
            "price": candidate.get("price", 0),
            "product_url": candidate.get("product_url", "") or reco.get("product_url", ""),
            "parent_category": candidate.get("parent_category", ""),
            "child_category": candidate.get("child_category", ""),
            "id": candidate.get("id", cand_id),
            "rationale": reco.get("rationale", "추천 이유 없음")
        }
        
        valid_selections.append(synced_reco)
    
    logger.info(f"Valid selections after validation: {len(valid_selections)}")
    return valid_selections

def product_node(state: PipelineState) -> PipelineState:
    """최종 상품 선택 노드"""
    try:
        logger.info("Starting final product selection")
        
        # 필수 데이터 검증
        if not state.get("candidate_products"):
            logger.warning("No candidate products available for selection")
            state["selected_products"] = []
            state["rationales"] = {}
            return state
            
        if not state.get("top3_children"):
            logger.warning("No top-3 children categories available")
            state["selected_products"] = []
            state["rationales"] = {}
            return state
        
        # LLM 클라이언트 초기화
        llm = LLMClient()
        profile = state.get("profile", {})
        budget_min = int(profile.get("budget_min", 0))
        budget_max = int(profile.get("budget_max", 10**9))
        
        # 정보 포맷팅
        parent_scores_info = _fmt_parent_scores(state)
        parent_evidence_info = _fmt_parent_evidence(state)
        child_scores_info = _fmt_child_scores(state)
        child_evidence_info = _fmt_child_evidence(state)
        candidate_products_info = _fmt_candidates(state)
        
        logger.info(f"Formatted data for LLM: {len(state.get('candidate_products', []))} candidates, {len(state.get('top3_children', []))} top categories")
        
        # LLM 호출
        resp = llm.select_products(
            profile=profile,
            parent_scores_info=parent_scores_info,
            parent_evidence_info=parent_evidence_info,
            child_scores_info=child_scores_info,
            child_evidence_info=child_evidence_info,
            candidate_products_info=candidate_products_info,
        )
        
        # 응답 검증 및 처리
        recos = resp.get("recommendations", [])
        if not recos:
            logger.warning("No recommendations returned from LLM")
            state["selected_products"] = []
            state["rationales"] = {}
            return state
            
        logger.info(f"LLM returned {len(recos)} recommendations")
        
        # 1단계: LLM 추천 상품 검증 및 동기화
        valid_selections = _validate_and_sync_recommendations(
            recos, 
            state.get("candidate_products", []), 
            budget_min, 
            budget_max
        )
        
        # 2단계: Top-3 카테고리 커버리지 보장
        final_selections = _ensure_category_coverage(
            valid_selections,
            state.get("top3_children", []),
            state.get("candidate_products", []),
            budget_min,
            budget_max
        )
        
        # 3단계: 최종 5개로 제한 (중복 제거)
        final_selections = final_selections[:5]
        
        # 최종 상품 선택 (5개)
        state["selected_products"] = final_selections
        
        # 추천 이유 및 URL 맵핑
        rationales: Dict[str, Dict[str, str]] = {}
        for rec in final_selections:
            product_name = rec.get("product_name", "")
            rationale = rec.get("rationale", "")
            product_url = rec.get("product_url", "")
            
            if product_name:
                # 상품명과 URL을 함께 저장
                rationales[product_name] = {
                    "rationale": rationale,
                    "product_url": product_url
                }
                
        state["rationales"] = rationales
        
        logger.info(f"Product selection completed: {len(final_selections)} products selected")
        return state
        
    except Exception as e:
        logger.error(f"Error in product selection: {e}")
        # 에러 시 빈 결과 반환
        state["selected_products"] = []
        state["rationales"] = {}
        return state
