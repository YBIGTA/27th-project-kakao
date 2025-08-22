import asyncio
import time
from typing import List, Dict, Any
from core.config import MAX_CONCURRENCY
import json

# 배치 처리 설정
BATCH_SIZE = 10  # 한 번에 처리할 문장 수
BATCH_DELAY = 2.0  # 배치 간 대기 시간 (초)
REQUEST_DELAY = 0.5  # 개별 요청 간 대기 시간 (초)

async def score_parents_for_sentences(sentences: List[Dict[str, Any]], parent_labels: List[str]) -> List[Dict[str, Any]]:
    print(f"🔍 score_parents_for_sentences 시작: {len(sentences)}개 문장")
    
    from services.llm.client import LLMClient
    client = LLMClient()
    
    # 🚀 최적화: 프롬프트 템플릿을 한 번만 생성
    from services.llm.prompts import format_parent_batch_template
    base_template = format_parent_batch_template(parent_labels)
    print(f"✅ 프롬프트 템플릿 생성 완료 (1회만)")
    
    results = []
    
    async def process_batch(batch_sentences: List[Dict[str, Any]], batch_start_idx: int) -> List[Dict[str, Any]]:
        """배치 단위로 문장들을 처리합니다."""
        print(f"🔄 배치 처리 중: {batch_start_idx}~{batch_start_idx + len(batch_sentences) - 1} ({len(batch_sentences)}개)")
        
        try:
            # 🚀 최적화: 문장 데이터만 교체하여 프롬프트 생성
            sentences_json = json.dumps(batch_sentences, ensure_ascii=False)
            prompt = base_template.replace("{{sentences}}", sentences_json)
            
            # 배치 방식으로 한 번에 처리
            batch_result = await client.score_parent_batch_with_prompt(prompt)
            print(f"✅ 배치 처리 완료: {len(batch_sentences)}개 문장")
            
            # 배치 결과를 개별 문장 결과로 변환
            if batch_result.get("unit") == "batch" and "results" in batch_result:
                batch_results = []
                for result in batch_result["results"]:
                    sentence_idx = result.get("sentence_idx", 0)
                    # 기존 형식과 호환되도록 변환
                    converted_result = {
                        "categories": result.get("categories", []),
                        "evidence_idx": []
                    }
                    # evidence_idx 수집
                    for cat in converted_result["categories"]:
                        converted_result["evidence_idx"].extend(cat.get("evidence_idx", []))
                    batch_results.append(converted_result)
                return batch_results
            else:
                # 기존 방식으로 fallback
                print(f"⚠️ 배치 결과 형식이 예상과 다름, 개별 처리로 fallback")
                return await _process_individual_sentences(batch_sentences, parent_labels, client)
                
        except Exception as e:
            print(f"❌ 배치 처리 실패, 개별 처리로 fallback: {e}")
            return await _process_individual_sentences(batch_sentences, parent_labels, client)

    # 배치 단위로 처리
    for i in range(0, len(sentences), BATCH_SIZE):
        batch = sentences[i:i + BATCH_SIZE]
        batch_results = await process_batch(batch, i)
        results.extend(batch_results)
        
        # 마지막 배치가 아니면 대기
        if i + BATCH_SIZE < len(sentences):
            print(f"⏳ 배치 간 대기 중... ({BATCH_DELAY}초)")
            await asyncio.sleep(BATCH_DELAY)

    return results

async def score_children_for_sentences(sentences: List[Dict[str, Any]], child_labels: List[str]) -> List[Dict[str, Any]]:
    print(f"🔍 score_children_for_sentences 시작: {len(sentences)}개 문장")
    
    from services.llm.client import LLMClient
    client = LLMClient()
    
    # 🚀 최적화: 프롬프트 템플릿을 한 번만 생성
    from services.llm.prompts import format_child_batch_template
    base_template = format_child_batch_template(child_labels)
    print(f"✅ 프롬프트 템플릿 생성 완료 (1회만)")
    
    results = []

    async def process_batch(batch_sentences: List[Dict[str, Any]], batch_start_idx: int) -> List[Dict[str, Any]]:
        """배치 단위로 문장들을 처리합니다."""
        print(f"🔄 배치 처리 중: {batch_start_idx}~{batch_start_idx + len(batch_sentences) - 1} ({len(batch_sentences)}개)")
        
        try:
            # 🚀 최적화: 문장 데이터만 교체하여 프롬프트 생성
            sentences_json = json.dumps(batch_sentences, ensure_ascii=False)
            prompt = base_template.replace("{{sentences}}", sentences_json)
            
            # 배치 방식으로 한 번에 처리
            batch_result = await client.score_child_batch_with_prompt(prompt)
            print(f"✅ 배치 처리 완료: {len(batch_sentences)}개 문장")
            
            # 배치 결과를 개별 문장 결과로 변환
            if batch_result.get("unit") == "batch" and "results" in batch_result:
                batch_results = []
                for result in batch_result["results"]:
                    sentence_idx = result.get("sentence_idx", 0)
                    # 기존 형식과 호환되도록 변환
                    converted_result = {
                        "subcategories": result.get("subcategories", []),
                        "evidence_idx": []
                    }
                    # evidence_idx 수집
                    for subcat in converted_result["subcategories"]:
                        converted_result["evidence_idx"].extend(subcat.get("evidence_idx", []))
                    batch_results.append(converted_result)
                return batch_results
            else:
                # 기존 방식으로 fallback
                print(f"⚠️ 배치 결과 형식이 예상과 다름, 개별 처리로 fallback")
                return await _process_individual_child_sentences(batch_sentences, child_labels, client)
                
        except Exception as e:
            print(f"❌ 배치 처리 실패, 개별 처리로 fallback: {e}")
            return await _process_individual_child_sentences(batch_sentences, child_labels, client)

    # 배치 단위로 처리
    for i in range(0, len(sentences), BATCH_SIZE):
        batch = sentences[i:i + BATCH_SIZE]
        batch_results = await process_batch(batch, i)
        results.extend(batch_results)
        
        # 마지막 배치가 아니면 대기
        if i + BATCH_SIZE < len(sentences):
            print(f"⏳ 배치 간 대기 중... ({BATCH_DELAY}초)")
            await asyncio.sleep(BATCH_DELAY)

    return results

async def _process_individual_sentences(sentences: List[Dict[str, Any]], parent_labels: List[str], client) -> List[Dict[str, Any]]:
    """개별 문장 처리 (fallback용)"""
    print(f"🔄 개별 문장 처리로 fallback: {len(sentences)}개")
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    async def one(i, s):
        async with sem:
            try:
                print(f"🔍 문장 {i} 처리 중: {s}")
                # 문장의 실제 idx 사용
                actual_idx = s.get("idx", i)
                result = await client.score_parent_sentence(s["text"], parent_labels, e_idx=actual_idx, date=s.get("date"))
                # 개별 요청 간 대기
                await asyncio.sleep(REQUEST_DELAY)
                return result
            except Exception as e:
                print(f"❌ 문장 {i} 처리 실패: {e}")
                print(f"   문장 데이터: {s}")
                raise

    tasks = [asyncio.create_task(one(j, s)) for j, s in enumerate(sentences)]
    return await asyncio.gather(*tasks)

async def _process_individual_child_sentences(sentences: List[Dict[str, Any]], child_labels: List[str], client) -> List[Dict[str, Any]]:
    """개별 문장 처리 (fallback용)"""
    print(f"🔄 개별 문장 처리로 fallback: {len(sentences)}개")
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    async def one(i, s):
        async with sem:
            try:
                # 문장의 실제 idx 사용
                actual_idx = s.get("idx", i)
                result = await client.score_child_sentence(s["text"], child_labels, e_idx=actual_idx, date=s.get("date"))
                # 개별 요청 간 대기
                await asyncio.sleep(REQUEST_DELAY)
                return result
            except Exception as e:
                print(f"❌ 문장 {i} 처리 실패: {e}")
                print(f"   문장 데이터: {s}")
                raise

    tasks = [asyncio.create_task(one(j, s)) for j, s in enumerate(sentences)]
    return await asyncio.gather(*tasks)
