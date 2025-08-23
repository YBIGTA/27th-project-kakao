"""
LLM Scorer 함수들
문장별로 상위/하위 카테고리 점수를 계산하는 함수들
"""

import asyncio
import json
from typing import List, Dict, Any
from .client import llm_client
from .prompts import format_parent_prompt, format_child_prompt
from ...config.settings import INDIVIDUAL_PARALLEL_THRESHOLD

async def score_parents_for_sentences(
    sentences: List[Dict[str, Any]], 
    parent_labels: List[str]
) -> List[Dict[str, Any]]:
    """
    문장별로 상위 카테고리 점수를 계산합니다 (배치 + 병렬 처리).
    
    Args:
        sentences: 문장 리스트 [{"idx": int, "date": str, "text": str}, ...]
        parent_labels: 상위 카테고리 라벨 리스트
        
    Returns:
        List[Dict]: 각 문장별 상위 카테고리 점수 결과
    """
    if not sentences:
        return []
    
    print(f"🚀 상위 카테고리 하이브리드 처리 시작: {len(sentences)}개 문장")
    
    # 문장 수에 따른 처리 방식 선택
    if len(sentences) <= INDIVIDUAL_PARALLEL_THRESHOLD:
        # 임계값 이하면 개별 병렬 처리 (빠름)
        print(f"📝 개별 병렬 처리 모드 (임계값: {INDIVIDUAL_PARALLEL_THRESHOLD})")
        return await _score_parents_individual_parallel(sentences, parent_labels)
    else:
        # 임계값 초과하면 배치 + 병렬 처리 (효율적)
        print(f"📦 배치 + 병렬 처리 모드 (임계값: {INDIVIDUAL_PARALLEL_THRESHOLD})")
        return await _score_parents_batch_parallel(sentences, parent_labels)

async def _score_parents_individual_parallel(
    sentences: List[Dict[str, Any]], 
    parent_labels: List[str]
) -> List[Dict[str, Any]]:
    """개별 문장 병렬 처리"""
    
    async def process_sentence(sentence: Dict[str, Any]) -> Dict[str, Any]:
        try:
            # 단일 문장에 대한 프롬프트 생성
            prompt = format_parent_prompt([sentence], parent_labels)
            
            # LLM 호출 (라운드 로빈으로 API 키 선택)
            response = await llm_client._call_upstage_api(prompt, llm_client._get_next_api_key())
            
            # 응답 파싱
            content = response["choices"][0]["message"]["content"]
            parsed_result = _parse_parent_response(content, sentence["idx"])
            
            return parsed_result
            
        except Exception as e:
            print(f"문장 {sentence['idx']} 상위 카테고리 점수 계산 실패: {e}")
            return {
                "sentence_idx": sentence["idx"],
                "error": str(e),
                "categories": []
            }
    
    # 모든 문장을 병렬로 처리
    tasks = [process_sentence(sentence) for sentence in sentences]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 예외 처리
    final_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"문장 {i} 처리 실패: {result}")
            final_results.append({
                "sentence_idx": i,
                "error": str(result),
                "categories": []
            })
        else:
            final_results.append(result)
    
    return final_results

async def _score_parents_batch_parallel(
    sentences: List[Dict[str, Any]], 
    parent_labels: List[str]
) -> List[Dict[str, Any]]:
    """배치 + 병렬 처리"""
    
    def split_sentences_into_batches(sentences, batch_size=5):
        """문장들을 배치로 나누기"""
        batches = []
        for i in range(0, len(sentences), batch_size):
            batch = sentences[i:i + batch_size]
            batches.append(batch)
        return batches
    
    async def process_batch(batch: List[Dict[str, Any]], batch_idx: int) -> List[Dict[str, Any]]:
        """단일 배치 처리"""
        try:
            print(f"📦 배치 {batch_idx + 1} 처리 중: {len(batch)}개 문장")
            
            # 배치 프롬프트 생성
            from .prompts import format_parent_batch_prompt
            prompt = format_parent_batch_prompt(batch, parent_labels)
            
            # 배치 처리 (라운드 로빈으로 API 키 선택)
            batch_result = await llm_client.score_parent_batch_with_prompt(prompt)
            
            # 배치 결과를 개별 문장 결과로 변환
            batch_results = []
            for sentence in batch:
                sentence_idx = sentence["idx"]
                
                # 배치 결과에서 해당 문장 찾기
                sentence_result = None
                if "results" in batch_result:
                    for result in batch_result["results"]:
                        if result.get("sentence_idx") == sentence_idx:
                            sentence_result = result
                            break
                
                if sentence_result:
                    batch_results.append({
                        "sentence_idx": sentence_idx,
                        "categories": sentence_result.get("categories", []),
                        "evidence_idx": [sentence_idx],
                        "mini_reason": f"배치 {batch_idx + 1}로 문장 {sentence_idx} 분석 완료"
                    })
                else:
                    # 배치에서 찾지 못한 경우 개별 처리
                    print(f"⚠️ 문장 {sentence_idx}를 배치에서 찾지 못해 개별 처리")
                    individual_result = await llm_client.score_parent_sentence(
                        sentence["text"], parent_labels, sentence_idx, sentence.get("date")
                    )
                    batch_results.append({
                        "sentence_idx": sentence_idx,
                        "categories": individual_result.get("categories", []),
                        "evidence_idx": [sentence_idx],
                        "mini_reason": f"개별 처리로 문장 {sentence_idx} 분석 완료"
                    })
            
            print(f"✅ 배치 {batch_idx + 1} 완료: {len(batch_results)}개 결과")
            return batch_results
            
        except Exception as e:
            print(f"❌ 배치 {batch_idx + 1} 처리 실패: {e}")
            # 배치 실패 시 개별 처리로 폴백
            fallback_results = []
            for sentence in batch:
                try:
                    individual_result = await llm_client.score_parent_sentence(
                        sentence["text"], parent_labels, sentence["idx"], sentence.get("date")
                    )
                    fallback_results.append({
                        "sentence_idx": sentence["idx"],
                        "categories": individual_result.get("categories", []),
                        "evidence_idx": [sentence["idx"]],
                        "mini_reason": f"폴백 처리로 문장 {sentence['idx']} 분석 완료"
                    })
                except Exception as e2:
                    print(f"문장 {sentence['idx']} 폴백 처리도 실패: {e2}")
                    fallback_results.append({
                        "sentence_idx": sentence["idx"],
                        "error": str(e2),
                        "categories": []
                    })
            return fallback_results
    
    # 문장들을 5개씩 배치로 나누기
    batches = split_sentences_into_batches(sentences, batch_size=5)
    print(f"📦 총 {len(batches)}개 배치로 분할")
    
    # 모든 배치를 병렬로 처리
    tasks = [process_batch(batch, i) for i, batch in enumerate(batches)]
    batch_results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 모든 배치 결과를 합치기
    final_results = []
    for i, result in enumerate(batch_results):
        if isinstance(result, Exception):
            print(f"배치 {i} 처리 실패: {result}")
            # 실패한 배치는 빈 결과로 처리
            final_results.extend([{
                "sentence_idx": j,
                "error": str(result),
                "categories": []
            } for j in range(i * 5, min((i + 1) * 5, len(sentences)))])
        else:
            final_results.extend(result)
    
    print(f"✅ 상위 카테고리 하이브리드 처리 완료: {len(final_results)}개 결과")
    return final_results

async def score_children_for_sentences(
    sentences: List[Dict[str, Any]], 
    child_labels: List[str]
) -> List[Dict[str, Any]]:
    """
    문장별로 하위 카테고리 점수를 계산합니다 (배치 + 병렬 처리).
    
    Args:
        sentences: 문장 리스트 [{"idx": int, "date": str, "text": str}, ...]
        child_labels: 하위 카테고리 라벨 리스트 ["상위/하위", ...]
        
    Returns:
        List[Dict]: 각 문장별 하위 카테고리 점수 결과
    """
    if not sentences:
        return []
    
    print(f"🚀 하위 카테고리 하이브리드 처리 시작: {len(sentences)}개 문장")
    
    # 문장 수에 따른 처리 방식 선택
    if len(sentences) <= 5:
        # 5개 이하면 개별 병렬 처리 (빠름)
        print("📝 개별 병렬 처리 모드")
        return await _score_children_individual_parallel(sentences, child_labels)
    else:
        # 5개 초과하면 배치 + 병렬 처리 (효율적)
        print("📦 배치 + 병렬 처리 모드")
        return await _score_children_batch_parallel(sentences, child_labels)

async def _score_children_individual_parallel(
    sentences: List[Dict[str, Any]], 
    child_labels: List[str]
) -> List[Dict[str, Any]]:
    """개별 문장 병렬 처리"""
    
    async def process_sentence(sentence: Dict[str, Any]) -> Dict[str, Any]:
        try:
            # 단일 문장에 대한 프롬프트 생성
            prompt = format_child_prompt([sentence], child_labels)
            
            # LLM 호출 (라운드 로빈으로 API 키 선택)
            response = await llm_client._call_upstage_api(prompt, llm_client._get_next_api_key())
            
            # 응답 파싱
            content = response["choices"][0]["message"]["content"]
            parsed_result = _parse_child_response(content, sentence["idx"])
            
            return parsed_result
            
        except Exception as e:
            print(f"문장 {sentence['idx']} 하위 카테고리 점수 계산 실패: {e}")
            return {
                "sentence_idx": sentence["idx"],
                "error": str(e),
                "subcategories": []
            }
    
    # 모든 문장을 병렬로 처리
    tasks = [process_sentence(sentence) for sentence in sentences]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 예외 처리
    final_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"문장 {i} 처리 실패: {result}")
            final_results.append({
                "sentence_idx": i,
                "error": str(result),
                "subcategories": []
            })
        else:
            final_results.append(result)
    
    return final_results

async def _score_children_batch_parallel(
    sentences: List[Dict[str, Any]], 
    child_labels: List[str]
) -> List[Dict[str, Any]]:
    """배치 + 병렬 처리"""
    
    def split_sentences_into_batches(sentences, batch_size=5):
        """문장들을 배치로 나누기"""
        batches = []
        for i in range(0, len(sentences), batch_size):
            batch = sentences[i:i + batch_size]
            batches.append(batch)
        return batches
    
    async def process_batch(batch: List[Dict[str, Any]], batch_idx: int) -> List[Dict[str, Any]]:
        """단일 배치 처리"""
        try:
            print(f"📦 배치 {batch_idx + 1} 처리 중: {len(batch)}개 문장")
            
            # 배치 프롬프트 생성
            from .prompts import format_child_batch_prompt
            prompt = format_child_batch_prompt(batch, child_labels)
            
            # 배치 처리 (라운드 로빈으로 API 키 선택)
            batch_result = await llm_client.score_child_batch_with_prompt(prompt)
            
            # 배치 결과를 개별 문장 결과로 변환
            batch_results = []
            for sentence in batch:
                sentence_idx = sentence["idx"]
                
                # 배치 결과에서 해당 문장 찾기
                sentence_result = None
                if "results" in batch_result:
                    for result in batch_result["results"]:
                        if result.get("sentence_idx") == sentence_idx:
                            sentence_result = result
                            break
                
                if sentence_result:
                    batch_results.append({
                        "sentence_idx": sentence_idx,
                        "subcategories": sentence_result.get("subcategories", []),
                        "evidence_idx": [sentence_idx],
                        "mini_reason": f"배치 {batch_idx + 1}로 문장 {sentence_idx} 분석 완료"
                    })
                else:
                    # 배치에서 찾지 못한 경우 개별 처리
                    print(f"⚠️ 문장 {sentence_idx}를 배치에서 찾지 못해 개별 처리")
                    individual_result = await llm_client.score_child_sentence(
                        sentence["text"], child_labels, sentence_idx, sentence.get("date")
                    )
                    batch_results.append({
                        "sentence_idx": sentence_idx,
                        "subcategories": individual_result.get("subcategories", []),
                        "evidence_idx": [sentence_idx],
                        "mini_reason": f"개별 처리로 문장 {sentence_idx} 분석 완료"
                    })
            
            print(f"✅ 배치 {batch_idx + 1} 완료: {len(batch_results)}개 결과")
            return batch_results
            
        except Exception as e:
            print(f"❌ 배치 {batch_idx + 1} 처리 실패: {e}")
            # 배치 실패 시 개별 처리로 폴백
            fallback_results = []
            for sentence in batch:
                try:
                    individual_result = await llm_client.score_child_sentence(
                        sentence["text"], child_labels, sentence["idx"], sentence.get("date")
                    )
                    fallback_results.append({
                        "sentence_idx": sentence["idx"],
                        "subcategories": individual_result.get("subcategories", []),
                        "evidence_idx": [sentence["idx"]],
                        "mini_reason": f"폴백 처리로 문장 {sentence['idx']} 분석 완료"
                    })
                except Exception as e2:
                    print(f"문장 {sentence['idx']} 폴백 처리도 실패: {e2}")
                    fallback_results.append({
                        "sentence_idx": sentence["idx"],
                        "error": str(e2),
                        "subcategories": []
                    })
            return fallback_results
    
    # 문장들을 5개씩 배치로 나누기
    batches = split_sentences_into_batches(sentences, batch_size=5)
    print(f"📦 총 {len(batches)}개 배치로 분할")
    
    # 모든 배치를 병렬로 처리
    tasks = [process_batch(batch, i) for i, batch in enumerate(batches)]
    batch_results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 모든 배치 결과를 합치기
    final_results = []
    for i, result in enumerate(batch_results):
        if isinstance(result, Exception):
            print(f"배치 {i} 처리 실패: {result}")
            # 실패한 배치는 빈 결과로 처리
            final_results.extend([{
                "sentence_idx": j,
                "error": str(result),
                "subcategories": []
            } for j in range(i * 5, min((i + 1) * 5, len(sentences)))])
        else:
            final_results.extend(result)
    
    print(f"✅ 하위 카테고리 하이브리드 처리 완료: {len(final_results)}개 결과")
    return final_results

def _parse_parent_response(content: str, sentence_idx: int) -> Dict[str, Any]:
    """상위 카테고리 응답을 파싱합니다."""
    try:
        # JSON 추출
        json_start = content.find('{')
        json_end = content.rfind('}') + 1
        
        if json_start == -1 or json_end == 0:
            raise ValueError("JSON을 찾을 수 없습니다")
        
        json_str = content[json_start:json_end]
        data = json.loads(json_str)
        
        return {
            "sentence_idx": sentence_idx,
            "categories": data.get("categories", []),
            "evidence_idx": [sentence_idx],
            "mini_reason": f"문장 {sentence_idx} 분석 완료"
        }
        
    except Exception as e:
        print(f"상위 카테고리 응답 파싱 실패: {e}")
        return {
            "sentence_idx": sentence_idx,
            "error": str(e),
            "categories": []
        }

def _parse_child_response(content: str, sentence_idx: int) -> Dict[str, Any]:
    """하위 카테고리 응답을 파싱합니다."""
    try:
        # JSON 추출
        json_start = content.find('{')
        json_end = content.rfind('}') + 1
        
        if json_start == -1 or json_end == 0:
            raise ValueError("JSON을 찾을 수 없습니다")
        
        json_str = content[json_start:json_end]
        data = json.loads(json_str)
        
        return {
            "sentence_idx": sentence_idx,
            "subcategories": data.get("subcategories", []),
            "evidence_idx": [sentence_idx],
            "mini_reason": f"문장 {sentence_idx} 분석 완료"
        }
        
    except Exception as e:
        print(f"하위 카테고리 응답 파싱 실패: {e}")
        return {
            "sentence_idx": sentence_idx,
            "error": str(e),
            "subcategories": []
        }
