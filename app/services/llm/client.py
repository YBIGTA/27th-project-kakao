
import os, json, time
import logging
import httpx
from typing import Dict, Any, List
from .prompts import PARENT_PROMPT, CHILD_PROMPT, FINAL_SELECTION_PROMPT
from app.core.config import LLM as LLM_CFG
import math


logger = logging.getLogger(__name__)

class LLMClient:
    def __init__(self):
        if not LLM_CFG.api_key:
            raise RuntimeError("UPSTAGE_API_KEY is not set.")
        
        self.api_key = LLM_CFG.api_key
        self.base_url = LLM_CFG.base_url
        self.model = LLM_CFG.model
        self.timeout = LLM_CFG.request_timeout
        
        # HTTP 클라이언트 설정
        self.client = httpx.Client(
            timeout=self.timeout,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
        )

    def _chat_json(self, prompt: str, max_retries: int = 3) -> Dict[str, Any]:
        """Upstage API를 직접 호출하여 JSON 응답 받기"""
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Upstage API request attempt {attempt + 1}/{max_retries}")
                
                # Upstage API 요청 데이터
                request_data = {
                    "model": self.model,
                    "response_format": {"type": "json_object"},
                    "messages": [
                        {"role": "system", "content": "You are a helpful assistant that returns pure JSON."},
                        {"role": "user", "content": prompt},
                    ]
                }
                
                # API 호출
                response = self.client.post(
                    f"{self.base_url}/chat/completions",
                    json=request_data
                )
                response.raise_for_status()
                
                # 응답 파싱
                result = response.json()
                content = result["choices"][0]["message"]["content"]
                
                try:
                    data = json.loads(content)
                    logger.info("Upstage API request successful")
                    return data
                except json.JSONDecodeError as e:
                    logger.warning(f"JSON decode error on attempt {attempt + 1}: {e}")
                    
                    # 향상된 JSON 추출 로직
                    extracted_data = self._extract_json_from_text(content)
                    if extracted_data:
                        logger.info("JSON extraction successful")
                        return extracted_data
                    
                    if attempt == max_retries - 1:
                        logger.error("All retries failed for JSON parsing")
                        raise RuntimeError(f"Failed to parse LLM response as JSON after {max_retries} attempts")
                    
                    time.sleep(1)  # Brief delay before retry
                    
            except Exception as e:
                logger.error(f"Upstage API request error on attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    raise RuntimeError(f"Upstage API request failed after {max_retries} attempts: {e}")
                time.sleep(2 ** attempt)  # Exponential backoff
        
        # 모든 시도가 실패한 경우
        raise RuntimeError("All retries failed for Upstage API request")

    def _extract_json_from_text(self, text: str) -> Dict[str, Any]:
        """텍스트에서 JSON을 추출하는 향상된 로직"""
        try:
            # 1. 정확한 JSON 객체 찾기
            start = text.find("{")
            end = text.rfind("}")
            
            if start != -1 and end != -1 and end > start:
                json_str = text[start:end+1]
                
                # 2. JSON 유효성 검사 및 파싱
                try:
                    data = json.loads(json_str)
                    return data
                except json.JSONDecodeError:
                    pass
                
                # 3. 줄바꿈과 공백 정리 후 재시도
                json_str = json_str.replace('\n', ' ').replace('\r', ' ')
                json_str = ' '.join(json_str.split())  # 연속 공백 제거
                
                try:
                    data = json.loads(json_str)
                    return data
                except json.JSONDecodeError:
                    pass
                
                # 4. 마지막 시도: 특수 문자 제거
                import re
                json_str = re.sub(r'[^\x20-\x7E]', '', json_str)  # ASCII 문자만 유지
                
                try:
                    data = json.loads(json_str)
                    return data
                except json.JSONDecodeError:
                    pass
            
            # 5. 여러 JSON 객체가 있는 경우 첫 번째 시도
            json_objects = re.findall(r'\{[^{}]*\}', text)
            for json_obj in json_objects:
                try:
                    data = json.loads(json_obj)
                    if isinstance(data, dict) and len(data) > 0:
                        logger.info(f"Found partial JSON with {len(data)} keys")
                        return data
                except json.JSONDecodeError:
                    continue
            
            logger.warning("No valid JSON found in response")
            return {}
            
        except Exception as e:
            logger.error(f"JSON extraction failed: {e}")
            return {}

    def score_parents(self, sentences, parent_list):
        logger.info(f"Scoring {len(sentences)} sentences against {len(parent_list)} parent categories")
        prompt = PARENT_PROMPT.format(sentences="\n".join(sentences), parent_list=parent_list)
        return self._chat_json(prompt)

    def score_children(self, sentences: List[str], child_list: List[str], parent_categories: List[str]) -> Dict[str, dict]:
        """
        하위 카테고리별 점수 계산 (부모별로 그룹화하여 처리)
        """
        try:
            logger.info(f"score_children 시작: {len(sentences)} 문장, {len(child_list)} 하위 카테고리, {len(parent_categories)} 부모 카테고리")
            
            # 부모 카테고리별로 자식 카테고리 그룹화
            parent_child_groups = self._group_children_by_parent(child_list, parent_categories)
            logger.info(f"부모별 그룹화 완료: {len(parent_child_groups)}개 부모 그룹")
            
            all_results = {}
            
            for parent_name, children in parent_child_groups.items():
                logger.info(f"부모 카테고리 '{parent_name}'의 자식 {len(children)}개 처리 중...")
                
                # 한 부모의 모든 자식들을 한 배치로 처리
                batch_size = len(children)  # 부모별 자식 수에 맞춰 배치 크기 설정
                batches = [children[i:i + batch_size] for i in range(0, len(children), batch_size)]
                logger.info(f"  배치 구성: {len(batches)}개 배치, 배치 크기: {batch_size}")
                
                parent_results = {}
                
                for batch_idx, batch in enumerate(batches):
                    logger.info(f"  배치 {batch_idx + 1}/{len(batches)}: {len(batch)}개 카테고리")
                    
                    # 배치별 LLM 호출
                    batch_result = self._score_children_batch(sentences, batch)
                    logger.info(f"  배치 {batch_idx + 1} LLM 응답: {type(batch_result)}, 결과 수: {len(batch_result) if batch_result else 0}")
                    
                    if batch_result:
                        parent_results.update(batch_result)
                    
                    logger.info(f"  배치 {batch_idx + 1} 완료: {len(batch_result) if batch_result else 0}개 카테고리")
                
                # 부모 그룹 내에서 softmax 적용 (합이 1이 되도록)
                if parent_results:
                    parent_results = self._apply_softmax_to_parent_group(parent_results)
                    all_results.update(parent_results)
                    logger.info(f"  부모 '{parent_name}' softmax 적용 후: {len(parent_results)}개 자식 카테고리")
                else:
                    logger.warning(f"  부모 '{parent_name}'에서 결과 없음")
                
                logger.info(f"부모 '{parent_name}' 완료: {len(parent_results)}개 자식 카테고리")
            
            logger.info(f"전체 하위 카테고리 점수 계산 완료: {len(all_results)}개")
            return all_results
            
        except Exception as e:
            logger.error(f"Error in child scoring: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {}
    
    def _group_children_by_parent(self, child_list: List[str], parent_categories: List[str]) -> Dict[str, List[str]]:
        """
        하위 카테고리를 부모 카테고리별로 그룹화
        """
        # 부모-자식 매핑 정의
        parent_child_mapping = {
            "교환권": ["베이커리/도넛/떡", "카페", "아이스크림/빙수", "치킨", "버거/피자", "편의점", "한식/중식/일식", "패밀리/호텔뷔페", "퓨전/외국/펍", "분식/죽/도시락"],
            "상품권": ["상품권/마트", "뷰티/패션/건강", "영화/OTT/게임", "헤어/네일/스파", "전시/테마/체험", "생활/교육/취미", "종교/나눔"],
            "뷰티": ["명품화장품", "향수", "바디", "스킨케어", "메이크업", "헤어/미용", "남성화장품"],
            "패션": ["명품브랜드", "주얼리", "파자마", "브랜드 가방/지갑", "브랜드 의류", "브랜드 신발", "언더웨어", "디자이너 브랜드", "브랜드 잡화", "브랜드 시계", "주문각인"],
            "식품": ["과일/견과/채소", "축산/수산", "쌀/반찬/김치", "건강식품", "다이어트/이너뷰티", "가공/보양식", "케이크", "디저트", "유제품/아이스크림", "커피/차/음료"],
            "와인/양주/전통주": ["와인", "양주", "전통주", "맥주/기타"],
            "리빙/도서": ["주방/수입주방", "캔들디퓨저 인센스", "식물/꽃배달", "침대/패브릭", "조명/무드등", "인테리어", "생필품", "수납/생활", "가구/DIY", "팬시/캐릭터", "문구/취미", "도서", "명품리빙", "리빙편집샵"],
            "레저/스포츠": ["글로벌 브랜드본사", "스포츠 의류", "스포츠 슈즈", "스포츠 잡화", "요가/헬스/수영", "레저/캠핑", "등산/아웃도어", "차량용품", "여행용품", "차량용 방향제"],
            "골프선물": ["골프/테니스"],
            "아티스트/캐릭터": ["스타앨범", "애니메이션 캐릭터", "인디작가", "애니멀캐릭터", "웹소설", "게임"],
            "유아동/반려": ["신생아선물세트", "베이비패션", "키즈패션", "임신/출산/육아", "장난감/인형", "유아교육/도서", "기저귀/물티슈", "분유/간식/영양제", "강아지 간식/용품", "고양이 간식/용품", "기타 소동물용품"],
            "디지털/가전": ["프리미엄 가전", "케이스", "모바일 액세서리", "미니가전", "건강용품/가전", "디지털/음향기기", "생활가전", "주방가전", "미용가전", "카메라"],
            "카카오프렌즈": ["토이", "리빙", "테크", "문구", "패션", "푸드", "골프"]
        }
        
        # 부모별로 그룹화
        groups = {}
        for parent in parent_categories:
            if parent in parent_child_mapping:
                # 해당 부모의 자식들 중에서 child_list에 있는 것만 필터링
                children = [child for child in parent_child_mapping[parent] if child in child_list]
                if children:
                    groups[parent] = children
        
        return groups
    
    def _score_children_batch(self, sentences: List[str], child_batch: List[str]) -> Dict[str, dict]:
        """
        배치별 하위 카테고리 점수 계산
        """
        try:
            logger.info(f"_score_children_batch 시작: {len(sentences)} 문장, {len(child_batch)} 카테고리")
            
            prompt = CHILD_PROMPT.format(
                sentences="\n".join(sentences),
                child_list=", ".join(child_batch)
            )
            
            logger.info(f"LLM 호출 중... (프롬프트 길이: {len(prompt)}자)")
            response = self._chat_json(prompt)
            logger.info(f"LLM 응답 타입: {type(response)}")
            logger.info(f"LLM 응답 내용: {response}")
            
            results = {}
            
            # 다양한 응답 형식 처리
            if response and isinstance(response, dict):
                # 형식 1: subcategories 배열
                if "subcategories" in response and isinstance(response["subcategories"], list):
                    for item in response["subcategories"]:
                        if isinstance(item, dict) and "name" in item:
                            name = item["name"]
                            results[name] = {
                                "relevance_raw": float(item.get("relevance_raw", 0.0)),
                                "interest_raw": float(item.get("interest_raw", 0.0)),
                                "reasoning": item.get("reasoning", ""),
                                "evidence_idx": self._parse_evidence_idx(item.get("evidence_idx", -1))
                            }
                
                # 형식 2: 직접 카테고리별 점수
                else:
                    for child_name in child_batch:
                        if child_name in response:
                            item = response[child_name]
                            if isinstance(item, dict):
                                results[child_name] = {
                                    "relevance_raw": float(item.get("relevance_raw", 0.0)),
                                    "interest_raw": float(item.get("interest_raw", 0.0)),
                                    "reasoning": item.get("reasoning", ""),
                                    "evidence_idx": self._parse_evidence_idx(item.get("evidence_idx", -1))
                                }
                            elif isinstance(item, (int, float)):
                                # 단순 점수인 경우
                                results[child_name] = {
                                    "relevance_raw": float(item),
                                    "interest_raw": float(item),
                                    "reasoning": f"LLM 점수: {item}",
                                    "evidence_idx": -1
                                }
            
            # 형식 3: 응답이 리스트인 경우
            elif response and isinstance(response, list):
                for item in response:
                    if isinstance(item, dict) and "name" in item:
                        name = item["name"]
                        results[name] = {
                            "relevance_raw": float(item.get("relevance_raw", 0.0)),
                            "interest_raw": float(item.get("interest_raw", 0.0)),
                            "reasoning": item.get("reasoning", ""),
                            "evidence_idx": self._parse_evidence_idx(item.get("evidence_idx", -1))
                        }
            
            # 결과가 없으면 기본값으로 생성
            if not results:
                logger.warning(f"LLM 응답에서 데이터를 추출할 수 없음. 기본값 생성: {child_batch}")
                for child_name in child_batch:
                    results[child_name] = {
                        "relevance_raw": 0.5,  # 기본값
                        "interest_raw": 0.5,   # 기본값
                        "reasoning": "기본값 (LLM 응답 파싱 실패)",
                        "evidence_idx": -1
                    }
            
            logger.info(f"배치 처리 결과: {len(results)}개 카테고리")
            return results
                
        except Exception as e:
            logger.error(f"Error in batch scoring: {e}")
            # 에러 시에도 기본값 반환
            results = {}
            for child_name in child_batch:
                results[child_name] = {
                    "relevance_raw": 0.5,
                    "interest_raw": 0.5,
                    "reasoning": f"에러 발생: {str(e)}",
                    "evidence_idx": -1
                }
            return results
    
    def _parse_evidence_idx(self, evidence_idx) -> int:
        """
        LLM이 반환하는 evidence_idx를 정수로 파싱
        """
        try:
            if evidence_idx is None:
                return -1
            elif isinstance(evidence_idx, list):
                # 리스트인 경우 첫 번째 요소 사용
                if evidence_idx:
                    first_item = evidence_idx[0]
                    if isinstance(first_item, (int, float)):
                        return int(first_item)
                    elif isinstance(first_item, str):
                        # 문자열에서 숫자만 추출
                        import re
                        numbers = re.findall(r'\d+', first_item)
                        if numbers:
                            return int(numbers[0])
                        return -1
                    else:
                        return -1
                return -1
            elif isinstance(evidence_idx, str):
                # 문자열에서 숫자만 추출
                import re
                numbers = re.findall(r'\d+', evidence_idx)
                if numbers:
                    return int(numbers[0])
                return -1
            elif isinstance(evidence_idx, (int, float)):
                return int(evidence_idx)
            else:
                return -1
        except Exception as e:
            logger.warning(f"evidence_idx 파싱 실패: {evidence_idx}, 오류: {e}")
            return -1

    def _softmax(self, scores: List[float], temperature: float = 1.0) -> List[float]:
        """
        점수 배열에 softmax 적용
        """
        try:
            if not scores:
                return []
            
            # 온도 조정
            adjusted_scores = [score / temperature for score in scores]
            
            # 최대값으로 정규화 (수치 안정성)
            max_score = max(adjusted_scores)
            exp_scores = [math.exp(score - max_score) for score in adjusted_scores]
            
            # 합계 계산
            sum_exp_scores = sum(exp_scores)
            
            # 확률 계산
            probabilities = [exp_score / sum_exp_scores for exp_score in exp_scores]
            
            return probabilities
            
        except Exception as e:
            logger.error(f"Error in softmax calculation: {e}")
            # 오류 시 균등 분포 반환
            return [1.0 / len(scores)] * len(scores) if scores else []

    def _apply_softmax_to_parent_group(self, parent_results: Dict[str, dict]) -> Dict[str, dict]:
        """
        같은 부모를 공유하는 형제들 간에 softmax 적용 (점수 합이 1이 되도록)
        """
        try:
            if not parent_results:
                return parent_results
            
            # relevance_raw + interest_raw = 최종 스코어
            final_scores = []
            for item in parent_results.values():
                relevance = item.get("relevance_raw", 0.0)
                interest = item.get("interest_raw", 0.0)
                final_score = relevance + interest
                final_scores.append(final_score)
            
            # 최종 스코어에 softmax 적용
            probabilities = self._softmax(final_scores)
            
            # 결과 업데이트
            result_keys = list(parent_results.keys())
            for i, key in enumerate(result_keys):
                parent_results[key]["final_score"] = final_scores[i]
                parent_results[key]["probability"] = probabilities[i]
            
            return parent_results
            
        except Exception as e:
            logger.error(f"Error applying softmax to parent group: {e}")
            return parent_results

    def select_products(self, *, profile, parent_scores_info, parent_evidence_info,
                        child_scores_info, child_evidence_info, candidate_products_info):
        logger.info("Selecting final products")
        prompt = FINAL_SELECTION_PROMPT.format(
            age=profile.get("age"),
            gender=profile.get("gender"),
            relation=profile.get("relation"),
            budget_min=profile.get("budget_min"),
            budget_max=profile.get("budget_max"),
            parent_scores_info=parent_scores_info,
            parent_evidence_info=parent_evidence_info,
            child_scores_info=child_scores_info,
            child_evidence_info=child_evidence_info,
            candidate_products_info=candidate_products_info,
        )
        return self._chat_json(prompt)
