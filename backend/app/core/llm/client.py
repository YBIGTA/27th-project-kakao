"""
다중 API 키를 사용한 LLM 클라이언트
여러 API 키로 병렬 처리하여 처리 속도 향상 + 캐싱 + 배치 처리
"""

import asyncio
import aiohttp
import json
import os
import hashlib
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

class Cache:
    """간단한 메모리 캐시"""
    def __init__(self):
        self._cache = {}
    
    def get(self, key: str) -> Optional[str]:
        if key in self._cache:
            value, expiry = self._cache[key]
            if datetime.now() < expiry:
                return value
            else:
                del self._cache[key]
        return None
    
    def set(self, key: str, value: str, ttl: int = 3600):
        expiry = datetime.now() + timedelta(seconds=ttl)
        self._cache[key] = (value, expiry)

def make_cache_key(prefix: str, data: Dict[str, Any]) -> str:
    """캐시 키 생성"""
    data_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return f"{prefix}:{hashlib.md5(data_str.encode()).hexdigest()}"

_cache = Cache()

class MultiKeyLLMClient:
    """여러 API 키를 사용하는 LLM 클라이언트 (병렬 처리 + 캐싱 + 배치 처리)"""
    
    def __init__(self, api_keys: List[str] = None):
        """
        Args:
            api_keys: Upstage API 키 목록
        """
        if api_keys is None:
            # 환경변수에서 API 키들 가져오기 (5개)
            api_keys = []
            for i in range(1, 6):  # UPSTAGE_API_KEY_1 ~ UPSTAGE_API_KEY_5
                key = os.getenv(f"UPSTAGE_API_KEY_{i}")
                if key:
                    api_keys.append(key)
            
            # 기본 API 키도 추가
            default_key = os.getenv("UPSTAGE_API_KEY")
            if default_key and default_key not in api_keys:
                api_keys.append(default_key)
        
        self.api_keys = api_keys
        self.current_key_index = 0
        self.base_url = os.getenv("UPSTAGE_BASE_URL", "https://api.upstage.ai/v1")
        self.chat_model = os.getenv("UPSTAGE_CHAT_MODEL", "solar-1-mini-chat")
        self.timeout = int(os.getenv("TIMEOUT_SECS", "30"))
        self.cache_ttl = int(os.getenv("CACHE_TTL_SECS", "3600"))
        
        print(f"🔑 사용 가능한 API 키: {len(self.api_keys)}개")
        if len(self.api_keys) < 2:
            print("⚠️ API 키가 2개 미만입니다. 병렬 처리가 제한됩니다.")
        elif len(self.api_keys) >= 5:
            print("🚀 5개 API 키로 최대 성능으로 병렬 처리 가능!")
    
    def _get_next_api_key(self) -> str:
        """다음 API 키를 가져옵니다 (라운드 로빈 방식)."""
        if not self.api_keys:
            raise ValueError("사용 가능한 API 키가 없습니다.")
        
        key = self.api_keys[self.current_key_index]
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        return key
    
    async def _call_upstage_api(self, prompt: str, api_key: str) -> Dict[str, Any]:
        """Upstage API를 호출합니다."""
        url = f"{self.base_url}/chat/completions"
        
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": self.chat_model,
            "messages": [
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.1,
            "max_tokens": 2000,
            "response_format": {"type": "json_object"}
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data, timeout=self.timeout) as response:
                if response.status == 200:
                    result = await response.json()
                    return result
                else:
                    error_text = await response.text()
                    raise RuntimeError(f"API 호출 실패: {response.status} - {error_text}")

    async def score_parent_sentence(self, text: str, parent_labels: List[str], e_idx: int, date: str = None) -> Dict[str, Any]:
        """단일 문장 상위 카테고리 점수화 (캐싱 포함)"""
        print(f"🔍 LLMClient.score_parent_sentence 호출: text={text[:50]}..., e_idx={e_idx}")
        
        # 캐시 키 생성
        key = make_cache_key("parent", {
            "text": text, 
            "labels": parent_labels, 
            "e_idx": e_idx, 
            "date": date, 
            "model": self.chat_model
        })
        
        # 캐시 확인
        cached = _cache.get(key)
        if cached: 
            print(f"✅ 캐시 히트: 문장 {e_idx}")
            return json.loads(cached)

        # 실제 LLM API 호출
        try:
            from .prompts import format_parent_prompt
            
            # 실제 날짜가 있으면 사용, 없으면 현재 날짜
            actual_date = date if date else datetime.now().strftime("%Y-%m-%d")
            sentences = [{"idx": e_idx, "date": actual_date, "text": text}]
            
            prompt = format_parent_prompt(sentences, parent_labels)
            print(f"🔍 prompt 생성 완료 (길이: {len(prompt)})")
            
            # 라운드 로빈으로 API 키 선택
            api_key = self._get_next_api_key()
            data = await self._call_upstage_api(prompt, api_key)
            out = json.loads(data["choices"][0]["message"]["content"])
            
            # 캐시 저장
            _cache.set(key, json.dumps(out, ensure_ascii=False), ttl=self.cache_ttl)
            print(f"💾 캐시 저장: 문장 {e_idx}")
            
            return out
            
        except Exception as e:
            print(f"❌ LLMClient.score_parent_sentence 오류: {e}")
            import traceback
            traceback.print_exc()
            raise

    async def score_child_sentence(self, text: str, child_labels: List[str], e_idx: int, date: str = None) -> Dict[str, Any]:
        """단일 문장 하위 카테고리 점수화 (캐싱 포함)"""
        # 캐시 키 생성
        key = make_cache_key("child", {
            "text": text, 
            "labels": child_labels, 
            "e_idx": e_idx, 
            "date": date, 
            "model": self.chat_model
        })
        
        # 캐시 확인
        cached = _cache.get(key)
        if cached: 
            print(f"✅ 캐시 히트: 문장 {e_idx}")
            return json.loads(cached)

        # 실제 LLM API 호출
        try:
            from .prompts import format_child_prompt
            
            actual_date = date if date else datetime.now().strftime("%Y-%m-%d")
            sentences = [{"idx": e_idx, "date": actual_date, "text": text}]
            prompt = format_child_prompt(sentences, child_labels)
            
            api_key = self._get_next_api_key()
            data = await self._call_upstage_api(prompt, api_key)
            out = json.loads(data["choices"][0]["message"]["content"])
            
            # 캐시 저장
            _cache.set(key, json.dumps(out, ensure_ascii=False), ttl=self.cache_ttl)
            print(f"💾 캐시 저장: 문장 {e_idx}")
            
            return out
            
        except Exception as e:
            print(f"❌ LLMClient.score_child_sentence 오류: {e}")
            import traceback
            traceback.print_exc()
            raise

    async def score_parent_batch(self, sentences: List[Dict[str, Any]], parent_labels: List[str]) -> Dict[str, Any]:
        """여러 문장을 한 번에 처리하여 상위 카테고리 점수화 (배치 처리)"""
        print(f"🔍 LLMClient.score_parent_batch 호출: {len(sentences)}개 문장")
        
        # 캐시 키 생성 (배치 전체를 위한 키)
        batch_key = make_cache_key("parent_batch", {
            "sentences": sentences, 
            "labels": parent_labels, 
            "model": self.chat_model
        })
        
        cached = _cache.get(batch_key)
        if cached: 
            print(f"✅ 배치 캐시 히트: {len(sentences)}개 문장")
            return json.loads(cached)

        try:
            from .prompts import format_parent_batch_prompt
            prompt = format_parent_batch_prompt(sentences, parent_labels)
            print(f"🔍 배치 프롬프트 생성 완료 (길이: {len(prompt)})")
            
            api_key = self._get_next_api_key()
            data = await self._call_upstage_api(prompt, api_key)
            out = json.loads(data["choices"][0]["message"]["content"])
            
            # 캐시 저장
            _cache.set(batch_key, json.dumps(out, ensure_ascii=False), ttl=self.cache_ttl)
            print(f"💾 배치 캐시 저장: {len(sentences)}개 문장")
            
            return out
            
        except Exception as e:
            print(f"❌ LLMClient.score_parent_batch 오류: {e}")
            import traceback
            traceback.print_exc()
            raise

    async def score_child_batch(self, sentences: List[Dict[str, Any]], child_labels: List[str]) -> Dict[str, Any]:
        """여러 문장을 한 번에 처리하여 하위 카테고리 점수화 (배치 처리)"""
        print(f"🔍 LLMClient.score_child_batch 호출: {len(sentences)}개 문장")
        
        # 캐시 키 생성 (배치 전체를 위한 키)
        batch_key = make_cache_key("child_batch", {
            "sentences": sentences, 
            "labels": child_labels, 
            "model": self.chat_model
        })
        
        cached = _cache.get(batch_key)
        if cached: 
            print(f"✅ 배치 캐시 히트: {len(sentences)}개 문장")
            return json.loads(cached)

        try:
            from .prompts import format_child_batch_prompt
            prompt = format_child_batch_prompt(sentences, child_labels)
            print(f"🔍 배치 프롬프트 생성 완료 (길이: {len(prompt)})")
            
            api_key = self._get_next_api_key()
            data = await self._call_upstage_api(prompt, api_key)
            out = json.loads(data["choices"][0]["message"]["content"])
            
            # 캐시 저장
            _cache.set(batch_key, json.dumps(out, ensure_ascii=False), ttl=self.cache_ttl)
            print(f"💾 배치 캐시 저장: {len(sentences)}개 문장")
            
            return out
            
        except Exception as e:
            print(f"❌ LLMClient.score_child_batch 오류: {e}")
            import traceback
            traceback.print_exc()
            raise

    async def score_parent_batch_with_prompt(self, prompt: str) -> Dict[str, Any]:
        """미리 생성된 프롬프트로 상위 카테고리 배치 처리"""
        print(f"🔍 LLMClient.score_parent_batch_with_prompt 호출: 프롬프트 길이={len(prompt)}")
        
        # 캐시 키 생성 (프롬프트 내용 기반)
        batch_key = make_cache_key("parent_batch_prompt", {
            "prompt": prompt, 
            "model": self.chat_model
        })
        
        cached = _cache.get(batch_key)
        if cached: 
            print(f"✅ 프롬프트 캐시 히트")
            return json.loads(cached)

        try:
            api_key = self._get_next_api_key()
            data = await self._call_upstage_api(prompt, api_key)
            out = json.loads(data["choices"][0]["message"]["content"])
            
            # 캐시 저장
            _cache.set(batch_key, json.dumps(out, ensure_ascii=False), ttl=self.cache_ttl)
            print(f"💾 프롬프트 캐시 저장")
            
            return out
            
        except Exception as e:
            print(f"❌ LLMClient.score_parent_batch_with_prompt 오류: {e}")
            import traceback
            traceback.print_exc()
            raise

    async def score_child_batch_with_prompt(self, prompt: str) -> Dict[str, Any]:
        """미리 생성된 프롬프트로 하위 카테고리 배치 처리"""
        print(f"🔍 LLMClient.score_child_batch_with_prompt 호출: 프롬프트 길이={len(prompt)}")
        
        # 캐시 키 생성 (프롬프트 내용 기반)
        batch_key = make_cache_key("child_batch_prompt", {
            "prompt": prompt, 
            "model": self.chat_model
        })
        
        cached = _cache.get(batch_key)
        if cached: 
            print(f"✅ 프롬프트 캐시 히트")
            return json.loads(cached)

        try:
            api_key = self._get_next_api_key()
            data = await self._call_upstage_api(prompt, api_key)
            out = json.loads(data["choices"][0]["message"]["content"])
            
            # 캐시 저장
            _cache.set(batch_key, json.dumps(out, ensure_ascii=False), ttl=self.cache_ttl)
            print(f"💾 프롬프트 캐시 저장")
            
            return out
            
        except Exception as e:
            print(f"❌ LLMClient.score_child_batch_with_prompt 오류: {e}")
            import traceback
            traceback.print_exc()
            raise

# 싱글톤 인스턴스
llm_client = MultiKeyLLMClient()
