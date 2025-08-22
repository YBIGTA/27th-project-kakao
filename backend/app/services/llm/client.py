import json, asyncio
from typing import List, Dict, Any
import httpx
from core.config import (
    LLM_PROVIDER, UPSTAGE_BASE_URL, UPSTAGE_API_KEY, 
    UPSTAGE_CHAT_MODEL, TIMEOUT_SECS, CACHE_TTL_SECS
)
from utils.cache import new_cache, make_cache_key

_cache = new_cache()

class LLMClient:
    """Upstage/OpenAI-compatible Chat API client with caching & concurrency"""
    def __init__(self):
        self.provider = LLM_PROVIDER.upper()
        self.base_url = UPSTAGE_BASE_URL
        self.api_key  = UPSTAGE_API_KEY
        self.chat_model = UPSTAGE_CHAT_MODEL
        self.timeout = TIMEOUT_SECS

    async def _post_json(self, path: str, payload: dict) -> dict:
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.post(f"{self.base_url}{path}", headers=headers, json=payload)
            r.raise_for_status()
            return r.json()

    async def score_parent_sentence(self, text: str, parent_labels: List[str], e_idx: int, date: str = None) -> Dict[str, Any]:
        print(f"🔍 LLMClient.score_parent_sentence 호출: text={text[:50]}..., e_idx={e_idx}, date={date}")
        
        key = make_cache_key("parent", {"text": text, "labels": parent_labels, "e_idx": e_idx, "date": date, "model": self.chat_model, "prov": self.provider})
        cached = _cache.get(key)
        if cached: return json.loads(cached)

        # 실제 LLM API 호출
        try:
            from services.llm.prompts import format_parent_prompt
            # 실제 날짜가 있으면 사용, 없으면 현재 날짜
            from datetime import datetime
            actual_date = date if date else datetime.now().strftime("%Y-%m-%d")
            sentences = [{"idx": e_idx, "date": actual_date, "text": text}]
            print(f"🔍 sentences 생성: {sentences}")
            
            prompt = format_parent_prompt(sentences, parent_labels)
            print(f"🔍 prompt 생성 완료 (길이: {len(prompt)})")
            
            payload = {"model": self.chat_model, "messages":[{"role":"user","content":prompt}], "temperature":0, "response_format":{"type":"json_object"}}
            print(f"🔍 payload 생성 완료")
            
            data = await self._post_json("/chat/completions", payload)
            out = json.loads(data["choices"][0]["message"]["content"])
            
            _cache.set(key, json.dumps(out, ensure_ascii=False), ttl=CACHE_TTL_SECS)
            return out
            
        except Exception as e:
            print(f"❌ LLMClient.score_parent_sentence 오류: {e}")
            import traceback
            traceback.print_exc()
            raise

    async def score_child_sentence(self, text: str, child_labels: List[str], e_idx: int, date: str = None) -> Dict[str, Any]:
        key = make_cache_key("child", {"text": text, "labels": child_labels, "e_idx": e_idx, "date": date, "model": self.chat_model, "prov": self.provider})
        cached = _cache.get(key)
        if cached: return json.loads(cached)

        # 실제 LLM API 호출
        from services.llm.prompts import format_child_prompt
        # 실제 날짜가 있으면 사용, 없으면 현재 날짜
        from datetime import datetime
        actual_date = date if date else datetime.now().strftime("%Y-%m-%d")
        sentences = [{"idx": e_idx, "date": actual_date, "text": text}]
        prompt = format_child_prompt(sentences, child_labels)
        payload = {"model": self.chat_model, "messages":[{"role":"user","content":prompt}], "temperature":0, "response_format":{"type":"json_object"}}
        data = await self._post_json("/chat/completions", payload)
        out = json.loads(data["choices"][0]["message"]["content"])
        
        _cache.set(key, json.dumps(out, ensure_ascii=False), ttl=CACHE_TTL_SECS)
        return out

    async def score_parent_batch(self, sentences: List[Dict[str, Any]], parent_labels: List[str]) -> Dict[str, Any]:
        """여러 문장을 한 번에 처리하여 상위 카테고리 점수화"""
        print(f"🔍 LLMClient.score_parent_batch 호출: {len(sentences)}개 문장")
        
        # 캐시 키 생성 (배치 전체를 위한 키)
        batch_key = make_cache_key("parent_batch", {
            "sentences": sentences, 
            "labels": parent_labels, 
            "model": self.chat_model, 
            "prov": self.provider
        })
        cached = _cache.get(batch_key)
        if cached: return json.loads(cached)

        try:
            from services.llm.prompts import format_parent_batch_prompt
            prompt = format_parent_batch_prompt(sentences, parent_labels)
            print(f"🔍 배치 프롬프트 생성 완료 (길이: {len(prompt)})")
            
            payload = {
                "model": self.chat_model, 
                "messages": [{"role": "user", "content": prompt}], 
                "temperature": 0, 
                "response_format": {"type": "json_object"}
            }
            
            data = await self._post_json("/chat/completions", payload)
            out = json.loads(data["choices"][0]["message"]["content"])
            
            _cache.set(batch_key, json.dumps(out, ensure_ascii=False), ttl=CACHE_TTL_SECS)
            return out
            
        except Exception as e:
            print(f"❌ LLMClient.score_parent_batch 오류: {e}")
            import traceback
            traceback.print_exc()
            raise

    async def score_child_batch(self, sentences: List[Dict[str, Any]], child_labels: List[str]) -> Dict[str, Any]:
        """여러 문장을 한 번에 처리하여 하위 카테고리 점수화"""
        print(f"🔍 LLMClient.score_child_batch 호출: {len(sentences)}개 문장")
        
        # 캐시 키 생성 (배치 전체를 위한 키)
        batch_key = make_cache_key("child_batch", {
            "sentences": sentences, 
            "labels": child_labels, 
            "model": self.chat_model, 
            "prov": self.provider
        })
        cached = _cache.get(batch_key)
        if cached: return json.loads(cached)

        try:
            from services.llm.prompts import format_child_batch_prompt
            prompt = format_child_batch_prompt(sentences, child_labels)
            print(f"🔍 배치 프롬프트 생성 완료 (길이: {len(prompt)})")
            
            payload = {
                "model": self.chat_model, 
                "messages": [{"role": "user", "content": prompt}], 
                "temperature": 0, 
                "response_format": {"type": "json_object"}
            }
            
            data = await self._post_json("/chat/completions", payload)
            out = json.loads(data["choices"][0]["message"]["content"])
            
            _cache.set(batch_key, json.dumps(out, ensure_ascii=False), ttl=CACHE_TTL_SECS)
            return out
            
        except Exception as e:
            print(f"❌ LLMClient.score_child_batch 오류: {e}")
            import traceback
            traceback.print_exc()
            raise

    # 🚀 최적화: 미리 생성된 프롬프트를 받아서 처리하는 메서드들
    async def score_parent_batch_with_prompt(self, prompt: str) -> Dict[str, Any]:
        """미리 생성된 프롬프트로 상위 카테고리 배치 처리"""
        print(f"🔍 LLMClient.score_parent_batch_with_prompt 호출: 프롬프트 길이={len(prompt)}")
        
        # 캐시 키 생성 (프롬프트 내용 기반)
        batch_key = make_cache_key("parent_batch_prompt", {
            "prompt": prompt, 
            "model": self.chat_model, 
            "prov": self.provider
        })
        cached = _cache.get(batch_key)
        if cached: return json.loads(cached)

        try:
            payload = {
                "model": self.chat_model, 
                "messages": [{"role": "user", "content": prompt}], 
                "temperature": 0, 
                "response_format": {"type": "json_object"}
            }
            
            data = await self._post_json("/chat/completions", payload)
            out = json.loads(data["choices"][0]["message"]["content"])
            
            _cache.set(batch_key, json.dumps(out, ensure_ascii=False), ttl=CACHE_TTL_SECS)
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
            "model": self.chat_model, 
            "prov": self.provider
        })
        cached = _cache.get(batch_key)
        if cached: return json.loads(cached)

        try:
            payload = {
                "model": self.chat_model, 
                "messages": [{"role": "user", "content": prompt}], 
                "temperature": 0, 
                "response_format": {"type": "json_object"}
            }
            
            data = await self._post_json("/chat/completions", payload)
            out = json.loads(data["choices"][0]["message"]["content"])
            
            _cache.set(batch_key, json.dumps(out, ensure_ascii=False), ttl=CACHE_TTL_SECS)
            return out
            
        except Exception as e:
            print(f"❌ LLMClient.score_child_batch_with_prompt 오류: {e}")
            import traceback
            traceback.print_exc()
            raise
