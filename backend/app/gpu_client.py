import os, json, aiohttp, logging
from typing import Any, Dict, List

log = logging.getLogger(__name__)

GPU_ENDPOINT_URL = os.getenv("GPU_ENDPOINT_URL")
RUNPOD_API_KEY = os.getenv("RUNPOD_API_KEY")

class GpuClient:
    async def infer(self, sentences: List[str]) -> Dict[str, Any]:
        """
        요청(백엔드 → GPU):
        { "sentences": ["...전처리된 문장1", "문장2", "..."] }

        응답(GPU → 백엔드): per_sentence = 문장별 원천 스코어
        {
          "per_sentence": [
            {
              "text": "문장 원문",
              "ts": "2025-08-01T12:34:56Z",
              "sentiment": 0.73,     # -1..1
              "intent": "purchase",  # need|purchase|interest|consider|negative|informative (국/영문 허용)
              "cat_scores": {        # DB products.sub_category와 100% 일치하는 라벨
                "베이커리/도넛/떡": 0.62,
                "향수": 0.81,
                "머그컵": 0.27
              }
            }
          ]
        }
        """
        if not GPU_ENDPOINT_URL or not RUNPOD_API_KEY:
            raise RuntimeError("GPU_ENDPOINT_URL / RUNPOD_API_KEY 미설정")

        payload = {"sentences": sentences}
        headers = {"Authorization": f"Bearer {RUNPOD_API_KEY}", "Content-Type": "application/json"}

        async with aiohttp.ClientSession() as sess:
            async with sess.post(GPU_ENDPOINT_URL, headers=headers, json=payload, timeout=60) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    log.error("GPU error %s: %s", resp.status, text[:800])
                    resp.raise_for_status()
                data = json.loads(text)
                if "per_sentence" not in data:
                    log.error("GPU 응답에 'per_sentence' 없음: %s", list(data.keys()))
                return data

def get_gpu_client() -> GpuClient:
    return GpuClient()
