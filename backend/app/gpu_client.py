import os, json, aiohttp, logging, base64
from typing import Any, Dict, List

log = logging.getLogger(__name__)

GPU_ENDPOINT_URL = os.getenv("GPU_ENDPOINT_URL")
RUNPOD_API_KEY = os.getenv("RUNPOD_API_KEY")

class GpuClient:
    async def infer(self, csv_file_path: str) -> Dict[str, Any]:
        """
        요청(백엔드 → GPU):
        { "csv_file": "base64_encoded_csv_content" }

        응답(GPU → 백엔드): JSON 파일 형태로 결과 반환
        {
          "sentences": [
            {
              "text": "문장 원문",
              "weight": 1.25,        # sentiment + intent 종합 가중치
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

        # CSV 파일을 base64로 인코딩
        try:
            with open(csv_file_path, 'rb') as f:
                csv_content = f.read()
                csv_base64 = base64.b64encode(csv_content).decode('utf-8')
        except Exception as e:
            log.error(f"CSV 파일 읽기 실패: {e}")
            raise RuntimeError(f"CSV 파일 읽기 실패: {e}")

        payload = {"csv_file": csv_base64}
        headers = {"Authorization": f"Bearer {RUNPOD_API_KEY}", "Content-Type": "application/json"}

        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.post(GPU_ENDPOINT_URL, headers=headers, json=payload, timeout=300) as resp:  # 5분 타임아웃
                    if resp.status >= 400:
                        text = await resp.text()
                        log.error("GPU 오류 %s: %s", resp.status, text[:800])
                        resp.raise_for_status()
                    
                    # JSON 파일로 응답받기
                    content_type = resp.headers.get('content-type', '')
                    
                    if 'application/json' in content_type:
                        # JSON 직접 응답
                        data = await resp.json()
                    else:
                        # JSON 파일 다운로드
                        file_content = await resp.read()
                        try:
                            data = json.loads(file_content.decode('utf-8'))
                        except json.JSONDecodeError:
                            log.error("JSON 파일 파싱 실패")
                            raise ValueError("GPU 응답 JSON 파싱 오류")
                    
                    if "sentences" not in data:
                        log.error("GPU 응답에 'sentences' 필드 없음: %s", list(data.keys()))
                        raise ValueError("GPU 응답 형식 오류")
                    
                    log.info(f"GPU 분석 완료: {len(data.get('sentences', []))}개 문장")
                    return data
                        
        except aiohttp.ClientError as e:
            log.error("GPU 연결 오류: %s", e)
            raise RuntimeError(f"GPU 서버 연결 실패: {e}")
        except json.JSONDecodeError as e:
            log.error("GPU 응답 JSON 파싱 오류: %s", e)
            raise RuntimeError("GPU 응답 형식 오류")
        finally:
            # 임시 CSV 파일 정리
            try:
                if os.path.exists(csv_file_path):
                    os.unlink(csv_file_path)
                    log.info(f"임시 CSV 파일 정리: {csv_file_path}")
            except Exception as e:
                log.warning(f"임시 파일 정리 실패: {e}")

def get_gpu_client() -> GpuClient:
    return GpuClient()
