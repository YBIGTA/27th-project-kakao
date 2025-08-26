from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import tempfile
import os
import json
import logging
from typing import Optional
import uvicorn
from dotenv import load_dotenv
from core.pipeline import run_pipeline

# 환경 변수 로딩
load_dotenv('.env')
print(f"UPSTAGE_API_KEY_1 loaded: {'UPSTAGE_API_KEY_1' in os.environ}")
print(f"API Key value: {os.getenv('UPSTAGE_API_KEY_1', 'NOT_FOUND')[:10]}...")
print(f"DB_URL loaded: {'DB_URL' in os.environ}")
print(f"DB_URL value: {os.getenv('DB_URL', 'NOT_FOUND')[:30]}...")

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="카카오 선물 추천 API", version="1.0.0")

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # React 개발 서버
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "카카오 선물 추천 API 서버가 실행 중입니다"}

@app.post("/recommendations")
async def get_recommendations(
    file: UploadFile = File(...),
    selected_user: str = Form(...),
    age: int = Form(...),
    gender: str = Form(...),
    relation: str = Form(...),
    budget_min: int = Form(...),
    budget_max: int = Form(...)
):
    """
    카카오톡 대화 파일을 분석하여 선물을 추천하는 API
    """
    try:
        # 입력 검증
        if age <= 0 or age > 120:
            raise HTTPException(status_code=400, detail="나이는 1-120 사이여야 합니다")
        if budget_min < 0 or budget_max < 0:
            raise HTTPException(status_code=400, detail="예산은 0 이상이어야 합니다")
        if budget_min > budget_max:
            raise HTTPException(status_code=400, detail="최소 예산은 최대 예산보다 작아야 합니다")
        
        # 파일 확장자 검증
        if not (file.filename.endswith('.txt') or file.filename.endswith('.csv')):
            raise HTTPException(status_code=400, detail="텍스트 파일(.txt) 또는 CSV 파일(.csv)만 업로드 가능합니다")
        
        # 파일 내용을 메모리에서 직접 처리
        content = await file.read()
        
        # 임시 파일로 저장 (파이프라인이 파일 경로를 요구하므로)
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1]) as temp_file:
            temp_file.write(content)
            temp_file_path = temp_file.name
        
        logger.info(f"업로드된 파일 처리: {file.filename} -> {temp_file_path}")
        
        try:
            # 성별 매핑
            gender_mapping = {
                "남": "남성",
                "여": "여성",
                "male": "남성",
                "female": "여성"
            }
            mapped_gender = gender_mapping.get(gender, gender)
            
            # 프로필 구성
            profile = {
                "age": age,
                "gender": mapped_gender,
                "relation": relation,
                "budget_min": budget_min,
                "budget_max": budget_max,
                "products_csv_path": "kakao_gifts.normalized.csv",  # 기본 상품 데이터베이스
                "target_user": selected_user if selected_user != "전체" else "default",
                "chat_csv_path": temp_file_path,  # 업로드된 파일 경로 추가
            }
            
            logger.info(f"파이프라인 실행 시작: {temp_file_path}")
            logger.info(f"프로필: {profile}")
            
            # 파이프라인 실행
            result = run_pipeline(profile)
            
            # 결과 검증
            if "error" in result:
                logger.error(f"파이프라인 실행 실패: {result['error']}")
                raise HTTPException(status_code=500, detail=result["error"])
            
            selected_products = result.get("selected_products", [])
            if not selected_products:
                return JSONResponse({
                    "success": True,
                    "message": "추천된 상품이 없습니다",
                    "data": {
                        "user_context": {
                            "age": age,
                            "gender": mapped_gender,
                            "relation": relation,
                            "budget_min": budget_min,
                            "budget_max": budget_max
                        },
                        "analysis": {
                            "top3_children": [],
                            "detailed_reasoning": ["분석 결과 추천할 수 있는 상품이 없습니다."],
                            "subcats": [],
                            "evidence_by_cat": {}
                        },
                        "products": {
                            "selected_products": []
                        }
                    }
                })
            
            # 전처리된 메시지 정보
            processed_messages = result.get("processed_messages", [])
            
            # 분석 결과 추출
            analysis = result.get("analysis", {})
            
            # 응답 데이터 구성
            response_data = {
                "success": True,
                "message": "선물 추천이 완료되었습니다",
                "data": {
                    "user_context": {
                        "age": age,
                        "gender": mapped_gender,
                        "relation": relation,
                        "budget_min": budget_min,
                        "budget_max": budget_max
                    },
                    "analysis": {
                        "top3_children": analysis.get("top3_children", []),
                        "detailed_reasoning": analysis.get("detailed_reasoning", []),
                        "subcats": analysis.get("subcats", []),
                        "evidence_by_cat": analysis.get("evidence_by_cat", {}),
                        "message": f"총 {len(selected_products)}개의 상품을 추천합니다."
                    },
                    "products": {
                        "selected_products": selected_products
                    }
                },
                "summary": {
                    "total_products": len(selected_products),
                    "categories": list(set(p.get("category", "") for p in selected_products if p.get("category"))),
                    "processed_messages": len(processed_messages) if processed_messages else 0,
                    "target_user": selected_user
                }
            }
            
            logger.info(f"추천 완료: {len(selected_products)}개 상품")
            return JSONResponse(response_data)
            
        finally:
            # 임시 파일 삭제
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
                
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"예상치 못한 오류: {e}")
        raise HTTPException(status_code=500, detail=f"서버 내부 오류: {str(e)}")

@app.get("/health")
async def health_check():
    """헬스 체크 엔드포인트"""
    return {"status": "healthy", "message": "서버가 정상적으로 실행 중입니다"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
