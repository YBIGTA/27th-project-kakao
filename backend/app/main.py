from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import os
import json
from pydantic import BaseModel, Field, validator
from core.pipeline import PipelineEngine
from config.settings import PORT

# 파이프라인 엔진 초기화
engine = PipelineEngine()

app = FastAPI(
    title="Gift Recommendation API",
    description="카카오 선물하기 기반 선물 추천 API",
    version="1.0.0"
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class RecommendationRequest(BaseModel):
    age: int = Field(..., ge=1, le=120, description="사용자 나이 (1-120)")
    gender: str = Field(..., regex="^[MF]$", description="성별 (M/F)")
    relation: str = Field(..., description="관계")
    budget_min: int = Field(..., ge=0, description="최소 예산")
    budget_max: int = Field(..., ge=0, description="최대 예산")
    
    @validator('budget_max')
    def budget_max_must_be_greater(cls, v, values):
        if 'budget_min' in values and v <= values['budget_min']:
            raise ValueError('최대 예산은 최소 예산보다 커야 합니다')
        return v

@app.get("/")
async def root():
    return {"message": "Gift Recommendation API v1.0.0"}

@app.post("/recommendations")
async def get_recommendations(
    file: UploadFile = File(...),
    age: int = Form(...),
    gender: str = Form(...),
    relation: str = Form(...),
    budget_min: int = Form(...),
    budget_max: int = Form(...)
):
    """
    선물 추천 API
    """
    try:
        # 파일 검증
        if not file.filename:
            raise HTTPException(status_code=400, detail="파일이 필요합니다.")
        
        if not file.filename.lower().endswith(('.txt', '.csv')):
            raise HTTPException(status_code=400, detail="지원하지 않는 파일 형식입니다. .txt 또는 .csv 파일을 업로드해주세요.")
        
        # 파일 크기 제한 (10MB)
        file_bytes = await file.read()
        if len(file_bytes) > 10 * 1024 * 1024:
            raise HTTPException(status_code=400, detail="파일 크기가 너무 큽니다. 10MB 이하의 파일을 업로드해주세요.")
        
        # Pydantic 모델로 사용자 입력 검증
        try:
            request = RecommendationRequest(
                age=age,
                gender=gender,
                relation=relation,
                budget_min=budget_min,
                budget_max=budget_max
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        
        # 파이프라인 실행
        result = await engine.run(
            file_bytes=file_bytes,
            filename=file.filename,
            age=request.age,
            gender=request.gender,
            relation=request.relation,
            budget_min=request.budget_min,
            budget_max=request.budget_max
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="서버 내부 오류가 발생했습니다.")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=True)


