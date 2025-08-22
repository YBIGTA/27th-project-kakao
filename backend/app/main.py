from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import os
import json
from .pipeline import PipelineEngine

# 환경 변수에서 설정 가져오기
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "upstage")
LLM_MODEL = os.getenv("LLM_MODEL", "solar-1-mini-chat")

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
        # 파일 읽기
        file_bytes = await file.read()
        
        # 파이프라인 실행
        result = await engine.run(
            file_bytes=file_bytes,
            filename=file.filename,
            age=age,
            gender=gender,
            relation=relation,
            budget_min=budget_min,
            budget_max=budget_max
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
