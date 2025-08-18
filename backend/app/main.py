from enum import Enum
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from pydantic import BaseModel, Field
from .pipeline import PipelineEngine

app = FastAPI(title="Gift Recommender API", version="4.0.0")
engine = PipelineEngine()


@app.get("/health")
async def health():
    return {"status": "ok"}

# 스키마 
class Gender(str, Enum):
    남 = "남"
    여 = "여"

class Profile(BaseModel):
    age: int = Field(..., ge=0, le=120)
    gender: Gender
    relation: str
    budget_min: int = Field(..., ge=0)
    budget_max: int = Field(..., ge=0)

class Analysis(BaseModel):
    subcats: List[str]                         # 0~3개
    evidence_by_cat: Dict[str, List[str]]      # {cat: [문장*최대3]}
    message: Optional[str] = None              # 카테고리가 없을 때 메시지

class Selection(BaseModel):
    sub_category: str
    product_name: str
    product_url: str | None = None
    brand: str | None = None
    price: int | None = None
    reason: str | None = None

class RecommendResponse(BaseModel):
    profile: Profile
    analysis: Analysis
    selections: List[Selection]                # 카테고리별 1개씩 (카테고리가 없으면 빈 리스트)

# 엔드포인트 
@app.post("/v1/recommendations", response_model=RecommendResponse)
async def recommend(
    file: UploadFile = File(...),          # .txt 또는 .csv
    age: int = Form(...),
    gender: Gender = Form(...),            # 남/여만 허용
    relation: str = Form(...),
    budget_min: int = Form(...),
    budget_max: int = Form(...),
):
    if budget_min > budget_max:
        raise HTTPException(status_code=400, detail="예산 범위가 잘못되었습니다.")

    file_bytes = await file.read()
    filename = file.filename or "upload"

    try:
        out = await engine.run(
            file_bytes=file_bytes,
            filename=filename,
            age=age,
            gender=gender.value,        # 문자열로 전달
            relation=relation,
            budget_min=budget_min,
            budget_max=budget_max,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception:
        raise HTTPException(status_code=500, detail="내부 오류")

    return RecommendResponse(
        profile=Profile(age=age, gender=gender, relation=relation, budget_min=budget_min, budget_max=budget_max),
        analysis=Analysis(**out["analysis"]),
        selections=[Selection(**x) for x in out["selections"]],
    )
