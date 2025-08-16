from typing import List
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from pydantic import BaseModel, Field
from .rag import RAGEngine

app = FastAPI(title="Gift Recommender API", version="1.0.0")
engine = RAGEngine()

class Profile(BaseModel):
    age: int = Field(..., ge=0, le=120)
    relation: str
    budget_min: int = Field(..., ge=0)
    budget_max: int = Field(..., ge=0)

class Analysis(BaseModel):
    top3_subcats: List[str] = []
    evidence: List[str] = []

class CandidateOut(BaseModel):
    product_name: str
    product_url: str | None = None
    image_url: str | None = None
    price: int | None = None
    brand: str | None = None
    reason: str | None = None

class IngestResponse(BaseModel):
    profile: Profile
    analysis: Analysis
    results: List[CandidateOut]

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/ingest", response_model=IngestResponse)
async def ingest(
    file: UploadFile = File(...),
    age: int = Form(...),
    relation: str = Form(...),
    budget_min: int = Form(...),
    budget_max: int = Form(...),
):
    if budget_min > budget_max:
        raise HTTPException(status_code=400, detail="예산 범위가 잘못되었습니다.")
    if file.content_type not in (None, "text/plain"):
        raise HTTPException(status_code=400, detail="텍스트(.txt)만 허용됩니다.")

    raw_txt = (await file.read()).decode("utf-8", errors="ignore")
    try:
        out = await engine.run(
            raw_txt=raw_txt, age=age, relation=relation,
            budget_min=budget_min, budget_max=budget_max
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception:
        raise HTTPException(status_code=500, detail="내부 오류")

    return IngestResponse(
        profile=Profile(**out["profile"]),
        analysis=Analysis(**out["analysis"]),
        results=[CandidateOut(**x) for x in out["results"]],
    )
