
import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class LLMConfig:
    provider: str = os.getenv("LLM_PROVIDER", "upstage")  # "upstage" (OpenAI-compatible), or "openai"
    base_url: str = os.getenv("UPSTAGE_BASE_URL", "https://api.upstage.ai/v1")
    api_key: str = os.getenv("UPSTAGE_API_KEY", "")
    model: str = os.getenv("UPSTAGE_MODEL", "solar-pro2")
    request_timeout: int = int(os.getenv("LLM_TIMEOUT", "60"))

@dataclass
class ScoreWeights:
    # Hierarchy combination weights
    beta_child: float = float(os.getenv("BETA_CHILD", "0.6"))
    gamma_parent: float = float(os.getenv("GAMMA_PARENT", "0.4"))
    # Single-child penalty (encourage diversity)
    single_child_penalty_lambda: float = float(os.getenv("SINGLE_CHILD_PENALTY_LAMBDA", "0.1"))

@dataclass
class SoftmaxConfig:
    temperature: float = float(os.getenv("SOFTMAX_TEMPERATURE", "0.9"))
    clamp_min: float = float(os.getenv("SCORE_CLAMP_MIN", "0.0"))
    clamp_max: float = float(os.getenv("SCORE_CLAMP_MAX", "1.0"))
    # 엔트로피 타깃 기반 자동 온도 튜닝
    entropy_target_parent: float = float(os.getenv("ENTROPY_TARGET_PARENT", "2.0"))  # 상위 카테고리 엔트로피 타깃
    entropy_target_child: float = float(os.getenv("ENTROPY_TARGET_CHILD", "3.0"))   # 하위 카테고리 엔트로피 타깃

@dataclass
class RepoConfig:
    # If provided, use PostgreSQL. Otherwise, fallback to CSV path
    postgres_dsn: Optional[str] = os.getenv("POSTGRES_DSN")  # e.g., "postgresql://user:pass@host:5432/dbname"
    csv_path: str = os.getenv("PRODUCTS_CSV_PATH", "kakao_gifts.normalized.csv")

@dataclass
class RuntimeConfig:
    # Concurrency knobs (used by orchestrator or external runners)
    max_concurrency: int = int(os.getenv("MAX_CONCURRENCY", "8"))

LLM = LLMConfig()
WEIGHTS = ScoreWeights()
SOFTMAX = SoftmaxConfig()
REPO = RepoConfig()
RUNTIME = RuntimeConfig()
