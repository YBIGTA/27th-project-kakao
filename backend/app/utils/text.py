
import pandas as pd
from typing import List, Optional

def load_sentences_from_csv(path: str, text_col: Optional[str] = None) -> List[str]:
    df = pd.read_csv(path)
    # Try to infer text column
    if text_col is None:
        for cand in ["text", "content", "message", "msg", "utterance", "utter", "sent"]:
            if cand in df.columns:
                text_col = cand
                break
    if text_col is None:
        # fallback to first column
        text_col = df.columns[0]
    series = df[text_col].astype(str)
    # Deduplicate & strip
    sentences = [s.strip() for s in series.tolist() if isinstance(s, str) and s.strip()]
    return sentences[:2000]  # hard cap for safety
