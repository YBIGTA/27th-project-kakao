"""
팀 전처리 모듈(preprocess 폴더)의 함수를 백엔드에서 유연하게 호출하기 위한 어댑터.

- TXT:  preprocess.main_processor.process_txt / .run / processor.process_txt / text_processor.process_txt
- CSV:  preprocess.csv_processor.process_csv / processor.process_csv
위 함수들 중 존재하는 것을 우선 호출. 없으면 간단 폴백 파서 사용.
"""

from typing import List, Callable, Optional
import importlib, re, csv, io
from datetime import datetime, timedelta

def _try_load(fn_path: str) -> Optional[Callable]:
    mod_path, func_name = fn_path.rsplit(".", 1)
    try:
        mod = importlib.import_module(mod_path)
        fn = getattr(mod, func_name, None)
        if callable(fn):
            return fn
    except Exception:
        pass
    return None

_TXT_FUNCS = [
    "preprocess.main_processor.process_txt",
    "preprocess.main_processor.run",
    "preprocess.processor.process_txt",
    "preprocess.text_processor.process_txt",
]

_CSV_FUNCS = [
    "preprocess.csv_processor.process_csv",
    "preprocess.processor.process_csv",
]

def preprocess_any(file_bytes: bytes, filename: str | None, recent_days: int = 90) -> List[str]:
    ext = (filename or "").lower()
    # ── CSV ───────────────────────────────────────────────────────────────────────
    if ext.endswith(".csv"):
        for path in _CSV_FUNCS:
            fn = _try_load(path)
            if fn:
                try:
                    out = fn(file_bytes)  # 팀 구현체가 bytes를 받도록 가정
                    if isinstance(out, list):
                        return [str(x) for x in out if isinstance(x, (str, bytes))]
                except Exception:
                    continue
        # 폴백: CSV 단순 파싱
        try:
            buf = io.StringIO(file_bytes.decode("utf-8", errors="ignore"))
            reader = csv.DictReader(buf)
            out: List[str] = []
            for row in reader:
                for key in ("text", "message", "content", "msg"):
                    if key in row and row[key]:
                        out.append(str(row[key])[:500])
                        break
            if out:
                return out[:2000]
        except Exception:
            pass
        return []

    # ── TXT ───────────────────────────────────────────────────────────────────────
    raw_text = file_bytes.decode("utf-8", errors="ignore")
    for path in _TXT_FUNCS:
        fn = _try_load(path)
        if fn:
            try:
                out = fn(raw_text)
                if isinstance(out, list):
                    return [str(x) for x in out if isinstance(x, (str, bytes))]
            except Exception:
                continue

    # 폴백: 매우 단순한 카톡 .txt 파서
    SYS_PAT = re.compile(r"^=+|^저장된 메시지|^-----|^사진|^\[알림\]")
    MSG_PAT = re.compile(r"^\[(.+?)\]\s\[(\d{4}\.\s?\d{1,2}\.\s?\d{1,2}\.\s?\w+\s?\d{1,2}:\d{2})\]\s(.+)$")
    def _parse_dt(s: str):
        s = s.replace("오전","AM").replace("오후","PM").replace("  "," ")
        for fmt in ("%Y. %m. %d. %p %I:%M","%Y.%m.%d. %p %I:%M","%Y.%m.%d. %H:%M","%Y. %m. %d. %H:%M"):
            try: return datetime.strptime(s.strip(), fmt)
            except: pass
        return None

    lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
    cutoff = datetime.now() - timedelta(days=recent_days)
    out: List[str] = []
    for ln in lines:
        if SYS_PAT.search(ln): continue
        m = MSG_PAT.match(ln)
        if m:
            ts, text = m.group(2), m.group(3)
            dt = _parse_dt(ts)
            if dt and dt < cutoff: continue
            txt = re.sub(r"\s+"," ", text)
            txt = re.sub(r"(ㅋ|ㅎ|ㅠ|ㅜ){3,}", r"\1\1", txt)
            if len(txt) >= 2: out.append(txt[:500])
        else:
            if len(ln) >= 2: out.append(ln[:500])
    # dedupe + cap
    seen, deduped = set(), []
    for s in out:
        if s in seen: continue
        seen.add(s); deduped.append(s)
    return deduped[:2000]
