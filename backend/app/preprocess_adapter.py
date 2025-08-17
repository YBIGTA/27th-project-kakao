"""
팀 전처리 모듈(preprocess 폴더)의 함수를 백엔드에서 유연하게 호출하기 위한 어댑터.

- TXT: TextProcessor 클래스 사용
- CSV: CSVProcessor 클래스 사용
위 클래스들 중 존재하는 것을 우선 호출. 없으면 간단 폴백 파서 사용.
"""

from typing import List, Callable, Optional
import importlib, re, csv, io
from datetime import datetime, timedelta
from pathlib import Path

def _try_load_class(class_path: str) -> Optional[Callable]:
    mod_path, class_name = class_path.rsplit(".", 1)
    try:
        mod = importlib.import_module(mod_path)
        cls = getattr(mod, class_name, None)
        if cls and callable(cls):
            return cls
    except Exception:
        pass
    return None

_TXT_CLASSES = [
    "app.preprocess.text_processor.TextProcessor",
]

_CSV_CLASSES = [
    "app.preprocess.csv_processor.CSVProcessor",
]

def preprocess_any(file_bytes: bytes, filename: str | None, recent_days: int = 90) -> List[str]:
    """
    파일 bytes를 받아서 전처리 후 문장 리스트를 반환합니다.
    
    Args:
        file_bytes: 업로드된 파일의 bytes
        filename: 파일명 (확장자로 파일 타입 판단)
        recent_days: 최근 N일 필터링 (기본값: 90일)
    
    Returns:
        List[str]: 전처리된 문장들의 리스트 (GPU에 전달할 형태)
    """
    ext = (filename or "").lower()
    
    # ── CSV ───────────────────────────────────────────────────────────────────────
    if ext.endswith(".csv"):
        for class_path in _CSV_CLASSES:
            cls = _try_load_class(class_path)
            if cls:
                try:
                    # CSVProcessor가 bytes를 받을 수 있도록 수정 필요
                    processor = cls.from_bytes(file_bytes, user_name="default")
                    result = processor.process()
                    
                    # Dict 리스트에서 message 필드만 추출
                    if isinstance(result, dict) and 'data' in result:
                        messages = []
                        for item in result['data']:
                            if item.get('message'):
                                messages.append(str(item['message']))
                        return messages
                    elif isinstance(result, list):
                        messages = []
                        for item in result:
                            if item.get('message'):
                                messages.append(str(item['message']))
                        return messages
                        
                except Exception as e:
                    print(f"CSV 처리 실패 ({class_path}): {e}")
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
    for class_path in _TXT_CLASSES:
        cls = _try_load_class(class_path)
        if cls:
            try:
                # TextProcessor가 bytes를 받을 수 있도록 수정 필요
                processor = cls.from_bytes(file_bytes, output_dir="/tmp", user_name="default")
                result = processor.process()
                
                # Dict 리스트에서 message 필드만 추출
                if isinstance(result, dict) and 'data' in result:
                    messages = []
                    for item in result['data']:
                        if item.get('message'):
                            messages.append(str(item['message']))
                    return messages
                elif isinstance(result, list):
                    messages = []
                    for item in result:
                        if item.get('message'):
                            messages.append(str(item['message']))
                    return messages
                        
            except Exception as e:
                print(f"TXT 처리 실패 ({class_path}): {e}")
                continue

    # 폴백: 매우 단순한 카톡 .txt 파서
    raw_text = file_bytes.decode("utf-8", errors="ignore")
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
