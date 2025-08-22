import argparse, json, asyncio
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

from core.pipeline import run_pipeline
from core.state import GiftContext

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--messages", type=str, required=True, 
                   help="Preprocessed CSV with columns: Date,User,Message (카카오톡 대화 내역)")
    ap.add_argument("--age", type=int, default=24)
    ap.add_argument("--gender", type=str, default="F")
    ap.add_argument("--relation", type=str, default="연인")
    ap.add_argument("--budget-min", type=int, default=30000)
    ap.add_argument("--budget-max", type=int, default=40000)
    return ap.parse_args()

def main():
    load_dotenv()
    args = parse_args()

    # 대화 내역 CSV 파일 읽기
    messages_path = Path(args.messages)
    if not messages_path.exists():
        raise FileNotFoundError(f"--messages file not found: {messages_path.resolve()}")

    df = pd.read_csv(messages_path)
    required = {"Date","User","Message"}
    if not required.issubset(df.columns):
        raise ValueError(f"--messages CSV must have columns {required}, got {set(df.columns)}")

    # 대화 내역을 파이프라인 입력 형식으로 변환
    rows = [{"idx": i, "date": r["Date"], "user": r["User"], "text": r["Message"]} 
            for i, (_, r) in enumerate(df.iterrows())]

    # 사용자 컨텍스트 생성
    ctx = GiftContext(
        age=args.age, gender=args.gender, relation=args.relation,
        budget_min=args.budget_min, budget_max=args.budget_max
    )

    # PostgreSQL에서 상품 데이터를 가져오도록 products_csv=None 전달
    result = asyncio.run(run_pipeline(rows, ctx, products_csv=None))
    print(json.dumps(result, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
