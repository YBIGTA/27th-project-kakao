#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
카카오톡 대화내역 파일을 전처리하고 LangGraph 파이프라인에 전달하는 통합 실행 파일
"""

import argparse
import json
import asyncio
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import tempfile
import os
import sys
from datetime import datetime

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# 절대 import 사용
from preprocess.main_processor import main as preprocess_main
from core.pipeline import run_pipeline
from core.state import GiftContext

class DateTimeEncoder(json.JSONEncoder):
    """datetime 객체를 JSON 직렬화할 수 있도록 하는 커스텀 엔코더"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def parse_args():
    """명령줄 인자를 파싱합니다."""
    ap = argparse.ArgumentParser(description='카카오톡 대화내역을 전처리하고 선물 추천 파이프라인을 실행합니다.')
    
    ap.add_argument("--input", type=str, required=True, 
                   help="카카오톡 대화내역 파일 경로 (CSV 또는 TXT)")
    ap.add_argument("--user", type=str, required=True,
                   help="대화 상대 이름 (예: '친구A', '여자친구')")
    ap.add_argument("--age", type=int, default=24,
                   help="대화 상대 나이 (기본값: 24)")
    ap.add_argument("--gender", type=str, default="F",
                   help="대화 상대 성별 F/M (기본값: F)")
    ap.add_argument("--relation", type=str, default="연인",
                   help="대화 상대와의 관계 (기본값: 연인)")
    ap.add_argument("--budget-min", type=int, default=30000,
                   help="예산 하한 (기본값: 30000)")
    ap.add_argument("--budget-max", type=int, default=40000,
                   help="예산 상한 (기본값: 40000)")
    ap.add_argument("--output-dir", type=str, default=None,
                   help="전처리 결과 저장 디렉토리 (기본값: 임시 디렉토리)")
    ap.add_argument("--keep-temp", action="store_true",
                   help="임시 파일 유지 (디버깅용)")
    
    return ap.parse_args()

def validate_input_file(file_path: str) -> bool:
    """입력 파일의 유효성을 검사합니다."""
    path = Path(file_path)
    
    if not path.exists():
        print(f"❌ 입력 파일을 찾을 수 없습니다: {file_path}")
        return False
    
    # 지원하는 파일 형식 확인
    supported_extensions = {'.csv', '.txt'}
    if path.suffix.lower() not in supported_extensions:
        print(f"❌ 지원하지 않는 파일 형식입니다: {path.suffix}")
        print(f"   지원 형식: {', '.join(supported_extensions)}")
        return False
    
    print(f"✅ 입력 파일 확인됨: {file_path}")
    return True

def run_preprocess(input_file: str, user_name: str, output_dir: str = None) -> str:
    """전처리 파이프라인을 실행합니다."""
    print(f"\n🔄 전처리 파이프라인 시작...")
    print(f"📁 입력 파일: {input_file}")
    print(f"👤 대상 사용자: {user_name}")
    
    try:
        # 전처리 실행
        processed_csv_path = preprocess_main(
            input_file_path=input_file,
            output_dir=output_dir,
            user_name=user_name
        )
        
        print(f"✅ 전처리 완료: {processed_csv_path}")
        return processed_csv_path
        
    except Exception as e:
        print(f"❌ 전처리 실패: {e}")
        raise

def validate_processed_csv(csv_path: str) -> pd.DataFrame:
    """전처리된 CSV 파일을 검증하고 읽습니다."""
    print(f"\n🔍 전처리된 CSV 파일 검증 중...")
    
    try:
        df = pd.read_csv(csv_path)
        print(f"📊 CSV 구조: {df.shape[0]}행 x {df.shape[1]}열")
        print(f"📋 컬럼: {list(df.columns)}")
        
        # 필수 컬럼 확인
        required_columns = {'date', 'user', 'message'}
        if not required_columns.issubset(set(df.columns)):
            missing = required_columns - set(df.columns)
            raise ValueError(f"필수 컬럼이 누락되었습니다: {missing}")
        
        # 데이터 샘플 출력
        print(f"\n📝 데이터 샘플:")
        for i, (_, row) in enumerate(df.head(3).iterrows()):
            print(f"  {i+1}: {row['date']} | {row['user']} | {row['message'][:50]}...")
        
        return df
        
    except Exception as e:
        print(f"❌ CSV 검증 실패: {e}")
        raise

def prepare_pipeline_input(df: pd.DataFrame) -> list:
    """LangGraph 파이프라인 입력 형식으로 데이터를 변환합니다."""
    print(f"\n🔄 파이프라인 입력 데이터 준비 중...")
    
    rows = []
    for i, (_, row) in enumerate(df.iterrows()):
        row_data = {
            "idx": i,
            "date": row["date"],
            "user": row["user"],
            "text": row["message"]
        }
        rows.append(row_data)
        
        # 디버깅: 첫 번째 행 출력
        if i == 0:
            print(f"   🔍 첫 번째 행 데이터: {row_data}")
    
    print(f"✅ {len(rows)}개 메시지 준비 완료")
    return rows

async def run_langgraph_pipeline(rows: list, ctx: GiftContext) -> dict:
    """LangGraph 파이프라인을 실행합니다."""
    print(f"\n🚀 LangGraph 파이프라인 실행 중...")
    
    try:
        # PostgreSQL에서 상품 데이터를 가져오도록 products_csv=None 전달
        result = await run_pipeline(rows, ctx, products_csv=None)
        print(f"✅ 파이프라인 실행 완료")
        return result
        
    except Exception as e:
        print(f"❌ 파이프라인 실행 실패: {e}")
        raise

def main():
    """메인 실행 함수"""
    load_dotenv()
    args = parse_args()
    
    temp_files = []  # 정리할 임시 파일들
    
    try:
        print("🎁 카카오톡 선물 추천 시스템")
        print("=" * 50)
        
        # 1. 입력 파일 검증
        if not validate_input_file(args.input):
            return 1
        
        # 2. 전처리 실행
        processed_csv = run_preprocess(
            input_file=args.input,
            user_name=args.user,
            output_dir=args.output_dir
        )
        
        if args.output_dir is None:
            temp_files.append(processed_csv)
        
        # 3. 전처리된 CSV 검증
        df = validate_processed_csv(processed_csv)
        
        # 4. 파이프라인 입력 데이터 준비
        rows = prepare_pipeline_input(df)
        
        # 5. 사용자 컨텍스트 생성
        ctx = GiftContext(
            age=args.age,
            gender=args.gender,
            relation=args.relation,
            budget_min=args.budget_min,
            budget_max=args.budget_max
        )
        
        print(f"\n👤 사용자 컨텍스트:")
        print(f"   나이: {ctx.age}세")
        print(f"   성별: {ctx.gender}")
        print(f"   관계: {ctx.relation}")
        print(f"   예산: {ctx.budget_min:,}원 ~ {ctx.budget_max:,}원")
        
        # 6. LangGraph 파이프라인 실행
        result = asyncio.run(run_langgraph_pipeline(rows, ctx))
        
        # 7. 결과 출력
        print(f"\n🎯 최종 결과:")
        
        # 실제 result 구조를 확인하기 위한 디버깅
        print(f"\n🔍 Result 구조 디버깅:")
        print(f"   result.keys(): {list(result.keys())}")
        
        # README 형식에 맞춘 최종 결과물 출력
        selected_products = None
        rationales = {}
        
        # 실제 구조에 맞게 접근
        if "data" in result and "products" in result["data"]:
            products_data = result["data"]["products"]
            selected_products = products_data.get("selected_products", [])
            rationales = products_data.get("rationales", {})
            print(f"   ✅ result.data.products에서 데이터 발견")
        elif "result" in result and "products" in result["result"]:
            products_data = result["result"]["products"]
            selected_products = products_data.get("selected_products", [])
            rationales = products_data.get("rationales", {})
            print(f"   ✅ result.result.products에서 데이터 발견")
        elif "products" in result:
            products_data = result["products"]
            selected_products = products_data.get("selected_products", [])
            rationales = products_data.get("rationales", {})
            print(f"   ✅ result.products에서 데이터 발견")
        else:
            print(f"   ⚠️ 예상하지 못한 result 구조입니다.")
            print(f"   🔍 사용 가능한 키들: {list(result.keys())}")
            # data 키가 있으면 그 안의 키들도 확인
            if "data" in result:
                print(f"   📁 result.data의 키들: {list(result['data'].keys()) if isinstance(result['data'], dict) else 'dict가 아님'}")
        
        if selected_products:
            print(f"\n🎁 추천 상품 ({len(selected_products)}개):")
            print("=" * 80)
            
            for i, product in enumerate(selected_products, 1):
                print(f"\n{i}. {product.get('title', '제목 없음')}")
                print(f"   💰 가격: {product.get('price', 0):,}원")
                print(f"   🏷️ 브랜드: {product.get('brand', '브랜드 없음')}")
                
                # 🚀 README 요구사항: 상품 URL 필수 포함
                product_url = product.get('product_url') or product.get('url', 'URL 없음')
                print(f"   🔗 상품 URL: {product_url}")
                
                # 🚀 README 요구사항: 자연어로 표현된 추천 근거 필수 포함
                product_id = product.get('id', '')
                if product_id in rationales:
                    rationale = rationales[product_id]
                    print(f"   💡 추천 근거:")
                    # 긴 텍스트는 가독성을 위해 줄바꿈 처리
                    if len(rationale) > 80:
                        # 80자마다 줄바꿈
                        wrapped_rationale = '\n      '.join([rationale[j:j+80] for j in range(0, len(rationale), 80)])
                        print(f"      {wrapped_rationale}")
                    else:
                        print(f"      {rationale}")
                else:
                    print(f"   💡 추천 근거: 정보 없음")
                
                # 추가 상품 정보
                print(f"   ⭐ 만족도: {product.get('satisfaction_pct', 0)}%")
                print(f"   📝 리뷰: {product.get('review_count', 0)}개")
                print(f"   ❤️ 찜: {product.get('wish_count', 0):,}개")
                
                # 카테고리 정보
                if 'category_child' in product:
                    print(f"   🏷️ 카테고리: {product['category_child']}")
                
                print("-" * 60)
        else:
            print(f"❌ 추천 상품을 찾을 수 없습니다.")
            print(f"   🔍 가능한 원인:")
            print(f"      - DB 연결 문제")
            print(f"      - 상품 데이터 부족")
            print(f"      - 예산 범위에 맞는 상품 없음")
            print(f"      - 파이프라인 실행 오류")
        
        # 간략한 시스템 정보
        print(f"\n📊 시스템 정보:")
        
        # 카테고리 정보
        categories = []
        if "data" in result and "categories" in result["data"]:
            categories = result["data"]["categories"].get("top3_selection", [])
        elif "result" in result and "categories" in result["result"]:
            categories = result["result"]["categories"].get("top3_selection", [])
        
        if categories:
            print(f"   🎯 추천 카테고리: {', '.join(categories)}")
        
        # 후보 상품 수 정보
        debug_info = result.get("debug", {})
        if "candidate_count" in debug_info:
            print(f"   📦 후보 상품 수: {debug_info['candidate_count']}개")
        
        print(f"\n✅ 추천 완료!")
        
        # 전체 JSON은 --debug 옵션이 있을 때만 출력
        if args.keep_temp:  # --keep-temp을 디버그 용도로 재활용
            print(f"\n📊 전체 시스템 결과 (JSON):")
            print(json.dumps(result, ensure_ascii=False, indent=2, cls=DateTimeEncoder))
        
        return 0
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        return 1
        
    finally:
        # 임시 파일 정리
        if not args.keep_temp:
            for temp_file in temp_files:
                if os.path.exists(temp_file):
                    try:
                        os.unlink(temp_file)
                        print(f"🧹 임시 파일 정리: {temp_file}")
                    except Exception as e:
                        print(f"⚠️ 임시 파일 정리 실패: {e}")

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
