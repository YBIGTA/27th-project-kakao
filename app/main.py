
import argparse, json, os
import logging
from app.core.pipeline import run_pipeline

def setup_logging():
    """로깅 설정"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def main():
    """메인 함수"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        parser = argparse.ArgumentParser(description="카카오톡 대화내역 기반 개인 맞춤형 선물 추천 서비스 (LangGraph)")
        parser.add_argument("--chat_csv", required=True, help="카카오톡 대화 CSV 파일 경로")
        parser.add_argument("--age", type=int, required=True, help="선물 받는 상대방의 나이")
        parser.add_argument("--gender", required=True, choices=["남성","여성","기타","male","female","other"], help="선물 받는 상대방의 성별")
        parser.add_argument("--relation", required=True, help="상대방과의 관계 (예: 친구, 연인, 가족, 동료 등)")
        parser.add_argument("--budget_min", type=int, required=True, help="최소 예산 (원)")
        parser.add_argument("--budget_max", type=int, required=True, help="최대 예산 (원)")
        parser.add_argument("--products_csv_path", default="kakao_gifts.normalized.csv", help="상품 데이터베이스 CSV 파일 경로")
        parser.add_argument("--target_user", default="default", help="분석할 특정 사용자명 (기본값: default)")
        
        args = parser.parse_args()
        
        # 입력 검증
        if args.age <= 0 or args.age > 120:
            raise ValueError("나이는 1-120 사이여야 합니다")
        if args.budget_min < 0 or args.budget_max < 0:
            raise ValueError("예산은 0 이상이어야 합니다")
        if args.budget_min > args.budget_max:
            raise ValueError("최소 예산은 최대 예산보다 작아야 합니다")
        if not os.path.exists(args.chat_csv):
            raise FileNotFoundError(f"대화 CSV 파일을 찾을 수 없습니다: {args.chat_csv}")
        if not os.path.exists(args.products_csv_path):
            raise FileNotFoundError(f"상품 CSV 파일을 찾을 수 없습니다: {args.products_csv_path}")
            
        logger.info("=== 카카오톡 선물 추천 서비스 시작 ===")
        logger.info(f"대화 파일: {args.chat_csv}")
        logger.info(f"분석 대상 사용자: {args.target_user}")
        logger.info(f"상대방 정보: {args.age}세, {args.gender}, {args.relation}")
        logger.info(f"예산 범위: {args.budget_min:,}원 ~ {args.budget_max:,}원")
        logger.info(f"상품 데이터: {args.products_csv_path}")
        
        # 프로필 구성
        profile = {
            "age": args.age,
            "gender": args.gender,
            "relation": args.relation,
            "budget_min": args.budget_min,
            "budget_max": args.budget_max,
            "products_csv_path": args.products_csv_path,
            "target_user": args.target_user,
        }
        
        # 파이프라인 실행 (대화 CSV 경로와 프로필 전달)
        logger.info("선물 추천 파이프라인 실행 중...")
        result = run_pipeline(args.chat_csv, profile)
        
        # 결과 출력
        if "error" in result:
            logger.error(f"파이프라인 실행 실패: {result['error']}")
            print(json.dumps({"error": result["error"]}, ensure_ascii=False, indent=2))
            return 1
            
        selected_products = result.get("selected_products", [])
        if not selected_products:
            logger.warning("추천된 상품이 없습니다")
            print(json.dumps({"message": "추천된 상품이 없습니다"}, ensure_ascii=False, indent=2))
            return 0
            
        logger.info(f"추천 완료: {len(selected_products)}개 상품")
        
        # 전처리된 메시지 정보도 포함
        processed_messages = result.get("processed_messages", [])
        if processed_messages:
            high_relevance = sum(1 for msg in processed_messages if msg.get('gift_relevance_score', 0) > 0)
            logger.info(f"전처리된 메시지: {len(processed_messages)}개, 선물 관련성 높은 메시지: {high_relevance}개")
        
        # 결과를 사용자 친화적으로 출력
        output = {
            "message": "선물 추천이 완료되었습니다",
            "recommendations": selected_products,
            "summary": {
                "total_products": len(selected_products),
                "categories": list(set(p.get("category", "") for p in selected_products if p.get("category"))),
                "processed_messages": len(processed_messages) if processed_messages else 0,
                "target_user": args.target_user
            }
        }
        
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0
        
    except ValueError as e:
        logger.error(f"입력 오류: {e}")
        print(json.dumps({"error": f"입력 오류: {e}"}, ensure_ascii=False, indent=2))
        return 1
    except FileNotFoundError as e:
        logger.error(f"파일 오류: {e}")
        print(json.dumps({"error": f"파일 오류: {e}"}, ensure_ascii=False, indent=2))
        return 1
    except Exception as e:
        logger.error(f"예상치 못한 오류: {e}")
        print(json.dumps({"error": f"예상치 못한 오류: {e}"}, ensure_ascii=False, indent=2))
        return 1

if __name__ == "__main__":
    exit(main())
