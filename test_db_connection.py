#!/usr/bin/env python3
# -*- coding: utf-8 -*-

def test_db_connection():
    """DB 연결 상태 테스트"""
    try:
        print("🔍 DB 연결 상태 테스트 시작")
        
        # 1. 환경 변수 확인
        import os
        print(f"\n📋 환경 변수:")
        print(f"  POSTGRES_DSN: {os.getenv('POSTGRES_DSN', '설정되지 않음')}")
        print(f"  PRODUCTS_CSV_PATH: {os.getenv('PRODUCTS_CSV_PATH', '설정되지 않음')}")
        
        # 2. 설정 확인
        from app.core.config import REPO
        print(f"\n⚙️ 설정값:")
        print(f"  postgres_dsn: {REPO.postgres_dsn}")
        print(f"  csv_path: {REPO.csv_path}")
        
        # 3. CSV 파일 존재 확인
        import os
        csv_exists = os.path.exists(REPO.csv_path)
        print(f"\n📁 CSV 파일:")
        print(f"  경로: {REPO.csv_path}")
        print(f"  존재: {'✅ 있음' if csv_exists else '❌ 없음'}")
        
        if csv_exists:
            import pandas as pd
            try:
                df = pd.read_csv(REPO.csv_path)
                print(f"  행 수: {len(df)}")
                print(f"  열 수: {len(df.columns)}")
                print(f"  열명: {list(df.columns)}")
            except Exception as e:
                print(f"  로드 실패: {e}")
        
        # 4. PostgreSQL 연결 테스트
        print(f"\n🐘 PostgreSQL 연결:")
        try:
            import psycopg2
            print(f"  psycopg2: ✅ 설치됨")
            
            if REPO.postgres_dsn:
                try:
                    conn = psycopg2.connect(REPO.postgres_dsn)
                    cur = conn.cursor()
                    
                    # 테이블 존재 여부 확인
                    cur.execute("""
                        SELECT table_name, table_type 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name IN ('products', 'products_meta')
                        ORDER BY table_name;
                    """)
                    
                    tables = cur.fetchall()
                    cur.close()
                    conn.close()
                    
                    print(f"  연결: ✅ 성공")
                    print(f"  테이블 목록:")
                    for table_name, table_type in tables:
                        print(f"    - {table_name} ({table_type})")
                    
                    if not tables:
                        print(f"    - products, products_meta 테이블 모두 없음")
                    
                except Exception as e:
                    print(f"  연결: ❌ 실패 - {e}")
            else:
                print(f"  연결: ⚠️ DSN이 설정되지 않음")
                
        except ImportError:
            print(f"  psycopg2: ❌ 설치되지 않음")
        
        # 5. ProductRepo 테스트
        print(f"\n🏪 ProductRepo 테스트:")
        try:
            from app.services.repo.product_repo import ProductRepo
            
            # CSV fallback으로 테스트
            repo = ProductRepo(csv_path=REPO.csv_path)
            print(f"  초기화: ✅ 성공")
            
            # 상품 조회 테스트
            try:
                candidates = repo.query_candidates_csv(
                    child_categories=["와인", "카페"],
                    budget_min=10000,
                    budget_max=50000
                )
                print(f"  CSV 조회: ✅ 성공 ({len(candidates)}개 상품)")
                
                if candidates:
                    print(f"  첫 번째 상품: {candidates[0]}")
                    
            except Exception as e:
                print(f"  CSV 조회: ❌ 실패 - {e}")
                
        except Exception as e:
            print(f"  초기화: ❌ 실패 - {e}")
        
        print(f"\n✅ DB 연결 테스트 완료!")
        
    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_db_connection()
