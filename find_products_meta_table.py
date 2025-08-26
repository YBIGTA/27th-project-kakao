#!/usr/bin/env python3
# -*- coding: utf-8 -*-

def find_products_meta_table():
    """products_meta 테이블 찾기 및 구조 확인"""
    try:
        print("🔍 products_meta 테이블 찾기 시작")
        
        # 환경 변수 확인
        import os
        postgres_dsn = os.getenv('POSTGRES_DSN')
        print(f"\n📋 연결 정보:")
        print(f"  DSN: {postgres_dsn}")
        
        if not postgres_dsn:
            print("❌ POSTGRES_DSN 환경 변수가 설정되지 않음")
            return
        
        # PostgreSQL 연결
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            
            print(f"\n🐘 PostgreSQL 연결 중...")
            conn = psycopg2.connect(postgres_dsn)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            print(f"✅ 연결 성공!")
            
            # 1. 모든 테이블 목록 조회
            print(f"\n📋 데이터베이스의 모든 테이블:")
            cur.execute("""
                SELECT table_name, table_type
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name;
            """)
            
            tables = cur.fetchall()
            if tables:
                for table in tables:
                    print(f"  {table['table_name']} ({table['table_type']})")
            else:
                print("  테이블이 없습니다.")
            
            # 2. products_meta 테이블 상세 정보
            print(f"\n🔍 products_meta 테이블 상세 정보:")
            cur.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_name = 'products_meta'
                ORDER BY ordinal_position;
            """)
            
            columns = cur.fetchall()
            if columns:
                print(f"  컬럼 수: {len(columns)}")
                for col in columns:
                    nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                    default = f" DEFAULT {col['column_default']}" if col['column_default'] else ""
                    print(f"    {col['column_name']}: {col['data_type']} {nullable}{default}")
            else:
                print("  products_meta 테이블이 존재하지 않습니다.")
            
            # 3. products_meta 테이블 데이터 샘플
            print(f"\n📊 products_meta 테이블 데이터 샘플:")
            try:
                cur.execute("SELECT * FROM products_meta LIMIT 5;")
                rows = cur.fetchall()
                if rows:
                    print(f"  첫 5개 행:")
                    for i, row in enumerate(rows, 1):
                        print(f"    {i}. {dict(row)}")
                else:
                    print("  데이터가 없습니다.")
            except Exception as e:
                print(f"  데이터 조회 실패: {e}")
            
            # 4. 테이블 크기 정보 (별도 연결로 실행)
            print(f"\n📏 테이블 크기 정보:")
            try:
                # 새로운 연결로 통계 조회
                conn_stats = psycopg2.connect(postgres_dsn)
                cur_stats = conn_stats.cursor(cursor_factory=RealDictCursor)
                
                cur_stats.execute("""
                    SELECT 
                        schemaname,
                        relname as tablename,
                        n_tup_ins as inserts,
                        n_tup_upd as updates,
                        n_tup_del as deletes,
                        n_live_tup as live_rows,
                        n_dead_tup as dead_rows
                    FROM pg_stat_user_tables 
                    WHERE relname = 'products_meta';
                """)
                
                stats = cur_stats.fetchone()
                if stats:
                    print(f"  스키마: {stats['schemaname']}")
                    print(f"  테이블명: {stats['tablename']}")
                    print(f"  삽입: {stats['inserts']}")
                    print(f"  업데이트: {stats['updates']}")
                    print(f"  삭제: {stats['deletes']}")
                    print(f"  활성 행: {stats['live_rows']}")
                    print(f"  죽은 행: {stats['dead_rows']}")
                else:
                    print("  통계 정보를 찾을 수 없습니다.")
                
                cur_stats.close()
                conn_stats.close()
                
            except Exception as e:
                print(f"  통계 조회 실패: {e}")
            
            # 5. 인덱스 정보 (별도 연결로 실행)
            print(f"\n🔍 인덱스 정보:")
            try:
                # 새로운 연결로 인덱스 조회
                conn_idx = psycopg2.connect(postgres_dsn)
                cur_idx = conn_idx.cursor(cursor_factory=RealDictCursor)
                
                cur_idx.execute("""
                    SELECT 
                        indexname,
                        indexdef
                    FROM pg_indexes 
                    WHERE tablename = 'products_meta';
                """)
                
                indexes = cur_idx.fetchall()
                if indexes:
                    for idx in indexes:
                        print(f"  {idx['indexname']}: {idx['indexdef']}")
                else:
                    print("  인덱스가 없습니다.")
                
                cur_idx.close()
                conn_idx.close()
                
            except Exception as e:
                print(f"  인덱스 조회 실패: {e}")
            
            cur.close()
            conn.close()
            
            print(f"\n✅ products_meta 테이블 조사 완료!")
            
        except ImportError:
            print("❌ psycopg2가 설치되지 않음")
        except Exception as e:
            print(f"❌ PostgreSQL 연결 실패: {e}")
            import traceback
            traceback.print_exc()
        
    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    find_products_meta_table()
