#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import psycopg2
from psycopg2 import sql

def test_postgresql_connection():
    """PostgreSQL 연결 테스트"""
    try:
        print("🔌 PostgreSQL 연결 테스트 시작...")
        
        # 환경 변수에서 연결 정보 가져오기
        postgres_dsn = os.getenv("POSTGRES_DSN")
        if not postgres_dsn:
            print("❌ POSTGRES_DSN 환경 변수가 설정되지 않았습니다.")
            return False
        
        print(f"📡 연결 시도: {postgres_dsn[:50]}...")
        
        # 연결 시도
        conn = psycopg2.connect(postgres_dsn)
        print("✅ PostgreSQL 연결 성공!")
        
        # 커서 생성
        cur = conn.cursor()
        
        # 데이터베이스 정보 조회
        cur.execute("SELECT current_database(), current_user, version();")
        db_info = cur.fetchone()
        print(f"📊 데이터베이스 정보:")
        print(f"  - 데이터베이스: {db_info[0]}")
        print(f"  - 사용자: {db_info[1]}")
        print(f"  - 버전: {db_info[2][:50]}...")
        
        # 테이블 목록 조회
        cur.execute("""
            SELECT table_name, table_type 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cur.fetchall()
        
        print(f"\n📋 테이블 목록 ({len(tables)}개):")
        for table_name, table_type in tables:
            print(f"  - {table_name} ({table_type})")
        
        # products_meta 테이블이 있는지 확인
        products_meta_exists = any(table[0] == 'products_meta' for table in tables)
        if products_meta_exists:
            print("\n🎯 products_meta 테이블 발견!")
            
            # 테이블 구조 확인
            cur.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'products_meta'
                ORDER BY ordinal_position;
            """)
            columns = cur.fetchall()
            
            print(f"📊 products_meta 테이블 구조 ({len(columns)}개 컬럼):")
            for col_name, data_type, is_nullable in columns:
                print(f"  - {col_name}: {data_type} ({'NULL' if is_nullable == 'YES' else 'NOT NULL'})")
            
            # 샘플 데이터 확인
            cur.execute("SELECT COUNT(*) FROM products_meta;")
            row_count = cur.fetchone()[0]
            print(f"\n📈 products_meta 테이블 행 수: {row_count:,}개")
            
            if row_count > 0:
                cur.execute("SELECT * FROM products_meta LIMIT 3;")
                sample_data = cur.fetchall()
                print(f"\n📋 샘플 데이터 (3개):")
                for i, row in enumerate(sample_data, 1):
                    print(f"  {i}. {row[:5]}...")  # 처음 5개 컬럼만 표시
        
        # 연결 종료
        cur.close()
        conn.close()
        print("\n✅ PostgreSQL 연결 테스트 완료!")
        return True
        
    except psycopg2.OperationalError as e:
        print(f"❌ PostgreSQL 연결 실패 (연결 오류): {e}")
        return False
    except psycopg2.Error as e:
        print(f"❌ PostgreSQL 오류: {e}")
        return False
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_postgresql_connection()
    if success:
        print("\n🎉 PostgreSQL 연결 성공!")
    else:
        print("\n💥 PostgreSQL 연결 실패!")
