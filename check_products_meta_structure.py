#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import psycopg2

def check_products_meta_structure():
    """products_meta 테이블 구조 확인"""
    try:
        print("🔍 products_meta 테이블 구조 확인 시작...")
        
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
        
        # 1. 테이블 존재 여부 확인
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'products_meta'
            );
        """)
        
        table_exists = cur.fetchone()[0]
        if not table_exists:
            print("❌ products_meta 테이블이 존재하지 않습니다.")
            return False
        
        print("✅ products_meta 테이블 발견!")
        
        # 2. 테이블 구조 확인
        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = 'products_meta'
            ORDER BY ordinal_position;
        """)
        
        columns = cur.fetchall()
        print(f"\n📊 테이블 구조 ({len(columns)}개 컬럼):")
        for col_name, data_type, is_nullable, col_default in columns:
            default_str = f" DEFAULT {col_default}" if col_default else ""
            print(f"  - {col_name}: {data_type} ({'NULL' if is_nullable == 'YES' else 'NOT NULL'}){default_str}")
        
        # 3. 샘플 데이터 확인
        cur.execute("SELECT COUNT(*) FROM products_meta;")
        row_count = cur.fetchone()[0]
        print(f"\n📈 테이블 행 수: {row_count:,}개")
        
        if row_count > 0:
            cur.execute("SELECT * FROM products_meta LIMIT 3;")
            sample_data = cur.fetchall()
            print(f"\n📋 샘플 데이터 (3개):")
            for i, row in enumerate(sample_data, 1):
                print(f"  {i}. {row}")
        
        # 4. 실제 데이터 타입 확인
        print(f"\n🔍 실제 데이터 타입 확인:")
        for col_name, data_type, _, _ in columns:
            if data_type in ['character varying', 'text']:
                cur.execute(f"SELECT DISTINCT {col_name} FROM products_meta WHERE {col_name} IS NOT NULL LIMIT 5;")
                sample_values = cur.fetchall()
                if sample_values:
                    print(f"  {col_name} 샘플값: {[val[0] for val in sample_values]}")
        
        # 연결 종료
        cur.close()
        conn.close()
        print("\n✅ 테이블 구조 확인 완료!")
        return True
        
    except Exception as e:
        print(f"❌ 테이블 구조 확인 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = check_products_meta_structure()
    if success:
        print("\n🎉 테이블 구조 확인 성공!")
    else:
        print("\n💥 테이블 구조 확인 실패!")
