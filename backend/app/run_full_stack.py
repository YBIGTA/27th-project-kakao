#!/usr/bin/env python3
"""
카카오 선물 추천 서비스 전체 스택 실행 스크립트
백엔드 API 서버와 프론트엔드를 함께 실행합니다.
"""

import subprocess
import sys
import time
import os
import signal
import threading
from pathlib import Path

def run_backend():
    """백엔드 API 서버 실행"""
    print("🚀 백엔드 API 서버를 시작합니다...")
    try:
        subprocess.run([sys.executable, "main.py"], check=True)
    except KeyboardInterrupt:
        print("\n🛑 백엔드 서버가 중단되었습니다.")
    except Exception as e:
        print(f"❌ 백엔드 서버 실행 중 오류: {e}")

def run_frontend():
    """프론트엔드 React 앱 실행"""
    print("🎨 프론트엔드를 시작합니다...")
    frontend_dir = Path("../frontend_test")
    
    if not frontend_dir.exists():
        print("❌ frontend_test 디렉토리를 찾을 수 없습니다.")
        return
    
    try:
        # npm install 실행
        print("📦 의존성을 설치합니다...")
        subprocess.run(["npm", "install"], cwd=frontend_dir, check=True)
        
        # npm start 실행
        print("🌐 React 개발 서버를 시작합니다...")
        subprocess.run(["npm", "start"], cwd=frontend_dir, check=True)
    except KeyboardInterrupt:
        print("\n🛑 프론트엔드가 중단되었습니다.")
    except FileNotFoundError:
        print("❌ Node.js/npm이 설치되어 있지 않습니다. Node.js를 설치해주세요.")
    except Exception as e:
        print(f"❌ 프론트엔드 실행 중 오류: {e}")

def check_dependencies():
    """필요한 의존성 확인"""
    print("🔍 의존성을 확인합니다...")
    
    # Python 패키지 확인
    try:
        import fastapi
        import uvicorn
        print("✅ Python 의존성 확인 완료")
    except ImportError as e:
        print(f"❌ Python 의존성이 누락되었습니다: {e}")
        print("pip install -r requirements.txt를 실행해주세요.")
        return False
    
    # Node.js 확인
    try:
        result = subprocess.run(["node", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Node.js 확인 완료: {result.stdout.strip()}")
        else:
            print("❌ Node.js가 설치되어 있지 않습니다.")
            return False
    except FileNotFoundError:
        print("❌ Node.js가 설치되어 있지 않습니다.")
        return False
    
    # npm 확인
    try:
        result = subprocess.run(["npm", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ npm 확인 완료: {result.stdout.strip()}")
        else:
            print("❌ npm이 설치되어 있지 않습니다.")
            return False
    except FileNotFoundError:
        print("❌ npm이 설치되어 있지 않습니다.")
        return False
    
    return True

def main():
    """메인 함수"""
    print("🎁 카카오 선물 추천 서비스 전체 스택 실행")
    print("=" * 50)
    
    # 의존성 확인
    if not check_dependencies():
        sys.exit(1)
    
    # 백엔드와 프론트엔드를 별도 스레드에서 실행
    backend_thread = threading.Thread(target=run_backend, daemon=True)
    frontend_thread = threading.Thread(target=run_frontend, daemon=True)
    
    try:
        # 백엔드 먼저 시작
        backend_thread.start()
        print("⏳ 백엔드 서버가 시작될 때까지 잠시 기다립니다...")
        time.sleep(3)
        
        # 프론트엔드 시작
        frontend_thread.start()
        
        print("\n🎉 서비스가 시작되었습니다!")
        print("📱 프론트엔드: http://localhost:3000")
        print("🔧 백엔드 API: http://localhost:8000")
        print("📖 API 문서: http://localhost:8000/docs")
        print("\n🛑 종료하려면 Ctrl+C를 누르세요.")
        
        # 메인 스레드 대기
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n🛑 서비스를 종료합니다...")
        sys.exit(0)
    except Exception as e:
        print(f"❌ 실행 중 오류 발생: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
