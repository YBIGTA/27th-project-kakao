# -*- coding: utf-8 -*-
"""
라벨링 파이프라인 테스트 스크립트
"""

import os
import sys
from config import LABELS, FEWSHOT_PER_LABEL
from utils import load_few_shot_examples
from labeling_agent import LabelingAgent
from groq_labeler import call_groq
from openai_refiner import call_openai

def test_few_shot_loading():
    """Few-shot 예시 로딩 테스트"""
    print("=== Few-shot 예시 로딩 테스트 ===")
    
    try:
        few_dict = load_few_shot_examples("labelling_sample.csv", LABELS, FEWSHOT_PER_LABEL)
        
        for label, examples in few_dict.items():
            print(f"{label}: {len(examples)}개 예시")
            for example in examples[:2]:  # 처음 2개만 출력
                print(f"  - {example}")
        print()
        return True
        
    except Exception as e:
        print(f"Few-shot 로딩 실패: {e}")
        return False

def test_agent_prompt():
    """라벨링 에이전트 프롬프트 생성 테스트"""
    print("=== 라벨링 에이전트 테스트 ===")
    
    try:
        few_dict = load_few_shot_examples("labelling_sample.csv", LABELS, FEWSHOT_PER_LABEL)
        agent = LabelingAgent(few_shot_examples=few_dict)
        
        test_text = "노트북이 너무 느려서 새로 사고 싶어"
        prompt = agent.build_prompt(test_text)
        
        print(f"테스트 텍스트: {test_text}")
        print(f"생성된 프롬프트 길이: {len(prompt)} 문자")
        print("프롬프트 미리보기:")
        print(prompt[:500] + "..." if len(prompt) > 500 else prompt)
        print()
        return True
        
    except Exception as e:
        print(f"에이전트 테스트 실패: {e}")
        return False

def test_api_connectivity():
    """API 연결성 테스트"""
    print("=== API 연결성 테스트 ===")
    
    # Groq API 키 확인
    groq_key = os.getenv("GROQ_API_KEY")
    if groq_key and groq_key != "YOUR_GROQ_API_KEY":
        print("✓ GROQ_API_KEY 설정됨")
        groq_ok = True
    else:
        print("✗ GROQ_API_KEY 설정되지 않음")
        groq_ok = False
    
    # OpenAI API 키 확인
    openai_key = os.getenv("OPENAI_API_KEY")
    if openai_key and openai_key != "YOUR_OPENAI_API_KEY":
        print("✓ OPENAI_API_KEY 설정됨")
        openai_ok = True
    else:
        print("✗ OPENAI_API_KEY 설정되지 않음")
        openai_ok = False
    
    print()
    
    # 두 API 키가 모두 설정되어 있으면 성공
    return groq_ok and openai_ok

def test_dataset_access():
    """데이터셋 접근 테스트"""
    print("=== 데이터셋 접근 테스트 ===")
    
    try:
        from utils import iter_dataset_files, load_json, extract_texts_any
        
        files = iter_dataset_files("dataset1")
        print(f"데이터셋 파일 개수: {len(files)}")
        
        if files:
            # 첫 번째 파일 테스트
            first_file = files[0]
            print(f"첫 번째 파일: {os.path.basename(first_file)}")
            
            data = load_json(first_file)
            texts = extract_texts_any(data)
            print(f"추출된 텍스트 개수: {len(texts)}")
            
            if texts:
                print(f"첫 번째 텍스트: {texts[0][:100]}...")
        
        print()
        return True
        
    except Exception as e:
        print(f"데이터셋 접근 테스트 실패: {e}")
        return False

def main():
    """메인 테스트 함수"""
    print("라벨링 파이프라인 테스트 시작\n")
    
    tests = [
        ("Few-shot 예시 로딩", test_few_shot_loading),
        ("라벨링 에이전트", test_agent_prompt),
        ("API 연결성", test_api_connectivity),
        ("데이터셋 접근", test_dataset_access)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"{test_name} 테스트에서 예외 발생: {e}")
            results.append((test_name, False))
    
    print("=== 테스트 결과 요약 ===")
    for test_name, result in results:
        status = "✓ 통과" if result else "✗ 실패"
        print(f"{test_name}: {status}")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    print(f"\n전체: {passed}/{total} 테스트 통과")
    
    if passed == total:
        print("🎉 모든 테스트 통과! 파이프라인을 실행할 수 있습니다.")
    else:
        print("⚠️  일부 테스트 실패. 설정을 확인해주세요.")

if __name__ == "__main__":
    main()
