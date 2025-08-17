
# -*- coding: utf-8 -*-
import os
import time
from config import (
    DATASET_DIR, RESULTS_DIR, CONFIDENCE_THRESHOLD, 
    FEWSHOT_SOURCE, FEWSHOT_PER_LABEL, LABELS, RATE_LIMIT_DELAY
)
from utils import (
    iter_dataset_files, load_json, extract_texts_any, 
    load_done_ids_from_jsonl, write_jsonl_append, load_few_shot_examples
)
from labeling_agent import LabelingAgent
from groq_labeler import call_groq
from openai_refiner import call_openai

def process_file(in_path: str, agent: LabelingAgent):
    """개별 파일 처리"""
    base = os.path.basename(in_path)
    out_path = os.path.join(RESULTS_DIR, base.replace(".json", "_labeled.jsonl"))
    os.makedirs(RESULTS_DIR, exist_ok=True)

    # 이미 처리된 샘플 ID 로드
    done_ids = load_done_ids_from_jsonl(out_path)
    data = load_json(in_path)
    texts = extract_texts_any(data)
    total = len(texts)
    
    if total == 0:
        print(f"[{base}] 텍스트 없음. 건너뜀.")
        return

    print(f"[{base}] 텍스트 {total}개. 이미 완료 {len(done_ids)}개.")

    for idx, text in enumerate(texts):
        sample_id = f"{base}#{idx}"
        if sample_id in done_ids:
            continue

        # 프롬프트 생성
        user_prompt = agent.build_prompt(text)

        # 1) Groq 초벌 라벨링
        try:
            print(f"\n[{base}] {idx+1}/{total} ====== 처리 시작 ======")
            print(f"텍스트: {text[:100]}...")
            print(f"Groq API 호출 중...")
            
            from groq_labeler import call_groq
            groq_result = call_groq(user_prompt, agent.get_few_shot_examples())
            
            print(f"Groq 결과: {groq_result}")
            
        except Exception as e:
            print(f"Groq API 호출 실패: {e}")
            groq_result = {
                "label": "단순 언급",
                "confidence": 0.0,
                "reason": f"groq_error: {str(e)}"
            }

        label = groq_result.get("label", "단순 언급")
        conf = float(groq_result.get("confidence", 0.0))
        print(f"Groq 라벨: {label}, 신뢰도: {conf:.3f}")

        # 2) OpenAI 재라벨링 (불확실한 경우)
        src_model = "groq"
        if conf < CONFIDENCE_THRESHOLD:
            try:
                print(f"신뢰도 {conf:.3f} < {CONFIDENCE_THRESHOLD} → OpenAI 재라벨링 시작")
                from openai_refiner import call_openai
                openai_result = call_openai(text)
                label = openai_result.get("label", label)
                conf = float(openai_result.get("confidence", conf))
                src_model = "openai_refined"
                print(f"OpenAI 재라벨링 결과: {label} (신뢰도: {conf:.3f})")
            except Exception as e:
                print(f"OpenAI 재라벨링 실패: {e}")
                src_model = "groq_fallback_err"
        else:
            print(f"신뢰도 {conf:.3f} >= {CONFIDENCE_THRESHOLD} → OpenAI 재라벨링 불필요")

        print(f"[{base}] {idx+1}/{total} ====== 최종 결과 ======")
        print(f"라벨: {label}")
        print(f"신뢰도: {conf:.3f}")
        print(f"모델: {src_model}")
        print(f"이유: {groq_result.get('reason', 'N/A')}")
        print(f"=====================================")

        # 결과 기록
        record = {
            "sample_id": sample_id,
            "file": base,
            "index": idx,
            "text": text,
            "label": label,
            "confidence": round(conf, 3),
            "source": src_model,
            "groq_result": groq_result
        }
        
        write_jsonl_append(out_path, record)

        # 진행상황 출력 (적절한 간격)
        if (idx + 1) % 5 == 0:  # 5개마다 진행상황 출력
            print(f"\n[{base}] ====== 진행상황 ======")
            print(f"처리된 텍스트: {idx + 1}/{total} ({((idx + 1)/total*100):.1f}%)")
            print(f"현재 라벨: {label}")
            print(f"현재 신뢰도: {conf:.3f}")
            print(f"사용된 모델: {src_model}")
            print(f"==============================\n")
        
        # Rate limit 준수 (30초 대기)
        print(f"[{base}] {idx + 1}/{total} API 호출 후 30초 대기 중...")
        time.sleep(RATE_LIMIT_DELAY)
        print(f"[{base}] {idx + 1}/{total} 대기 완료, 다음 텍스트 처리 시작")

    print(f"[{base}] 완료 → {out_path}")

def main():
    """메인 실행 함수"""
    print("라벨링 파이프라인 시작...")
    
    # Few-shot 예시 로드
    print(f"Few-shot 예시 로드 중... ({FEWSHOT_SOURCE})")
    few_dict = load_few_shot_examples(FEWSHOT_SOURCE, LABELS, FEWSHOT_PER_LABEL)
    
    # 각 라벨별 예시 개수 출력
    for label, examples in few_dict.items():
        print(f"  {label}: {len(examples)}개 예시")
    
    # 라벨링 에이전트 생성
    agent = LabelingAgent(few_shot_examples=few_dict)

    # 데이터셋 파일들 처리
    files = iter_dataset_files(DATASET_DIR)
    if not files:
        print(f"데이터 파일이 없습니다: {DATASET_DIR}")
        return
    
    print(f"총 {len(files)}개 파일 처리 예정")
    
    for fp in files:
        process_file(fp, agent)
    
    print("모든 파일 처리 완료!")

if __name__ == "__main__":
    main()
