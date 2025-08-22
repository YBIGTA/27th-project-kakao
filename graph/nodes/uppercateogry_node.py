import os
import json
import numpy as np
import pandas as pd
from rag.llm import chat
from rag.prompt import get_upper_prompt

def softmax(x):
    """소프트맥스: 지수 함수를 사용하여 확률 분포로 변환 (부드러운 확률 분포)"""
    e_x = np.exp(x - np.max(x))
    return e_x / e_x.sum()

def uppercategory_node(state: dict) -> dict:
    """
    Upper Category Node
    Input:
        - CSV 파일 (processed_message.csv)
    Output:
        - state["upper_conf"]: 상위 카테고리 확률 분포
        - state["upper_reasoning"]: 상위 카테고리별 reasoning
        - state["upper_evidence"]: 상위 카테고리별 evidence (LLM 추론 근거 문장들)
    """
    # ⚠️ TODO: OUTPUT_DIR은 최종적으로 config/settings.py에서 불러오도록 수정 필요
    output_dir = state.get("output_dir", "db/output_db")  # 임시 기본값
    csv_path = os.path.join(output_dir, "processed_message.csv")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV 파일을 찾을 수 없습니다: {csv_path}")

    # 1. CSV 읽기 (message 열만 모아서 conversation_text로 변환)
    df = pd.read_csv(csv_path)
    if "message" not in df.columns:
        raise ValueError("CSV에 'message' 열이 없습니다.")
    
    conversation_text = "\n".join(df["message"].astype(str).tolist())

    # 2. 프롬프트 생성
    prompt = get_upper_prompt(conversation_text)

    # 3. LLM 호출
    try:
        response = chat(prompt)
        result = json.loads(response)
    except json.JSONDecodeError:
        raise ValueError(f"LLM 응답이 JSON 형식이 아님: {response}")
    except Exception as e:
        raise RuntimeError(f"LLM 호출 실패: {e}")

    # 4. 응답 구조 검증
    if "categories" not in result:
        raise ValueError(f"LLM 응답에 'categories' 필드가 없습니다: {result}")
    
    if not result["categories"]:
        raise ValueError(f"LLM 응답에 카테고리가 없습니다: {result}")

    # 5. confidence, reasoning, evidence 추출
    # evidence가 필요한 이유:
    # - product_node에서 상품 추천 시 사용자의 구체적인 요구사항 파악
    # - 디버깅 및 투명성: 왜 이 카테고리가 선택되었는지 추적 가능
    scores, category_names = [], []
    reasoning = {}
    evidence = {}

    for cat in result["categories"]:
        name = cat["name"]
        conf = cat["confidence"]
        reason = cat.get("reason", "")
        cat_evidence = cat.get("evidence", [])

        category_names.append(name)
        scores.append(conf)
        reasoning[name] = reason
        evidence[name] = cat_evidence

    # 6. Softmax 정규화 (프롬프팅으로 점수 차이를 조절하여 왜곡 최소화)
    probs = softmax(np.array(scores))

    # 7. 최종 Output (state에 저장)
    upper_conf = {cat: float(p) for cat, p in zip(category_names, probs)}

    state["upper_conf"] = upper_conf
    state["upper_reasoning"] = reasoning
    state["upper_evidence"] = evidence

    return state
