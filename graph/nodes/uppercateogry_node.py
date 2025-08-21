import os
import json
import numpy as np
from rag.llm import chat
from rag.prompt import get_upper_prompt

def softmax(x):
    e_x = np.exp(x - np.max(x))
    return e_x / e_x.sum()

def uppercategory_node(state):
    """
    Upper Category Node
    Input: 전처리된 CSV 파일 (processed_message.csv)
    Output: reasoning, 상위 카테고리 확률 분포
    """
    # ⚠️ TODO: OUTPUT_DIR은 최종적으로 config/settings.py에서 불러오도록 수정 필요
    output_dir = state.get("output_dir", "db/output_db")  # 임시 기본값
    csv_path = os.path.join(output_dir, "processed_message.csv")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV 파일을 찾을 수 없습니다: {csv_path}")

    # 1. CSV 읽기 (message 열만 모아서 conversation_text로 변환)
    import pandas as pd
    df = pd.read_csv(csv_path)
    if "message" not in df.columns:
        raise ValueError("CSV에 'message' 열이 없습니다.")
    
    conversation_text = "\n".join(df["message"].astype(str).tolist())

    # 2. 프롬프트 생성
    prompt = get_upper_prompt(conversation_text)

    # 3. LLM 호출
    response = chat(prompt)
    result = json.loads(response)

    # 5. confidence, reasoning 추출
    scores, category_names = [], []
    reasoning = {}

    for cat in result["categories"]:
        name = cat["name"]
        conf = cat["confidence"]
        reason = cat.get("reason", "")

        category_names.append(name)
        scores.append(conf)
        reasoning[name] = reason

    # 6. Softmax 정규화
    probs = softmax(np.array(scores))

    # 7. 최종 Output (state에 저장)
    upper_conf = {cat: float(p) for cat, p in zip(category_names, probs)}

    state["upper_conf"] = upper_conf
    state["upper_reasoning"] = reasoning

    return state
