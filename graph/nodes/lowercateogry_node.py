import os
import json
import numpy as np
import pandas as pd
from rag.llm import chat
from rag.prompt import get_lower_prompt

def softmax(x):
    e_x = np.exp(x - np.max(x))
    return e_x / e_x.sum()

def lowercategory_node(state):
    """
    Lower Category Node
    Input:
        - CSV 파일 (processed_message.csv)
        - state["upper_conf"]: 상위 카테고리 확률
    Output:
        - state["lower_conf"]: 하위 카테고리 조건부 확률 분포
        - state["lower_reasoning"]: 하위 카테고리별 reasoning
    """

    # ⚠️ TODO: OUTPUT_DIR은 settings.py에서 불러오도록 수정 필요
    output_dir = state.get("output_dir", "db/output_db")  # 임시
    csv_path = os.path.join(output_dir, "processed_message.csv")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV 파일을 찾을 수 없습니다: {csv_path}")

    # 1. CSV 읽기
    df = pd.read_csv(csv_path)
    if "message" not in df.columns:
        raise ValueError("CSV에 'message' 열이 없습니다.")
    conversation_text = "\n".join(df["message"].astype(str).tolist())

    # 2. 상위 카테고리 확률 가져오기
    upper_conf = state.get("upper_conf", {})
    if not upper_conf:
        raise ValueError("상위 카테고리 확률(upper_conf)이 state에 없습니다.")

    lower_conf, lower_reasoning, lower_evidence = {}, {}, {}

    # 3. 각 상위 카테고리에 대해 하위 카테고리 점수 계산
    for upper_cat, upper_prob in upper_conf.items():
        # 하위 카테고리 목록 (프롬프트에 포함될 예정)
        # lower_cats = category_hierarchy[upper_cat]  # 이 부분은 나중에 프롬프트 함수에서 처리

        # 프롬프트 생성
        prompt = get_lower_prompt(conversation_text, upper_cat)

        # LLM 호출
        response = chat(prompt)
        try:
            result = json.loads(response)
        except json.JSONDecodeError:
            raise ValueError(f"LLM 응답이 JSON 형식이 아님: {response}")

        # confidence 추출
        scores, names = [], []
        reasoning = {}

        for cat in result["categories"]:
            name = cat["name"]
            conf = cat["confidence"]
            reason = cat.get("reason", "")

            names.append(name)
            scores.append(conf)
            reasoning[name] = reason

        # Softmax 정규화 (조건부 확률 P(lower|upper))
        cond_probs = softmax(np.array(scores))

        # 조건부 확률 P(lower|upper) 그대로 저장
        for name, cond_p in zip(names, cond_probs):
            lower_conf[name] = float(cond_p)
            lower_reasoning[name] = reasoning[name]

    # state 업데이트
    state["lower_conf"] = lower_conf
    state["lower_reasoning"] = lower_reasoning

    return state
