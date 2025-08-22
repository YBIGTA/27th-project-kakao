import os
import json
import numpy as np
import pandas as pd
from rag.llm import chat
from rag.prompt import get_lower_prompt

def softmax(x):
    """소프트맥스: 지수 함수를 사용하여 확률 분포로 변환 (부드러운 확률 분포)"""
    e_x = np.exp(x - np.max(x))
    return e_x / e_x.sum()

def lowercategory_node(state: dict) -> dict:
    """
    Lower Category Node
    Input:
        - CSV 파일 (processed_message.csv)
        - state["upper_conf"]: 상위 카테고리 확률
    Output:
        - state["lower_conf"]: 하위 카테고리 조건부 확률 분포 (계층형 구조)
        - state["lower_reasoning"]: 하위 카테고리별 reasoning (계층형 구조)
        - state["lower_evidence"]: 하위 카테고리별 evidence (계층형 구조)
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

    # 계층형 구조로 변경: {upper_cat: {lower_cat: prob, ...}, ...}
    lower_conf = {}
    lower_reasoning = {}
    lower_evidence = {}

    # 3. 각 상위 카테고리에 대해 하위 카테고리 점수 계산
    for upper_cat, upper_prob in upper_conf.items():
        # 프롬프트 생성
        prompt = get_lower_prompt(conversation_text, upper_cat)

        # LLM 호출
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

        # 5. confidence, reasoning, evidence 추출 (LLM이 원시 점수를 제공)
        # evidence가 필요한 이유:
        # - product_node에서 상품 추천 시 사용자의 세부 요구사항 파악
        # - 디버깅 및 투명성: 왜 이 하위 카테고리가 선택되었는지 추적 가능
        # - 계층형 구조로 저장하여 상위-하위 관계와 함께 evidence 관리
        scores, names = [], []
        reasoning = {}
        evidence = {}

        for cat in result["categories"]:
            name = cat["name"]
            conf = cat["confidence"]
            reason = cat.get("reason", "")
            cat_evidence = cat.get("evidence", [])

            names.append(name)
            scores.append(conf)
            reasoning[name] = reason
            evidence[name] = cat_evidence

        # 6. Softmax 정규화 (프롬프팅으로 점수 차이를 조절하여 왜곡 최소화)
        cond_probs = softmax(np.array(scores))

        # 7. 계층형 구조로 저장: {upper_cat: {lower_cat: cond_prob, ...}}
        lower_conf[upper_cat] = {}
        lower_reasoning[upper_cat] = {}
        lower_evidence[upper_cat] = {}
        for name, cond_p in zip(names, cond_probs):
            lower_conf[upper_cat][name] = float(cond_p)
            lower_reasoning[upper_cat][name] = reasoning[name]
            lower_evidence[upper_cat][name] = evidence[name]

    # 8. state 업데이트
    state["lower_conf"] = lower_conf
    state["lower_reasoning"] = lower_reasoning
    state["lower_evidence"] = lower_evidence

    return state
