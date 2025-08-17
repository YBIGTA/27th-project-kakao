# -*- coding: utf-8 -*-
import json
import os
import csv
import random
from typing import Dict, List, Any, Optional
from pathlib import Path

def safe_json_extract(content: str) -> Dict[str, Any]:
    """JSON 파싱을 안전하게 수행하고 실패 시 기본값 반환"""
    try:
        # JSON 블록 찾기
        start = content.find('{')
        end = content.rfind('}') + 1
        if start != -1 and end != 0:
            json_str = content[start:end]
            result = json.loads(json_str)
            
            # 필수 필드 확인 및 기본값 설정
            if 'label' not in result:
                result['label'] = '단순 언급'
            if 'confidence' not in result:
                result['confidence'] = 0.5
            if 'reason' not in result:
                result['reason'] = '기본값'
                
            return result
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        pass
    
    # 파싱 실패 시 기본값 반환
    return {
        "label": "단순 언급",
        "confidence": 0.0,
        "reason": f"parse_fail: {str(e) if 'e' in locals() else 'unknown'}",
        "raw": content
    }

def load_json(file_path: str) -> Dict[str, Any]:
    """JSON 파일 로드"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"JSON 파일 로드 실패: {file_path}, 오류: {e}")
        return {}

def save_json(data: Dict[str, Any], file_path: str) -> None:
    """JSON 파일 저장"""
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"JSON 파일 저장 실패: {file_path}, 오류: {e}")

def iter_dataset_files(dataset_dir: str) -> List[str]:
    """데이터셋 폴더의 모든 JSON 파일 경로 반환"""
    if not os.path.exists(dataset_dir):
        return []
    
    json_files = []
    for file in os.listdir(dataset_dir):
        if file.endswith('.json'):
            json_files.append(os.path.join(dataset_dir, file))
    
    return sorted(json_files)

def extract_texts_any(data: Dict[str, Any]) -> List[str]:
    """JSON 데이터에서 텍스트 추출 (SJML.text[].content 구조에서 content만 추출)"""
    texts = []

    # SJML.text[].content 구조 파싱
    if isinstance(data, dict) and "SJML" in data:
        sjml_data = data["SJML"]
        if isinstance(sjml_data, dict) and "text" in sjml_data:
            text_array = sjml_data["text"]
            if isinstance(text_array, list):
                for item in text_array:
                    if isinstance(item, dict) and "content" in item:
                        content = item["content"]
                        if isinstance(content, str) and len(content.strip()) > 0:
                            texts.append(content.strip())

    # content가 추출되지 않은 경우에만 fallback 로직 사용
    if not texts:
        print("경고: SJML.text[].content 구조에서 텍스트를 찾을 수 없습니다. fallback 로직을 사용합니다.")
        
        # 직접 텍스트 필드가 있는 경우
        if isinstance(data, dict):
            for key in ['text', 'content', 'message', 'sentence']:
                if key in data and isinstance(data[key], str):
                    texts.append(data[key])
                elif key in data and isinstance(data[key], list):
                    texts.extend([item for item in data[key] if isinstance(item, str)])

        # 중첩된 구조에서 텍스트 찾기
        def find_texts_recursive(obj):
            if isinstance(obj, str):
                if len(obj.strip()) > 5:  # 의미있는 텍스트만
                    texts.append(obj.strip())
            elif isinstance(obj, dict):
                for value in obj.values():
                    find_texts_recursive(value)
            elif isinstance(obj, list):
                for item in obj:
                    find_texts_recursive(item)

        find_texts_recursive(data)

    # 중복 제거만 하고 순서는 유지
    seen = set()
    unique_texts = []
    for text in texts:
        if text not in seen:
            seen.add(text)
            unique_texts.append(text)
    
    return unique_texts

def load_done_ids_from_jsonl(file_path: str) -> set:
    """JSONL 파일에서 이미 처리된 샘플 ID 로드"""
    done_ids = set()
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        data = json.loads(line.strip())
                        if 'sample_id' in data:
                            done_ids.add(data['sample_id'])
        except Exception as e:
            print(f"JSONL 파일 읽기 실패: {file_path}, 오류: {e}")
    return done_ids

def write_jsonl_append(file_path: str, record: Dict[str, Any]) -> None:
    """JSONL 파일에 레코드 추가 (발화 텍스트와 라벨만 저장)"""
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # 필요한 열만 추출하여 저장
        simplified_record = {
            "text": record.get("text", ""),  # 발화 텍스트
            "label": record.get("label", "")  # 라벨 (필요, 구매, 관심, 고민, 부정, 단순 언급)
        }
        
        with open(file_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(simplified_record, ensure_ascii=False) + '\n')
    except Exception as e:
        print(f"JSONL 파일 쓰기 실패: {file_path}, 오류: {e}")

def load_few_shot_examples(csv_path: str, labels: List[str], examples_per_label: int) -> Dict[str, List[str]]:
    """CSV 파일에서 few-shot 예시 로드"""
    few_shot_dict = {}
    
    try:
        with open(csv_path, 'r', encoding='utf-8-sig') as f:  # utf-8-sig로 BOM 제거
            reader = csv.DictReader(f)
            rows = list(reader)
            
            # BOM 문제 해결: 키 이름 정규화
            if rows:
                # 첫 번째 행의 키들을 확인하고 BOM 제거
                first_row = rows[0]
                text_key = None
                for key in first_row.keys():
                    if 'text' in key.lower():
                        text_key = key
                        break
                
                if text_key is None:
                    raise ValueError("CSV 파일에 'text' 컬럼을 찾을 수 없습니다")
                
                for label in labels:
                    label_rows = [row for row in rows if row['label'] == label]
                    if label_rows:
                        # 각 라벨별로 지정된 개수만큼 예시 선택
                        selected = random.sample(label_rows, min(examples_per_label, len(label_rows)))
                        few_shot_dict[label] = [row[text_key] for row in selected]
                    else:
                        few_shot_dict[label] = []
            else:
                raise ValueError("CSV 파일이 비어있습니다")
                    
    except Exception as e:
        print(f"Few-shot 예시 로드 실패: {csv_path}, 오류: {e}")
        # 기본 예시 제공
        few_shot_dict = {
            "필요": ["노트북이 버벅거려", "휴대폰 케이스가 찢어졌어"],
            "구매": ["맨투맨 사고 싶네", "이번에 운동화 살 거야"],
            "관심": ["에어팟 봤어?", "저 가방 디자인 마음에 들어"],
            "고민": ["어떤 신발 살까", "색상을 뭘로 할지 고민돼"],
            "부정": ["나는 립스틱은 별로야", "그거는 안 좋아해"],
            "단순 언급": ["오늘 날씨 예쁘다", "점심에 김치찌개 먹었어"]
        }
    
    return few_shot_dict

def compute_confidence() -> float:
    """신뢰도 점수 계산 (테스트용)"""
    return round(random.uniform(0.4, 0.95), 2)
