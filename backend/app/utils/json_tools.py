#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
from typing import Any, Optional, Union, Dict, List
import re

logger = logging.getLogger(__name__)

def safe_json_loads(text: str, fallback: Any = None) -> Any:
    """
    안전한 JSON 파싱
    
    Args:
        text: 파싱할 JSON 문자열
        fallback: 파싱 실패 시 반환할 기본값
    
    Returns:
        파싱된 객체 또는 fallback 값
    """
    try:
        if not text or not isinstance(text, str):
            return fallback
            
        # 문자열 정리
        cleaned = text.strip()
        if not cleaned:
            return fallback
            
        # JSON 파싱 시도
        return json.loads(cleaned)
        
    except json.JSONDecodeError as e:
        logger.warning(f"JSON 파싱 실패: {e}")
        # 부분 파싱 시도
        return _partial_json_parse(cleaned, fallback)
        
    except Exception as e:
        logger.error(f"JSON 파싱 중 예상치 못한 오류: {e}")
        return fallback

def _partial_json_parse(text: str, fallback: Any) -> Any:
    """부분 JSON 파싱 시도"""
    try:
        # 마크다운 코드 블록 제거
        text = re.sub(r'```json\s*', '', text)
        text = re.sub(r'```\s*$', '', text)
        
        # 불필요한 공백 제거
        text = re.sub(r'\s+', ' ', text)
        
        # 다시 파싱 시도
        return json.loads(text)
        
    except Exception:
        # 완전 실패 시 fallback 반환
        return fallback

def extract_json_from_text(text: str, fallback: Any = None) -> Any:
    """
    텍스트에서 JSON 부분 추출
    
    Args:
        text: JSON이 포함된 텍스트
        fallback: 추출 실패 시 반환할 기본값
    
    Returns:
        추출된 JSON 객체 또는 fallback 값
    """
    try:
        if not text:
            return fallback
            
        # JSON 객체 패턴 찾기
        json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
        matches = re.findall(json_pattern, text)
        
        if matches:
            # 가장 긴 JSON 문자열 선택
            longest_match = max(matches, key=len)
            return safe_json_loads(longest_match, fallback)
            
        # JSON 배열 패턴 찾기
        array_pattern = r'\[[^\[\]]*(?:\{[^{}]*\}[^\[\]]*)*\]'
        matches = re.findall(array_pattern, text)
        
        if matches:
            longest_match = max(matches, key=len)
            return safe_json_loads(longest_match, fallback)
            
        return fallback
        
    except Exception as e:
        logger.error(f"JSON 추출 중 오류: {e}")
        return fallback

def validate_json_structure(data: Any, expected_keys: List[str]) -> bool:
    """
    JSON 구조 검증
    
    Args:
        data: 검증할 데이터
        expected_keys: 필수 키 목록
    
    Returns:
        검증 성공 여부
    """
    try:
        if not isinstance(data, dict):
            return False
            
        for key in expected_keys:
            if key not in data:
                logger.warning(f"필수 키 누락: {key}")
                return False
                
        return True
        
    except Exception as e:
        logger.error(f"JSON 구조 검증 중 오류: {e}")
        return False

def clean_json_string(text: str) -> str:
    """
    JSON 문자열 정리
    
    Args:
        text: 정리할 텍스트
    
    Returns:
        정리된 텍스트
    """
    try:
        if not text:
            return ""
            
        # 마크다운 코드 블록 제거
        text = re.sub(r'```(?:json)?\s*', '', text)
        text = re.sub(r'```\s*$', '', text)
        
        # 불필요한 공백 정리
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        
        return text
        
    except Exception as e:
        logger.error(f"JSON 문자열 정리 중 오류: {e}")
        return text

def merge_json_objects(obj1: Dict, obj2: Dict, overwrite: bool = True) -> Dict:
    """
    두 JSON 객체 병합
    
    Args:
        obj1: 첫 번째 객체
        obj2: 두 번째 객체
        overwrite: obj2로 덮어쓸지 여부
    
    Returns:
        병합된 객체
    """
    try:
        if not isinstance(obj1, dict):
            return obj2 if isinstance(obj2, dict) else {}
            
        if not isinstance(obj2, dict):
            return obj1
            
        result = obj1.copy()
        
        for key, value in obj2.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                # 중첩 딕셔너리인 경우 재귀 병합
                result[key] = merge_json_objects(result[key], value, overwrite)
            elif overwrite or key not in result:
                # 덮어쓰기 또는 키가 없는 경우
                result[key] = value
                
        return result
        
    except Exception as e:
        logger.error(f"JSON 객체 병합 중 오류: {e}")
        return obj1 if isinstance(obj1, dict) else {}
