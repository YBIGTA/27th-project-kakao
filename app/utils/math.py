#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
from typing import List, Union, Dict
import logging

logger = logging.getLogger(__name__)

def softmax_with_temp(scores: Union[List[float], Dict[str, float]], 
                      temperature: float = 1.0,
                      clamp_min: float = 0.0,
                      clamp_max: float = 1.0) -> Union[List[float], Dict[str, float]]:
    """
    온도 조절 가능한 softmax 함수
    
    Args:
        scores: 점수 리스트 또는 딕셔너리
        temperature: 온도 파라미터 (낮을수록 더 확실한 선택)
        clamp_min: 최소값 제한
        clamp_max: 최대값 제한
    
    Returns:
        확률 분포 (합이 1.0)
    """
    try:
        if isinstance(scores, dict):
            # 딕셔너리인 경우
            names = list(scores.keys())
            values = list(scores.values())
            probs = _softmax_core(values, temperature, clamp_min, clamp_max)
            return dict(zip(names, probs))
        else:
            # 리스트인 경우
            return _softmax_core(scores, temperature, clamp_min, clamp_max)
            
    except Exception as e:
        logger.error(f"Softmax 계산 오류: {e}")
        # 에러 시 균등 분포 반환
        if isinstance(scores, dict):
            n = len(scores)
            return {k: 1.0/n for k in scores.keys()}
        else:
            n = len(scores)
            return [1.0/n] * n

def _softmax_core(values: List[float], 
                  temperature: float,
                  clamp_min: float,
                  clamp_max: float) -> List[float]:
    """softmax 핵심 계산"""
    try:
        # 값 클램핑
        clamped = [max(clamp_min, min(clamp_max, v)) for v in values]
        
        # 온도 조절
        if temperature <= 0:
            temperature = 1e-6  # 0으로 나누기 방지
        
        # softmax 계산
        exp_scores = np.exp(np.array(clamped) / temperature)
        probs = exp_scores / np.sum(exp_scores)
        
        return probs.tolist()
        
    except Exception as e:
        logger.error(f"Softmax 핵심 계산 오류: {e}")
        # 에러 시 균등 분포 반환
        n = len(values)
        return [1.0/n] * n

def normalize_scores(scores: Union[List[float], Dict[str, float]], 
                    method: str = "minmax") -> Union[List[float], Dict[str, float]]:
    """
    점수 정규화
    
    Args:
        scores: 점수 리스트 또는 딕셔너리
        method: 정규화 방법 ("minmax", "zscore", "robust")
    
    Returns:
        정규화된 점수
    """
    try:
        if isinstance(scores, dict):
            names = list(scores.keys())
            values = list(scores.values())
            normalized = _normalize_core(values, method)
            return dict(zip(names, normalized))
        else:
            return _normalize_core(scores, method)
            
    except Exception as e:
        logger.error(f"정규화 오류: {e}")
        return scores

def _normalize_core(values: List[float], method: str) -> List[float]:
    """정규화 핵심 계산"""
    try:
        if not values:
            return values
            
        if method == "minmax":
            # Min-Max 정규화 (0~1)
            min_val = min(values)
            max_val = max(values)
            if max_val == min_val:
                return [0.5] * len(values)
            return [(v - min_val) / (max_val - min_val) for v in values]
            
        elif method == "zscore":
            # Z-score 정규화
            mean_val = np.mean(values)
            std_val = np.std(values)
            if std_val == 0:
                return [0.0] * len(values)
            return [(v - mean_val) / std_val for v in values]
            
        elif method == "robust":
            # Robust 정규화 (중앙값 기반)
            median_val = float(np.median(values))
            mad = float(np.median([abs(float(v) - median_val) for v in values]))
            if mad == 0:
                return [0.0] * len(values)
            return [float((float(v) - median_val) / mad) for v in values]
            
        else:
            logger.warning(f"알 수 없는 정규화 방법: {method}")
            return values
            
    except Exception as e:
        logger.error(f"정규화 핵심 계산 오류: {e}")
        return values
