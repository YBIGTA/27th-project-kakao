"""
파이프라인 엔진 (pipeline.py)

구조:
1. uppercategory_node.py - 상위 카테고리 확률 계산
2. lowercategory_node.py - 하위 카테고리 확률 계산  
3. joint_gate_node.py - 로그 가중합 결합 및 Top-K 선택
4. db_filter_node.py - 선택된 카테고리로 상품 필터링
5. product_node.py - 최종 상품 랭킹/가드레일
"""

from typing import Dict, Any
from .preprocess.main_processor import main
from .core.nodes import (
    init_node,
    parent_score_node,
    child_score_node,
    hierarchy_node,
    select_top3_node,
    db_filter_node,
    product_node,
    pack_node
)

class PipelineEngine:
    def __init__(self):
        """
        LangGraph 파이프라인 엔진을 초기화합니다.
        """



    
    async def run(
        self,
        file_bytes: bytes,
        filename: str,
        age: int,
        gender: str,
        relation: str,
        budget_min: int,
        budget_max: int,
    ) -> Dict[str, Any]:
        """
        파이프라인을 실행합니다.
        
        Args:
            file_bytes: 파일 bytes
            filename: 파일명
            age: 사용자 연령
            gender: 사용자 성별
            relation: 관계
            budget_min: 최소 예산
            budget_max: 최대 예산
            
        Returns:
            Dict[str, Any]: 파이프라인 실행 결과
        """
        try:
            # 1) 전처리: main_processor.py로 CSV 파일 생성
            csv_file_path: str = main(file_bytes=file_bytes, filename=filename)
            if not csv_file_path:
                raise ValueError("전처리된 CSV 파일을 생성하지 못했습니다.")
            
            # 사용자 프로필 구성
            user_profile = {
                "age": age,
                "gender": gender,
                "relation": relation,
                "budget_min": budget_min,
                "budget_max": budget_max,
            }
            
            # 전처리된 데이터 구성
            preprocessed_data = {
                "csv_file_path": csv_file_path,
                "user_profile": user_profile,
            }
            
            # 2) CSV 파일에서 rows 생성
            import pandas as pd
            df = pd.read_csv(csv_file_path)
            rows = []
            for idx, row in df.iterrows():
                text_col = None
                for col in ['text', 'message', 'content']:
                    if col in df.columns:
                        text_col = col
                        break

                if text_col and pd.notna(row[text_col]):
                    from .core.state import MessageRow
                    rows.append(MessageRow(
                        idx=idx,
                        date=str(row.get('date', '')),
                        user=str(row.get('user', '')),
                        text=str(row[text_col]).strip()
                    ))

            # GraphState 생성 및 초기화
            from .core.state import GraphState, GiftContext
            state = GraphState(
                rows=rows,
                ctx=GiftContext(
                    age=age,
                    gender=gender,
                    relation=relation,
                    budget_min=budget_min,
                    budget_max=budget_max
                ),
                debug={}
            )
            
            # init_node 호출
            state = init_node(state)
            
            # parent_score_node 호출
            state = await parent_score_node(state)
            
            if not state.parent_scores:
                return {
                    "analysis": {
                        "message": "상위 카테고리를 분석할 수 없습니다.",
                        "parent_reasoning": state.parent_reasoning
                    },
                    "selections": []
                }
            
            # 3) 하위 카테고리 노드 (child_score_node)
            state = await child_score_node(state)
            
            if not state.child_scores:
                return {
                    "analysis": {
                        "message": "하위 카테고리를 분석할 수 없습니다.",
                        "parent_scores": state.parent_scores
                    },
                    "selections": []
                }
            
            # 4) 계층 결합 노드 (hierarchy_node)
            state = await hierarchy_node(state)
            
            # 5) Top-3 선택 노드 (select_top3_node)
            state = select_top3_node(state)
            
            if not state.top3_children:
                return {
                    "analysis": {
                        "message": "최종 카테고리를 선택할 수 없습니다.",
                        "parent_scores": state.parent_scores,
                        "child_scores": state.child_scores
                    },
                    "selections": []
                }
            
            # 6) DB 필터링 노드 (PostgreSQL 상품 조회)
            state = await db_filter_node(state)
            
            if not state.candidate_products:
                return {
                    "analysis": {
                        "message": "조건에 맞는 상품을 찾을 수 없습니다.",
                        "selected_categories": state.top3_children
                    },
                    "selections": []
                }
            
            # 7) 상품 랭킹/가드레일 노드
            state = await product_node(state)
            
            # 8) 패키징 노드
            state = pack_node(state)
            
            # 결과 반환
            return state.debug["final_payload"]
            
        except Exception as e:
            print(f"파이프라인 실행 중 오류 발생: {e}")
            return {
                "analysis": {
                    "message": f"파이프라인 실행 중 오류가 발생했습니다: {str(e)}"
                },
                "selections": []
            }
    

