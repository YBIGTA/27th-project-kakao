import logging
from typing import Dict, List, Optional, Any
from app.core.state import PipelineState

logger = logging.getLogger(__name__)

def preprocess_node(state: PipelineState) -> PipelineState:
    """대화내역 전처리 노드 - 기존 preprocess 모듈 사용"""
    try:
        logger.info("Starting chat data preprocessing using existing preprocess module")
        
        # CSV 파일 경로 가져오기
        csv_path = state.get("profile", {}).get("chat_csv_path")
        if not csv_path:
            logger.warning("No chat CSV path provided in profile")
            state["sentences"] = []
            state["processed_messages"] = []
            return state
        
        # 기존 preprocess 모듈 사용
        try:
            from app.preprocess.csv_processor import CSVProcessor
            
            # 사용자명 설정 (기본값: "default")
            user_name = state.get("profile", {}).get("target_user", "default")
            
            logger.info(f"Processing CSV with user filter: {user_name}")
            
            # CSVProcessor를 사용하여 전처리
            processor = CSVProcessor(csv_path, user_name)
            result = processor.process()
            
            if not result or 'data' not in result:
                logger.warning("Preprocessing returned no data")
                state["sentences"] = []
                state["processed_messages"] = []
                return state
            
            processed_data = result['data']
            logger.info(f"Preprocessing completed: {len(processed_data)} messages")
            
            # LangGraph에 적합한 형태로 변환
            sentences = []
            processed_messages: List[Dict[str, Any]] = []
            
            for idx, item in enumerate(processed_data):
                message = item.get('message', '').strip()
                if message:  # 빈 메시지 제외
                    # 문장 리스트 (LLM 분석용)
                    sentences.append(message)
                    
                    # 전처리된 메시지 상세 정보
                    processed_messages.append({
                        'index': len(processed_messages),  # 0부터 시작하는 인덱스
                        'original_index': idx,  # 원본 데이터 인덱스
                        'message': message,
                        'user': item.get('user', ''),
                        'date': item.get('date', ''),
                        'original_message': message,  # 이미 정리된 상태
                        'gift_relevance_score': 0  # 기본값, 필요시 계산 가능
                    })
            
            # 상태 업데이트
            state["sentences"] = sentences
            state["processed_messages"] = processed_messages
            
            logger.info(f"Preprocessing completed: {len(sentences)} sentences ready for analysis")
            
            return state
            
        except ImportError as e:
            logger.error(f"Failed to import preprocess module: {e}")
            # fallback: 간단한 CSV 읽기
            return _fallback_csv_processing(state, csv_path)
            
    except Exception as e:
        logger.error(f"Error in preprocessing node: {e}")
        # 에러 시 빈 결과 반환
        state["sentences"] = []
        state["processed_messages"] = []
        return state

def _fallback_csv_processing(state: PipelineState, csv_path: str) -> PipelineState:
    """fallback: 간단한 CSV 처리"""
    try:
        import pandas as pd
        
        logger.info("Using fallback CSV processing")
        
        df = pd.read_csv(csv_path)
        df.columns = [col.strip().lower() for col in df.columns]
        
        # 컬럼명 매핑
        message_col: Optional[str] = None
        user_col: Optional[str] = None
        date_col: Optional[str] = None
        
        for col in df.columns:
            if 'message' in col or 'text' in col or 'content' in col:
                message_col = col
            elif 'user' in col or 'sender' in col:
                user_col = col
            elif 'date' in col or 'time' in col:
                date_col = col
        
        if not message_col:
            logger.error("No message column found in CSV")
            state["sentences"] = []
            state["processed_messages"] = []
            return state
        
        # 간단한 전처리
        sentences = []
        processed_messages: List[Dict[str, Any]] = []
        
        for idx, row in df.iterrows():
            message = str(row.get(message_col, '')).strip()
            if message and len(message) > 3:  # 기본 필터링
                sentences.append(message)
                processed_messages.append({
                    'index': len(processed_messages),
                    'original_index': idx,
                    'message': message,
                    'user': str(row.get(user_col, '')) if user_col else '',
                    'date': str(row.get(date_col, '')) if date_col else '',
                    'original_message': message,
                    'gift_relevance_score': 0
                })
        
        state["sentences"] = sentences
        state["processed_messages"] = processed_messages  # type: ignore
        
        logger.info(f"Fallback processing completed: {len(sentences)} sentences")
        return state
        
    except Exception as e:
        logger.error(f"Fallback processing failed: {e}")
        state["sentences"] = []
        state["processed_messages"] = []
        return state
