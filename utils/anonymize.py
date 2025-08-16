import re
from typing import Dict, List, Tuple, Any
from transformers import AutoTokenizer, AutoModelForTokenClassification, pipeline, BertTokenizerFast, BertForTokenClassification

class PIIProcessor:
    """KoELECTRA NER + kor-naver-ner-name을 결합한 하이브리드 PII 마스킹 프로세서"""
    
    def __init__(self):
        """PII 프로세서 초기화"""
        self.koelectra_model = None
        self.koelectra_tokenizer = None
        self.koelectra_pipeline = None
        
        self.name_model = None
        self.name_tokenizer = None
        self.name_pipeline = None
        
        self._load_models()
    
    def _load_models(self):
        """두 모델을 로드합니다."""
        try:
            print("🔄 KoELECTRA NER 모델 로드 중...")
            
            # KoELECTRA NER 모델 로드
            ko_model_name = "Leo97/KoELECTRA-small-v3-modu-ner"
            self.koelectra_tokenizer = AutoTokenizer.from_pretrained(ko_model_name)
            self.koelectra_model = AutoModelForTokenClassification.from_pretrained(ko_model_name)
            self.koelectra_pipeline = pipeline("ner", model=self.koelectra_model, tokenizer=self.koelectra_tokenizer)
            
            print("✅ KoELECTRA NER 모델 로드 완료!")
            
            print("🔄 kor-naver-ner-name 모델 로드 중...")
            
            # kor-naver-ner-name 모델 로드
            name_model_name = "joon09/kor-naver-ner-name"
            self.name_tokenizer = BertTokenizerFast.from_pretrained(name_model_name)
            self.name_model = BertForTokenClassification.from_pretrained(name_model_name)
            self.name_pipeline = pipeline("ner", model=self.name_model, tokenizer=self.name_tokenizer)
            
            print("✅ kor-naver-ner-name 모델 로드 완료!")
            
        except Exception as e:
            print(f"❌ 모델 로드 실패: {e}")
            self.koelectra_model = None
            self.name_model = None
    
    def mask_pii(self, text: str) -> str:
        """텍스트에서 PII를 마스킹합니다."""
        if not self.koelectra_pipeline or not self.name_pipeline:
            print("⚠️ 일부 모델을 사용할 수 없습니다.")
            return text
        
        try:
            # 1단계: kor-naver-ner-name으로 이름 인식
            name_entities = self.name_pipeline(text, grouped_entities=True, aggregation_strategy='average')
            
            # 2단계: KoELECTRA로 다른 개체명 인식
            other_entities = self.koelectra_pipeline(text)
            
            # 3단계: 두 결과를 결합하여 마스킹 적용
            masked_text = self._apply_hybrid_masking(text, name_entities, other_entities)
            
            # 4단계: 숫자 패턴 직접 마스킹 (전체 텍스트에서)
            masked_text = self._mask_number_patterns(masked_text)
            
            return masked_text
            
        except Exception as e:
            print(f"❌ PII 마스킹 중 오류 발생: {e}")
            return text
    
    def _apply_hybrid_masking(self, text: str, name_entities: List[Dict], other_entities: List[Dict]) -> str:
        """두 모델의 결과를 결합하여 마스킹을 적용합니다."""
        if not name_entities and not other_entities:
            return text
        
        # 모든 개체명을 하나의 리스트로 결합
        all_entities = []
        
        # 이름 개체명 처리 (kor-naver-ner-name)
        for entity in name_entities:
            if entity['entity_group'] == 'PER':
                all_entities.append({
                    'start': entity['start'],
                    'end': entity['end'],
                    'word': entity['word'],
                    'entity': 'B-PS',  # KoELECTRA 형식으로 통일
                    'score': entity['score'],
                    'source': 'name_model'
                })
        
        # 다른 개체명 처리 (KoELECTRA)
        for entity in other_entities:
            # 이름이 아닌 개체명만 추가 (이름은 이미 처리됨)
            if not entity['entity'].startswith('B-PS') and not entity['entity'].startswith('I-PS'):
                all_entities.append({
                    'start': entity['start'],
                    'end': entity['end'],
                    'word': entity['word'],
                    'entity': entity['entity'],
                    'score': 1.0,  # KoELECTRA는 기본 점수
                    'source': 'koelectra'
                })
        
        # 개체명을 위치 순으로 정렬 (끝 위치 기준 내림차순)
        sorted_entities = sorted(all_entities, key=lambda x: x['end'], reverse=True)
        
        masked_text = text
        
        for entity in sorted_entities:
            start = entity['start']
            end = entity['end']
            entity_text = entity['word']
            entity_type = entity['entity']
            score = entity['score']
            
            # 마스킹 태그 결정
            mask_tag = self._get_mask_tag(entity_type, entity_text)
            
            if mask_tag:
                # 원본 텍스트에서 개체명을 마스킹 태그로 교체
                masked_text = masked_text[:start] + mask_tag + masked_text[end:]
        
        return masked_text
    
    def _get_mask_tag(self, entity_type: str, entity_text: str) -> str:
        """개체명 타입에 따른 마스킹 태그를 반환합니다."""
        # 필요한 민감정보만 마스킹 (과도한 마스킹 방지)
        tag_mapping = {
            'B-PS': '{이름}',      # 인명 (PERSON)
            'I-PS': '{이름}',      # 인명 (PERSON)
            'B-LC': '{주소}',      # 지역/장소 (LOCATION)
            'I-LC': '{주소}',      # 지역/장소 (LOCATION)
        }
        
        # 숫자 패턴 확인 (3개 이상의 아라비아 숫자)
        if re.match(r'^\d{3,}$', entity_text):
            return '{숫자}'
        
        # 숫자+하이픈+공백 패턴 확인
        hyphen_regex = r'^\d+[-\s]+\d+[-\s]*\d*$'
        if re.match(hyphen_regex, entity_text):
            return '{숫자}'
        
        return tag_mapping.get(entity_type, None)
    
    def _mask_number_patterns(self, text: str) -> str:
        """전체 텍스트에서 숫자 패턴을 찾아 마스킹합니다."""
        # 1. 연속된 숫자 3개 이상 (전화번호, 계좌번호 등)
        text = re.sub(r'\d{3,}', '{숫자}', text)
        
        # 2. 숫자-숫자-숫자 패턴 (전화번호 형식)
        text = re.sub(r'\d+-\d+-\d+', '{숫자}', text)
        
        # 3. 숫자-숫자 패턴
        text = re.sub(r'\d+-\d+', '{숫자}', text)
        
        # 4. 숫자 공백 숫자 패턴
        text = re.sub(r'\d+\s+\d+', '{숫자}', text)
        
        return text
    
    def get_pii_info(self, text: str) -> Dict[str, List[str]]:
        """텍스트에서 발견된 PII 정보를 반환합니다."""
        if not self.koelectra_pipeline or not self.name_pipeline:
            return {}
        
        try:
            # 이름 개체명 추출 (kor-naver-ner-name)
            name_entities = self.name_pipeline(text, grouped_entities=True, aggregation_strategy='average')
            
            # 다른 개체명 추출 (KoELECTRA)
            other_entities = self.koelectra_pipeline(text)
            
            # PII 정보 수집
            pii_info = {}
            
            # 이름 정보 수집
            for entity in name_entities:
                if entity['entity_group'] == 'PER':
                    if '이름' not in pii_info:
                        pii_info['이름'] = []
                    pii_info['이름'].append(entity['word'])
            
            # 다른 개체명 정보 수집
            for entity in other_entities:
                entity_type = entity['entity']
                entity_text = entity['word']
                
                # 이름이 아닌 개체명만 추가
                if not entity_type.startswith('B-PS') and not entity_type.startswith('I-PS'):
                    # 태그 타입 정리 (B-, I- 제거)
                    clean_type = entity_type.replace('B-', '').replace('I-', '')
                    
                    # 한글 태그명으로 변환
                    korean_type = self._get_korean_type_name(clean_type)
                    
                    if korean_type not in pii_info:
                        pii_info[korean_type] = []
                    
                    if entity_text not in pii_info[korean_type]:
                        pii_info[korean_type].append(entity_text)
            
            return pii_info
            
        except Exception as e:
            print(f"❌ PII 정보 추출 중 오류 발생: {e}")
            return {}
    
    def _get_korean_type_name(self, entity_type: str) -> str:
        """영문 태그를 한글명으로 변환합니다."""
        type_mapping = {
            'PS': '이름',
            'LC': '주소',
            'OG': '소속정보',
            'DT': '날짜',
            'TI': '시간',
            'QT': '숫자',
            'AF': '물건',
            'EV': '사건',
            'AM': '동물',
            'PT': '식물',
            'MT': '재료',
            'CV': '문명',
            'FD': '학문',
            'TR': '이론',
            'TM': '용어'
        }
        
        return type_mapping.get(entity_type, entity_type)


def anonymize_messages(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    메시지 리스트에서 민감정보를 익명화합니다.
    
    Args:
        data (List[Dict[str, Any]]): 메시지 데이터 리스트
    
    Returns:
        List[Dict[str, Any]]: 익명화된 메시지 데이터 리스트
    """
    print("🔒 익명화 처리 중...")
    
    # PII 프로세서 초기화
    try:
        pii_processor = PIIProcessor()
        print("✅ PII 프로세서 초기화 완료")
    except Exception as e:
        print(f"⚠️ PII 프로세서 초기화 실패: {e}")
        print("   원본 메시지를 그대로 사용합니다.")
        return data
    
    anonymized_data = []
    for item in data:
        anonymized_item = item.copy()
        if 'message' in anonymized_item:
            try:
                anonymized_item['message'] = pii_processor.mask_pii(anonymized_item['message'])
            except Exception as e:
                print(f"⚠️ 메시지 익명화 실패: {e}")
                # 실패 시 원본 메시지 유지
                pass
        anonymized_data.append(anonymized_item)
    
    print(f"✅ 익명화 완료: {len(data)}개 메시지")
    return anonymized_data
