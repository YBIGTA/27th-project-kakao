import re
from typing import Dict, List, Any


class NumberMasker:
    """숫자 패턴만 마스킹하는 간단한 프로세서"""
    
    def __init__(self):
        """숫자 마스커 초기화"""
        pass
    
    def mask_numbers(self, text: str) -> str:
        """텍스트에서 숫자 패턴을 찾아 마스킹합니다."""
        try:
            # 1. 연속된 숫자 3개 이상 (전화번호, 계좌번호 등)
            text = re.sub(r'\d{3,}', '{숫자}', text)
            
            # 2. 숫자-숫자-숫자 패턴 (전화번호 형식)
            text = re.sub(r'\d+-\d+-\d+', '{숫자}', text)
            
            # 3. 숫자-숫자 패턴
            text = re.sub(r'\d+-\d+', '{숫자}', text)
            
            # 4. 숫자 공백 숫자 패턴
            text = re.sub(r'\d+\s+\d+', '{숫자}', text)
            
            return text
            
        except Exception as e:
            print(f"❌ 숫자 마스킹 중 오류 발생: {e}")
            return text
    
    def get_number_info(self, text: str) -> List[str]:
        """텍스트에서 발견된 숫자 패턴 정보를 반환합니다."""
        try:
            numbers = []
            
            # 연속된 숫자 3개 이상
            numbers.extend(re.findall(r'\d{3,}', text))
            
            # 숫자-숫자-숫자 패턴
            numbers.extend(re.findall(r'\d+-\d+-\d+', text))
            
            # 숫자-숫자 패턴
            numbers.extend(re.findall(r'\d+-\d+', text))
            
            # 숫자 공백 숫자 패턴
            numbers.extend(re.findall(r'\d+\s+\d+', text))
            
            return list(set(numbers))  # 중복 제거
            
        except Exception as e:
            print(f"❌ 숫자 정보 추출 중 오류 발생: {e}")
            return []


def anonymize_messages(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    메시지 리스트에서 숫자만 익명화합니다.
    
    Args:
        data (List[Dict[str, Any]]): 메시지 데이터 리스트
    
    Returns:
        List[Dict[str, Any]]: 숫자가 마스킹된 메시지 데이터 리스트
    """
    print("🔒 숫자 마스킹 처리 중...")
    
    # 숫자 마스커 초기화
    try:
        number_masker = NumberMasker()
        print("✅ 숫자 마스커 초기화 완료")
    except Exception as e:
        print(f"⚠️ 숫자 마스커 초기화 실패: {e}")
        print("   원본 메시지를 그대로 사용합니다.")
        return data
    
    masked_data = []
    for item in data:
        masked_item = item.copy()
        if 'message' in masked_item:
            try:
                masked_item['message'] = number_masker.mask_numbers(
                    masked_item['message']
                )
            except Exception as e:
                print(f"⚠️ 메시지 숫자 마스킹 실패: {e}")
                # 실패 시 원본 메시지 유지
                pass
        masked_data.append(masked_item)
    
    print(f"✅ 숫자 마스킹 완료: {len(data)}개 메시지")
    return masked_data
