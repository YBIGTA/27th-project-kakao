import React, { useState } from 'react';
import './GiftRecommender.css';
import axios from 'axios';

interface FormData {
  file: File | null;
  selectedUser: string;
  age: number;
  gender: string;
  relation: string;
  budget_min: number;
  budget_max: number;
}

interface Selection {
  title?: string;
  product_name?: string;
  category_child?: string;
  sub_category?: string;
  url?: string;
  product_url?: string | null;
  brand?: string | null;
  price: number | null;
  rationale?: string;
  reason?: string | null;
}

interface Analysis {
  top3_children?: string[];
  detailed_reasoning?: string[];
  subcats?: string[];
  evidence_by_cat?: { [key: string]: string[] };
  message?: string;
}

interface RecommendationResult {
  success?: boolean;
  message?: string;
  data?: {
    user_context: {
      age: number;
      gender: string;
      relation: string;
      budget_min: number;
      budget_max: number;
    };
    analysis: Analysis;
    products: {
      selected_products: Selection[];
    };
  };
  profile?: {
    age: number;
    gender: string;
    relation: string;
    budget_min: number;
    budget_max: number;
  };
  analysis?: Analysis;
  products?: {
    selected_products: Selection[];
  };
  selections?: Selection[];
}

const GiftRecommender: React.FC = () => {
  const [formData, setFormData] = useState<FormData>({
    file: null,
    selectedUser: '전체',
    age: 0,
    gender: '여',
    relation: '',
    budget_min: 0,
    budget_max: 0
  });

  const [isLoading, setIsLoading] = useState(false);
  const [result, setResult] = useState<RecommendationResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [availableUsers, setAvailableUsers] = useState<string[]>(['전체']);
  const [selectedCategory, setSelectedCategory] = useState<string>('전체');

  // 선택된 카테고리에 따라 상품 필터링
  const filteredProducts = React.useMemo(() => {
    if (!result) return [];
    
    const products = result.data?.products?.selected_products || result.products?.selected_products || [];
    
    if (selectedCategory === '전체') {
      return products;
    }
    
    return products.filter(product => 
      product.category_child === selectedCategory || 
      product.sub_category === selectedCategory
    );
  }, [result, selectedCategory]);

  const handleFileChange = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file && (file.type === 'text/plain' || file.name.endsWith('.csv'))) {
      setFormData(prev => ({ ...prev, file }));
      setError(null);
      
      // 파일에서 사용자명 추출
      try {
        const users = await extractUsersFromFile(file);
        setAvailableUsers(['전체', ...users]);
        setFormData(prev => ({ ...prev, selectedUser: '전체' }));
      } catch (err) {
        console.error('사용자명 추출 실패:', err);
        setAvailableUsers(['전체']);
      }
    } else {
      setError('텍스트 파일(.txt) 또는 CSV 파일(.csv)만 업로드 가능합니다.');
    }
  };

  const extractUsersFromFile = async (file: File): Promise<string[]> => {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = (e) => {
        try {
          const content = e.target?.result as string;
          const users = new Set<string>();
          
          if (file.name.endsWith('.csv')) {
            // CSV 파일 처리
            const lines = content.split('\n');
            if (lines.length > 0) {
              const headers = lines[0].split(',');
              const userIndex = headers.findIndex(h => 
                h.toLowerCase().includes('user') || 
                h.toLowerCase().includes('사용자') ||
                h.toLowerCase().includes('name') ||
                h.toLowerCase().includes('이름')
              );
              
              if (userIndex !== -1) {
                for (let i = 1; i < Math.min(lines.length, 100); i++) { // 처음 100줄만 확인
                  const values = lines[i].split(',');
                  if (values[userIndex]) {
                    const user = values[userIndex].trim().replace(/"/g, '');
                    if (user && user !== 'User' && user !== '사용자') {
                      users.add(user);
                    }
                  }
                }
              }
            }
          } else {
            // TXT 파일 처리 (카카오톡 형식)
            const lines = content.split('\n');
            for (let i = 0; i < Math.min(lines.length, 100); i++) { // 처음 100줄만 확인
              const line = lines[i];
              // 카카오톡 형식: "2023-01-01 12:00:00, 사용자명, 메시지"
              const match = line.match(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\s*([^,]+),/);
              if (match) {
                const user = match[1].trim();
                if (user && user !== 'User' && user !== '사용자') {
                  users.add(user);
                }
              }
            }
          }
          
          resolve(Array.from(users));
        } catch (err) {
          reject(err);
        }
      };
      reader.onerror = reject;
      reader.readAsText(file);
    });
  };

  const handleInputChange = (event: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
    const { name, value } = event.target;
    setFormData(prev => ({
      ...prev,
      [name]: name === 'age' || name === 'budget_min' || name === 'budget_max' 
        ? parseInt(value) 
        : value
    }));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!formData.file) {
      setError('파일을 선택해주세요.');
      return;
    }

    setIsLoading(true);
    setError('');
    setResult(null);

    // 진행 상황 메시지
    const progressInterval = setInterval(() => {
      console.log('분석 중... 잠시만 기다려주세요.');
    }, 10000); // 10초마다 메시지

    try {
      const formDataToSend = new FormData();
      formDataToSend.append('file', formData.file);
      formDataToSend.append('selected_user', formData.selectedUser);
      formDataToSend.append('age', formData.age.toString());
      formDataToSend.append('gender', formData.gender);
      formDataToSend.append('relation', formData.relation);
      formDataToSend.append('budget_min', formData.budget_min.toString());
      formDataToSend.append('budget_max', formData.budget_max.toString());

      const response = await axios.post('/recommendations', formDataToSend, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
        timeout: 300000, // 5분 타임아웃
      });

      // 응답 데이터 검증
      if (!response.data) {
        throw new Error('서버에서 응답 데이터를 받지 못했습니다.');
      }

      // 성공 여부 확인
      if (!response.data.success) {
        throw new Error(response.data.message || '분석 중 오류가 발생했습니다.');
      }

      setResult(response.data);
      console.log('추천 결과:', response.data);
      console.log('응답 구조 확인:', {
        hasData: !!response.data,
        hasDataData: !!response.data?.data,
        topLevelKeys: Object.keys(response.data || {}),
        dataKeys: Object.keys(response.data?.data || {})
      });
    } catch (err: any) {
      console.error('API 호출 오류:', err);
      
      if (err.response?.status === 422) {
        setError('분석할 수 있는 충분한 대화 내용이 없습니다. 더 많은 대화를 포함한 파일을 업로드해주세요.');
      } else if (err.code === 'ECONNABORTED') {
        setError('요청 시간이 초과되었습니다. 잠시 후 다시 시도해주세요.');
      } else if (err.response?.status === 500) {
        setError('서버 내부 오류가 발생했습니다. 잠시 후 다시 시도해주세요.');
      } else if (err.message) {
        setError(err.message);
      } else {
        setError('추천 요청 중 오류가 발생했습니다. 다시 시도해주세요.');
      }
    } finally {
      clearInterval(progressInterval);
      setIsLoading(false);
    }
  };

  return (
    <div className="gift-recommender">
      <div className="form-container">
        <h2>선물 추천을 위한 정보 입력</h2>
        
        <form onSubmit={handleSubmit} className="recommendation-form">
          {/* 파일 업로드 */}
          <div className="form-group">
            <label htmlFor="file">카카오톡 대화 파일 (TXT/CSV)</label>
            <input
              type="file"
              id="file"
              name="file"
              accept=".txt,.csv"
              onChange={handleFileChange}
              required
            />
            <small>카카오톡에서 내보낸 텍스트 파일 또는 CSV 파일을 업로드해주세요.</small>
          </div>

          {/* 사용자 선택 */}
          <div className="form-group">
            <label htmlFor="selectedUser">분석할 사용자</label>
            <select
              id="selectedUser"
              name="selectedUser"
              value={formData.selectedUser}
              onChange={handleInputChange}
              required
            >
              {availableUsers.map(user => (
                <option key={user} value={user}>
                  {user === '전체' ? '전체 사용자' : user}
                </option>
              ))}
            </select>
            <small>
              {availableUsers.length > 1 
                ? `파일에서 발견된 사용자: ${availableUsers.slice(1).join(', ')}`
                : '파일을 업로드하면 사용자가 자동으로 감지됩니다.'
              }
            </small>
          </div>

          {/* 나이 */}
          <div className="form-group">
            <label htmlFor="age">나이</label>
            <div className="age-input-container">
              <input
                type="number"
                id="age"
                name="age"
                value={formData.age || ''}
                onChange={handleInputChange}
                min="0"
                max="120"
                placeholder="00"
                required
              />
              <span className="age-unit">세</span>
            </div>
          </div>

          {/* 성별 */}
          <div className="form-group">
            <label htmlFor="gender">성별</label>
            <select
              id="gender"
              name="gender"
              value={formData.gender}
              onChange={handleInputChange}
              required
            >
              <option value="여">여성</option>
              <option value="남">남성</option>
            </select>
          </div>

          {/* 관계 */}
          <div className="form-group">
            <label htmlFor="relation">관계</label>
            <input
              type="text"
              id="relation"
              name="relation"
              value={formData.relation}
              onChange={handleInputChange}
              placeholder="예) 선생님, 부모님, 친구 등"
              required
            />
          </div>

          {/* 예산 범위 */}
          <div className="form-group budget-group">
            <label>예산 범위</label>
            <div className="budget-inputs">
              <div className="budget-input-container">
                <input
                  type="number"
                  name="budget_min"
                  value={formData.budget_min || ''}
                  onChange={handleInputChange}
                  placeholder="최소 예산"
                  min="0"
                  required
                />
                <span className="budget-unit">원</span>
              </div>
              <span className="budget-separator">~</span>
              <div className="budget-input-container">
                <input
                  type="number"
                  name="budget_max"
                  value={formData.budget_max || ''}
                  onChange={handleInputChange}
                  placeholder="최대 예산"
                  min="0"
                  required
                />
                <span className="budget-unit">원</span>
              </div>
            </div>
          </div>

          {/* 에러 메시지 */}
          {error && (
            <div className="error-message">
              {error}
            </div>
          )}

          {/* 제출 버튼 */}
          <button 
            type="submit" 
            className="submit-btn"
            disabled={isLoading}
          >
            {isLoading ? '분석 중...' : '선물 추천 받기'}
          </button>
        </form>
      </div>

      {/* 결과 표시 */}
      {result && (
        <div className="result-container">
          <h3>🎁 추천 선물</h3>
          
          {/* 디버깅 정보 */}
          <div style={{background: '#f0f0f0', padding: '10px', margin: '10px 0', fontSize: '12px'}}>
            <strong>디버깅:</strong> 
            {result.success ? '성공' : '실패'} | 
            {result.message || '메시지 없음'} |
            상품 수: {result.data?.products?.selected_products?.length || result.products?.selected_products?.length || 0}
          </div>
          
          <div className="top-categories">
            <h4>분석된 관심 카테고리</h4>
            <div className="category-tags">
              <span 
                key="전체" 
                className={`category-tag ${selectedCategory === '전체' ? 'selected' : ''}`}
                onClick={() => setSelectedCategory('전체')}
              >
                전체
              </span>
              {(result.data?.analysis?.top3_children || result.analysis?.top3_children || []).map((category, index) => (
                <span 
                  key={index} 
                  className={`category-tag ${selectedCategory === category ? 'selected' : ''}`}
                  onClick={() => setSelectedCategory(category)}
                >
                  {category}
                </span>
              ))}
            </div>
          </div>

          <div className="recommended-products">
            <h4>추천 상품 {selectedCategory !== '전체' && `(${selectedCategory})`}</h4>
            {filteredProducts.length === 0 ? (
              <p className="no-products">선택한 카테고리에 해당하는 상품이 없습니다.</p>
            ) : (
              filteredProducts.map((product, index) => (
                <div key={index} className="product-card" onClick={() => {
                  const url = product.url || product.product_url;
                  if (url) {
                    window.open(url, '_blank');
                  }
                }}>
                  <h5>{product.title || product.product_name || '상품명 없음'}</h5>
                  {product.price && (
                    <p className="product-price">{product.price.toLocaleString()}원</p>
                  )}
                  <p className="product-category">{product.category_child || product.sub_category || '카테고리 없음'}</p>
                  {product.rationale && (
                    <p className="product-reason">{product.rationale}</p>
                  )}
                  {(product.url || product.product_url) && (
                    <div className="product-link-hint">클릭하면 상품 페이지로 이동합니다</div>
                  )}
                </div>
              ))
            )}
          </div>


        </div>
      )}
    </div>
  );
};

export default GiftRecommender;
