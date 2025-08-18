import React, { useState } from 'react';
import './GiftRecommender.css';
import axios from 'axios';

interface FormData {
  file: File | null;
  age: number;
  gender: string;
  relation: string;
  budget_min: number;
  budget_max: number;
}

interface Selection {
  sub_category: string;
  product_name: string;
  product_url: string | null;
  brand: string | null;
  price: number | null;
  reason: string | null;
}

interface Analysis {
  subcats: string[];
  evidence_by_cat: { [key: string]: string[] };
  message?: string;
}

interface RecommendationResult {
  profile: {
    age: number;
    gender: string;
    relation: string;
    budget_min: number;
    budget_max: number;
  };
  analysis: Analysis;
  selections: Selection[];
}

const GiftRecommender: React.FC = () => {
  const [formData, setFormData] = useState<FormData>({
    file: null,
    age: 0,
    gender: '여',
    relation: '',
    budget_min: 0,
    budget_max: 0
  });

  const [isLoading, setIsLoading] = useState(false);
  const [result, setResult] = useState<RecommendationResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file && (file.type === 'text/plain' || file.name.endsWith('.csv'))) {
      setFormData(prev => ({ ...prev, file }));
      setError(null);
    } else {
      setError('텍스트 파일(.txt) 또는 CSV 파일(.csv)만 업로드 가능합니다.');
    }
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

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    
    if (!formData.file) {
      setError('카카오톡 대화 파일을 업로드해주세요.');
      return;
    }

    if (!formData.relation) {
      setError('관계를 입력해주세요.');
      return;
    }

    if (formData.age === 0) {
      setError('나이를 입력해주세요.');
      return;
    }

    if (formData.budget_min === 0 && formData.budget_max === 0) {
      setError('예산 범위를 입력해주세요.');
      return;
    }

    if (formData.budget_min > formData.budget_max) {
      setError('최소 예산은 최대 예산보다 작거나 같아야 합니다.');
      return;
    }

    setIsLoading(true);
    setError(null);

    try {
      const formDataToSend = new FormData();
      formDataToSend.append('file', formData.file);
      formDataToSend.append('age', formData.age.toString());
      formDataToSend.append('gender', formData.gender);
      formDataToSend.append('relation', formData.relation);
      formDataToSend.append('budget_min', formData.budget_min.toString());
      formDataToSend.append('budget_max', formData.budget_max.toString());

      const response = await axios.post('/v1/recommendations', formDataToSend, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
        timeout: 120000, // 2분 타임아웃
      });

      setResult(response.data);
    } catch (err: any) {
      if (err.response?.status === 422) {
        setError('분석할 수 있는 충분한 대화 내용이 없습니다. 더 많은 대화를 포함한 파일을 업로드해주세요.');
      } else if (err.code === 'ECONNABORTED') {
        setError('요청 시간이 초과되었습니다. 잠시 후 다시 시도해주세요.');
      } else {
        setError('추천 요청 중 오류가 발생했습니다. 다시 시도해주세요.');
      }
    } finally {
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
          
          <div className="top-categories">
            <h4>분석된 관심 카테고리</h4>
            <div className="category-tags">
              {result.analysis.subcats.map((category, index) => (
                <span key={index} className="category-tag">{category}</span>
              ))}
            </div>
          </div>

          <div className="recommended-products">
            <h4>추천 상품</h4>
            {result.selections.map((product, index) => (
              <div key={index} className="product-card">
                <h5>{product.product_name}</h5>
                {product.price && (
                  <p className="product-price">{product.price.toLocaleString()}원</p>
                )}
                <p className="product-category">{product.sub_category}</p>
                {product.reason && (
                  <p className="product-reason">{product.reason}</p>
                )}
                {product.product_url && (
                  <a 
                    href={product.product_url} 
                    target="_blank" 
                    rel="noopener noreferrer"
                    className="product-link"
                  >
                    상품 보기
                  </a>
                )}
              </div>
            ))}
          </div>

          <div className="evidence-sentences">
            <h4>분석 근거 문장</h4>
            <ul>
              {Object.entries(result.analysis.evidence_by_cat).map(([category, sentences]) => (
                sentences.map((sentence, index) => (
                  <li key={`${category}-${index}`}>
                    <strong>{category}:</strong> {sentence}
                  </li>
                ))
              ))}
            </ul>
          </div>
        </div>
      )}
    </div>
  );
};

export default GiftRecommender;
