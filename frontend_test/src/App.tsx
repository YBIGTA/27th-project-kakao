import React from 'react';
import './App.css';
import GiftRecommender from './components/GiftRecommender';

function App() {
  return (
    <div className="App">
      <header className="App-header">
        <div className="logo-container">
          <img src="/images/kakao-gift-logo.png" alt="카카오 선물하기 로고" className="kakao-logo" />
        </div>
        <h1>카카오 선물 추천</h1>
        <p>카카오톡 대화를 분석해서 맞춤형 선물을 추천해드려요!</p>
      </header>
      <main>
        <GiftRecommender />
      </main>
    </div>
  );
}

export default App;
