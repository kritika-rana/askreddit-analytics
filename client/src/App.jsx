import React, { useState } from 'react';
import TrendingQuestions from './components/TrendingQuestions';
import SentimentCorrelation from './components/CorrelationAnalysis';
import CorrelationAnalysis from "./components/CorrelationAnalysis";

function App() {
    const [activeTab, setActiveTab] = useState('trending');

    return (
        <div className="min-h-screen bg-gray-50">
            <div className="max-w-7xl mx-auto py-8 px-4 sm:px-6 lg:px-8">
                <h1 className="text-4xl font-bold text-gray-900 mb-8 text-center border-b pb-4">
                    AskReddit Analytics Platform
                </h1>

                {/* Tab Navigation */}
                <div className="flex border-b border-gray-200 mb-6">
                    <button
                        className={`px-6 py-3 font-medium text-sm mr-4 border-b-2 ${
                            activeTab === 'trending'
                                ? 'border-blue-500 text-blue-600'
                                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                        }`}
                        onClick={() => setActiveTab('trending')}
                    >
                        Trending Questions
                    </button>
                    <button
                        className={`px-6 py-3 font-medium text-sm border-b-2 ${
                            activeTab === 'correlation'
                                ? 'border-blue-500 text-blue-600'
                                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                        }`}
                        onClick={() => setActiveTab('correlation')}
                    >
                        Sentiment-Engagement Correlation Analysis
                    </button>
                </div>

                {/* Tab Content */}
                <div>
                    {activeTab === 'trending' ? (
                        <TrendingQuestions />
                    ) : (
                        <CorrelationAnalysis />
                    )}
                </div>
            </div>
        </div>
    );
}

export default App;