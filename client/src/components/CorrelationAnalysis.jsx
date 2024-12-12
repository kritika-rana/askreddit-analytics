import React, { useState, useEffect } from 'react';
import { ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';

function CorrelationAnalysis() {
    const [correlationData, setCorrelationData] = useState([]);
    const [isLoading, setIsLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        fetch('/api/sentiment-correlation')
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error! Status: ${response.status}`);
                }
                return response.json();
            })
            .then(data => {
                setCorrelationData(data);
                setIsLoading(false);
            })
            .catch(error => {
                console.error('Fetch error:', error);
                setError(error.message);
                setIsLoading(false);
            });
    }, []);

    // Calculate correlation coefficient
    const calculateCorrelation = (x, y) => {
        const n = x.length;
        if (n === 0) return 0;

        const sumX = x.reduce((a, b) => a + b, 0);
        const sumY = y.reduce((a, b) => a + b, 0);
        const sumXY = x.reduce((a, b, i) => a + b * y[i], 0);
        const sumX2 = x.reduce((a, b) => a + b * b, 0);
        const sumY2 = y.reduce((a, b) => a + b * b, 0);

        const numerator = n * sumXY - sumX * sumY;
        const denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));

        return denominator === 0 ? 0 : numerator / denominator;
    };

    if (isLoading) return <div className="p-4">Loading...</div>;
    if (error) return <div className="p-4 text-red-500">Error: {error}</div>;

    // Calculate correlations
    const questionSentiments = correlationData.map(d => d.questionSentiment);
    const answerSentiments = correlationData.map(d => d.answerSentiment);
    const votes = correlationData.map(d => d.votes);
    const answerCounts = correlationData.map(d => d.answerCount);

    const correlations = {
        questionSentimentVotes: calculateCorrelation(questionSentiments, votes).toFixed(3),
        questionSentimentAnswers: calculateCorrelation(questionSentiments, answerCounts).toFixed(3),
        answerSentimentVotes: calculateCorrelation(answerSentiments, votes).toFixed(3)
    };

    return (
        <div className="max-w-6xl mx-auto p-4">
            <h2 className="text-2xl font-bold mb-6">Sentiment Correlation Analysis</h2>

            {/* Correlation Coefficients */}
            <div className="grid grid-cols-3 gap-4 mb-8">
                <div className="bg-white rounded-lg shadow p-4">
                    <h3 className="text-lg font-semibold mb-2">Question Sentiment vs Votes</h3>
                    <p className="text-3xl font-bold text-blue-600">{correlations.questionSentimentVotes}</p>
                    <p className="text-sm text-gray-600 mt-2">Pearson Correlation Coefficient</p>
                    <p className="text-xs text-gray-500 mt-1">
                        Range: -1 (perfect negative) to +1 (perfect positive)
                    </p>
                </div>
                <div className="bg-white rounded-lg shadow p-4">
                    <h3 className="text-lg font-semibold mb-2">Question Sentiment vs Answers Count</h3>
                    <p className="text-3xl font-bold text-blue-600">{correlations.questionSentimentAnswers}</p>
                    <p className="text-sm text-gray-600 mt-2">Pearson Correlation Coefficient</p>
                    <p className="text-xs text-gray-500 mt-1">
                        Range: -1 (perfect negative) to +1 (perfect positive)
                    </p>
                </div>
                <div className="bg-white rounded-lg shadow p-4">
                    <h3 className="text-lg font-semibold mb-2">Avg Answer Sentiment vs Votes</h3>
                    <p className="text-3xl font-bold text-blue-600">{correlations.answerSentimentVotes}</p>
                    <p className="text-sm text-gray-600 mt-2">Pearson Correlation Coefficient</p>
                    <p className="text-xs text-gray-500 mt-1">
                        Range: -1 (perfect negative) to +1 (perfect positive)
                    </p>
                </div>
            </div>

            {/* Scatter Plots */}
            <div className="grid grid-cols-2 gap-4">
                {/* Question Sentiment vs Votes */}
                <div className="bg-white rounded-lg shadow p-4">
                    <h3 className="text-lg font-semibold mb-4">Question Sentiment vs Votes</h3>
                    <div className="h-80">
                        <ResponsiveContainer width="100%" height="100%">
                            <ScatterChart margin={{top: 20, right: 20, bottom: 20, left: 20}}>
                                <CartesianGrid/>
                                <XAxis
                                    type="number"
                                    dataKey="questionSentiment"
                                    name="Sentiment"
                                    label={{value: 'Question Sentiment', position: 'bottom'}}
                                />
                                <YAxis
                                    type="number"
                                    dataKey="votes"
                                    name="Votes"
                                    label={{value: 'Votes', angle: -90, position: 'left'}}
                                />
                                <Tooltip cursor={{strokeDasharray: '3 3'}}/>
                                <Scatter data={correlationData} fill="#8884d8"/>
                            </ScatterChart>
                        </ResponsiveContainer>
                    </div>
                </div>

                {/* Avg Answer Sentiment vs Votes */}
                <div className="bg-white rounded-lg shadow p-4">
                    <h3 className="text-lg font-semibold mb-4">Avg Answer Sentiment vs Votes</h3>
                    <div className="h-80">
                        <ResponsiveContainer width="100%" height="100%">
                            <ScatterChart margin={{ top: 20, right: 20, bottom: 20, left: 20 }}>
                                <CartesianGrid />
                                <XAxis
                                    type="number"
                                    dataKey="answerSentiment"
                                    name="Sentiment"
                                    label={{ value: 'Avg Answer Sentiment', position: 'bottom' }}
                                />
                                <YAxis
                                    type="number"
                                    dataKey="votes"
                                    name="Votes"
                                    label={{ value: 'Votes', angle: -90, position: 'left' }}
                                />
                                <Tooltip cursor={{ strokeDasharray: '3 3' }} />
                                <Scatter data={correlationData} fill="#82ca9d" />
                            </ScatterChart>
                        </ResponsiveContainer>
                    </div>
                </div>
            </div>
        </div>
    );
}

export default CorrelationAnalysis;