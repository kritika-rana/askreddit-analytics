import React, { useState, useEffect } from 'react';

export default function AnswerList({ questionId, onBack }) {
    const [answers, setAnswers] = useState([]);
    const [isLoading, setIsLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        fetch(`/api/question-answers/${questionId}`)
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error! Status: ${response.status}`);
                }
                return response.json();
            })
            .then(data => {
                setAnswers(data);
                setIsLoading(false);
            })
            .catch(error => {
                console.error('Fetch error:', error);
                setError(error.message);
                setIsLoading(false);
            });
    }, [questionId]);

    const formatSentimentScore = (score) => {
        return score ? score.toFixed(2) : 0;
    };

    if (isLoading) return <div className="p-4">Loading...</div>;
    if (error) return <div className="p-4 text-red-500">Error: {error}</div>;

    return (
        <div className="max-w-6xl mx-auto p-4">
            <button
                onClick={onBack}
                className="mb-6 px-4 py-2 bg-gray-600 text-white rounded-md hover:bg-gray-700 transition-colors flex items-center"
            >
                ← Back to Trending Questions
            </button>

            <h2 className="text-2xl font-bold mb-6">Answers</h2>

            <div className="space-y-4 max-h-[70vh] overflow-y-auto pr-2">
                {answers.map(answer => (
                    <div key={answer.answerId} className="bg-white rounded-lg shadow-md p-6 border border-gray-200">
                        <div className="space-y-4">
                            <p className="text-lg">{answer.text}</p>
                            <div className="flex space-x-6 text-sm text-gray-600">
                                <p><span className="font-medium">Votes:</span> {answer.votes}</p>
                                <p><span className="font-medium">Sentiment Score:</span> {formatSentimentScore(answer.sentimentScore)}</p>
                            </div>
                        </div>
                    </div>
                ))}
            </div>
        </div>
    );
}