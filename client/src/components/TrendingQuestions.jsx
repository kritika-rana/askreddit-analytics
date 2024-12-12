import React, { useState, useEffect } from 'react';
import SentimentDiversity from './SentimentDiversity';
import AnswerList from './AnswerList';

function TrendingQuestions() {
    const [questions, setQuestions] = useState([]);
    const [isLoading, setIsLoading] = useState(true);
    const [error, setError] = useState(null);
    const [selectedQuestion, setSelectedQuestion] = useState(null);
    const [sentimentStats, setSentimentStats] = useState(null);
    const [showingAnswers, setShowingAnswers] = useState(false);
    const [selectedQuestionId, setSelectedQuestionId] = useState(null);

    useEffect(() => {
        fetch('/api/trending-questions')
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error! Status: ${response.status}`);
                }
                return response.json();
            })
            .then(data => {
                setQuestions(data);
                setIsLoading(false);
            })
            .catch(error => {
                console.error('Fetch error:', error);
                setError(error.message);
                setIsLoading(false);
            });
    }, []);

    const fetchSentimentStats = async (questionId) => {
        try {
            const response = await fetch(`/api/questions/${questionId}`);
            if (!response.ok) {
                throw new Error(`HTTP error! Status: ${response.status}`);
            }
            const stats = await response.json();
            setSentimentStats(stats);
            setSelectedQuestion(questionId);
        } catch (error) {
            console.error('Error fetching sentiment stats:', error);
        }
    };

    const handleViewAnswers = (questionId) => {
        setSelectedQuestionId(questionId);
        setShowingAnswers(true);
    };

    const formatSentimentScore = (score) => {
        return score ? score.toFixed(2) : 'N/A';
    };

    if (isLoading) return <div className="p-4">Loading...</div>;
    if (error) return <div className="p-4 text-red-500">Error: {error}</div>;

    if (showingAnswers) {
        return <AnswerList
            questionId={selectedQuestionId}
            onBack={() => setShowingAnswers(false)}
        />;
    }

    return (
        <div className="max-w-6xl mx-auto p-4">
            <h2 className="text-2xl font-bold mb-6">Top 5 Trending Questions</h2>
            <div className="space-y-4">
                {questions.map(question => (
                    <div key={question.id} className="bg-white rounded-lg shadow-md p-6 border border-gray-200">
                        <h3 className="text-xl font-semibold mb-4">{question.text}</h3>
                        <div className="grid md:grid-cols-2 gap-6">
                            <div>
                                <div className="space-y-2 text-sm text-gray-600">
                                    <p><span className="font-medium">Votes:</span> {question.votes}</p>
                                    <p><span className="font-medium">Sentiment Score:</span> {formatSentimentScore(question.sentimentScore)}</p>
                                    <p><span className="font-medium">Avg Answer Sentiment:</span> {formatSentimentScore(question.avgAnswerSentiment)}</p>
                                    <p><span className="font-medium">Date:</span> {new Date(question.datetime).toLocaleDateString()}</p>
                                </div>
                                <div className="mt-4 space-x-4">
                                    <button
                                        onClick={() => fetchSentimentStats(question.id)}
                                        className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors"
                                    >
                                        Analyze Answer Sentiments
                                    </button>
                                    <button
                                        onClick={() => handleViewAnswers(question.id)}
                                        className="px-4 py-2 bg-green-600 text-white rounded-md hover:bg-green-700 transition-colors"
                                    >
                                        View Answers
                                    </button>
                                </div>
                            </div>

                            {selectedQuestion === question.id && sentimentStats && (
                                <SentimentDiversity sentimentStats={sentimentStats} />
                            )}
                        </div>
                    </div>
                ))}
            </div>
        </div>
    );
}

export default TrendingQuestions;