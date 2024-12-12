import React from 'react';
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from 'recharts';

const SENTIMENT_COLORS = {
    highlyPositive: '#4CAF50',  // Green
    positive: '#8BC34A',        // Light green
    neutral: '#FFC107',         // Yellow
    negative: '#FF5722',        // Orange
    highlyNegative: '#F44336'   // Red
};

function SentimentDiversity({ sentimentStats }) {
    return (
        <div className="grid grid-cols-2 gap-4">
            <div className="h-48">
                <ResponsiveContainer width="100%" height="100%">
                    <PieChart>
                        <Pie
                            data={[
                                { name: 'Highly Positive', value: sentimentStats.distribution.highlyPositive },
                                { name: 'Positive', value: sentimentStats.distribution.positive },
                                { name: 'Neutral', value: sentimentStats.distribution.neutral },
                                { name: 'Negative', value: sentimentStats.distribution.negative },
                                { name: 'Highly Negative', value: sentimentStats.distribution.highlyNegative }
                            ]}
                            cx="50%"
                            cy="50%"
                            innerRadius={30}
                            outerRadius={60}
                            dataKey="value"
                        >
                            <Cell fill={SENTIMENT_COLORS.highlyPositive} />
                            <Cell fill={SENTIMENT_COLORS.positive} />
                            <Cell fill={SENTIMENT_COLORS.neutral} />
                            <Cell fill={SENTIMENT_COLORS.negative} />
                            <Cell fill={SENTIMENT_COLORS.highlyNegative} />
                        </Pie>
                        <Tooltip />
                    </PieChart>
                </ResponsiveContainer>
            </div>

            <div className="text-sm">
                <p className="font-medium">Total Answers: {sentimentStats.totalAnswers}</p>
                <p className="font-medium">Standard Deviation: {sentimentStats.stdDev ? sentimentStats.stdDev.toFixed(3) : 'N/A'}</p>
                <div className="mt-4">
                    <p className="font-semibold mb-2">Distribution:</p>
                    <div className="space-y-1">
                        <p>
                            <span
                                className="inline-block w-4 h-4 mr-2 rounded-full align-middle"
                                style={{ backgroundColor: SENTIMENT_COLORS.highlyPositive }}
                            />
                            Highly Positive: {sentimentStats.distribution.highlyPositive}
                        </p>
                        <p>
                            <span
                                className="inline-block w-4 h-4 mr-2 rounded-full align-middle"
                                style={{ backgroundColor: SENTIMENT_COLORS.positive }}
                            />
                            Positive: {sentimentStats.distribution.positive}
                        </p>
                        <p>
                            <span
                                className="inline-block w-4 h-4 mr-2 rounded-full align-middle"
                                style={{ backgroundColor: SENTIMENT_COLORS.neutral }}
                            />
                            Neutral: {sentimentStats.distribution.neutral}
                        </p>
                        <p>
                            <span
                                className="inline-block w-4 h-4 mr-2 rounded-full align-middle"
                                style={{ backgroundColor: SENTIMENT_COLORS.negative }}
                            />
                            Negative: {sentimentStats.distribution.negative}
                        </p>
                        <p>
                            <span
                                className="inline-block w-4 h-4 mr-2 rounded-full align-middle"
                                style={{ backgroundColor: SENTIMENT_COLORS.highlyNegative }}
                            />
                            Highly Negative: {sentimentStats.distribution.highlyNegative}
                        </p>
                    </div>
                </div>
            </div>
        </div>
    );
}

export default SentimentDiversity;