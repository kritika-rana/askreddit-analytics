const express = require('express');
const app = express();
const hbase = require('hbase');

const url = new URL(process.argv[3]);
require('dotenv').config()
const port = Number(process.argv[2]);

var hclient = hbase({
    host: url.hostname,
    path: url.pathname ?? "/",
    port: url.port,
    protocol: url.protocol.slice(0, -1), // Don't want the colon
    encoding: 'latin1',
    auth: process.env.HBASE_AUTH
});

// Helper functions
function counterToNumber(c) {
    return Number(Buffer.from(c, 'latin1').readBigInt64BE());
}

const columnConfig = {
    // Specify how each column family should be processed
    engagement: 'number',
    sentiment: 'number',
    answer: 'number',
    metadata: 'string'
};

function rowToMap(row) {
    // If we're processing multiple rows, each will have {key, column, $} structure
    var stats = {};

    if (Array.isArray(row)) {
        // If it's an array of column values for a single row
        row.forEach(item => {
            if (item.column && item.$) {
                // Get the column name after the family name (e.g., 'votes' from 'engagement:votes')
                const columnName = item.column.split(':')[1];
                try {
                    // Handle specific fields differently based on columnConfig
                    if (['avg_sentiment', 'stddev', 'score'].includes(columnName)) {
                        // Parse as double
                        stats[columnName] = Buffer.from(item.$, 'latin1').readDoubleBE();
                    } else if (['text', 'datetime'].includes(columnName)) {
                        // Parse as string
                        stats[columnName] = item.$.toString('latin1');
                    } else {
                        // Default to number
                        stats[columnName] = counterToNumber(item.$);
                    }
                } catch (e) {
                    // If parsing fails, treat it as a string value
                    stats[columnName] = item.$.toString('latin1');
                }
            }
        });
    } else if (row.column && row.$) {
        // Single column value case
        const columnName = row.column.split(':')[1];
        try {
            if (['avg_sentiment', 'stddev', 'score'].includes(columnName)) {
                stats[columnName] = Buffer.from(row.$, 'latin1').readDoubleBE();
            } else if (['text', 'datetime'].includes(columnName)) {
                stats[columnName] = row.$.toString('latin1');
            } else {
                stats[columnName] = counterToNumber(row.$);
            }
        } catch (e) {
            stats[columnName] = row.$.toString('latin1');
        }
    }

    // Add the row key if present
    if (row.key) {
        stats.id = row.key;
    }

    return stats;
}



// 1. Trending Questions API
app.get('/api/trending-questions', async (req, res) => {
    hclient.table('kritikarana_trending_questions_hbase').scan({}, async (err, rows) => {
        if (err) {
            console.error('Error fetching trending question IDs:', err);
            return res.status(500).send(err);
        }

        if (!rows || rows.length === 0) {
            return res.json([]);
        }

        // Extract IDs from trending questions
        const questionIds = rows.map(row => row.key); // Get row keys (IDs)

        // Fetch details for each ID from `kritikarana_questions_hbase`
        const questionsWithDetails = await Promise.all(
            questionIds.map(id => {
                return new Promise((resolve, reject) => {
                    hclient.table('kritikarana_questions_hbase')
                        .row(id)
                        .get((err, row) => {
                            if (err) {
                                console.error(`Error fetching details for ID ${id}:`, err);
                                return resolve(null); // Skip if there's an error
                            }
                            if (!row) {
                                console.warn(`No data found for ID ${id}`);
                                return resolve(null); // Skip if no data
                            }
                            const details = rowToMap(row);
                            resolve({
                                id,
                                text: details.text,
                                datetime: details.datetime,
                                votes: details.votes,
                                avgAnswerSentiment: details.avg_sentiment,
                                sentimentScore: details.score,
                            });
                        });
                });
            })
        );

        res.json(questionsWithDetails);
    });
});




// 2. Get answer sentiment stats for question
app.get('/api/questions/:questionId', async (req, res) => {
    hclient.table('kritikarana_questions_hbase')
        .row(req.params.questionId)
        .get((err, value) => {
            if (err) return res.status(500).send(err);

            const stats = rowToMap(value);
            res.json({
                questionId: req.params.questionId,
                distribution: {
                    highlyPositive: stats.highly_positive,
                    positive: stats.positive,
                    neutral: stats.neutral,
                    negative: stats.negative,
                    highlyNegative: stats.highly_negative
                },
                stdDev: stats.stddev,
                totalAnswers: stats.total_answers
            });
        });
});

// 3. Question Answers API
app.get('/api/question-answers/:questionId', async (req, res) => {
    const { questionId } = req.params;

    // Create a scanner for the HBase table
    const scanner = hclient.table('kritikarana_answers_hbase').scan({
        startRow: questionId + ':',     // Start from first row with this prefix
        endRow: questionId + ':\xff',   // End at last possible row with this prefix
        maxVersions: 1                  // Only get latest version of each row
    });

    const answersMap = {}; // Accumulate answers here

    // Process rows using event emitter pattern
    scanner.on('data', (row) => {
        const data = rowToMap(row); // Map row data
        const [qId, answerId] = row.key.toString().split(':');

        if (!answersMap[answerId]) {
            answersMap[answerId] = { questionId: qId, answerId: answerId };
        }

        // Merge data into the corresponding answer dictionary
        Object.assign(answersMap[answerId], {
            text: data.text || answersMap[answerId].text, // Add text
            votes: data.votes || answersMap[answerId].votes, // Add votes
            sentimentScore: data.score || answersMap[answerId].sentimentScore // Add sentiment score
        });
    });

    scanner.on('error', (err) => {
        console.error('Error scanning answers:', err);
        res.status(500).send('Error retrieving answers');
    });

    scanner.on('end', () => {
        const answers = Object.values(answersMap); // Convert map to array
        if (answers.length === 0) {
            console.warn(`No answers found for questionId: ${questionId}`);
        }
        res.json(answers);
    });
});



// 4. Correlation Data API
app.get('/api/sentiment-correlation', async (req, res) => {
    const SAMPLE_SIZE = 1000; // Adjust this based on your needs
    const totalRows = 189565;
    const samplingRate = SAMPLE_SIZE / totalRows;

    try {
        const scanner = hclient.table('kritikarana_questions_hbase').scan({
            filter: {
                type: 'RandomRowFilter',
                chance: samplingRate
            }
        });

        const rows = [];

        scanner.on('readable', function() {
            let row;
            while ((row = scanner.read())) {
                rows.push(row);
            }
        });

        scanner.on('error', function(err) {
            console.error('Error scanning HBase table:', err);
            res.status(500).send('Error retrieving data from HBase');
        });

        scanner.on('end', function() {
            console.log(`Sampled ${rows.length} rows from ${totalRows} total rows`);

            const correlationData = rows
                .map(rowToMap)
                .map(q => ({
                    questionSentiment: q.score || 0,
                    answerSentiment: q.avg_sentiment || 0,
                    votes: q.votes || 0,
                    answerCount: q.total_answers || 0
                }))
                .filter(data =>
                    data.questionSentiment !== 0 ||
                    data.answerSentiment !== 0 ||
                    data.votes !== 0 ||
                    data.answerCount !== 0
                );

            res.json(correlationData);
        });

    } catch (error) {
        console.error('Unexpected error in /api/sentiment-correlation:', error);
        res.status(500).send('Unexpected error while processing request');
    }
});


app.listen(port);