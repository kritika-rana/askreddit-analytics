'use strict';

const axios = require('axios');
const kafka = require('kafka-node');
require('dotenv').config();

// Reddit API configuration
const REDDIT_API_BASE = 'https://oauth.reddit.com';
const SUBREDDIT = 'AskReddit';
const POLL_INTERVAL = 5 * 60 * 1000; // 5 minutes

// OAuth configuration
const CLIENT_ID = process.env.REDDIT_CLIENT_ID;
const CLIENT_SECRET = process.env.REDDIT_CLIENT_SECRET;

// Kafka setup
const Producer = kafka.Producer;
const KeyedMessage = kafka.KeyedMessage;
const kafkaClient = new kafka.KafkaClient({ kafkaHost: process.argv[2] });
const producer = new Producer(kafkaClient);

// Track post metadata to detect changes
const postMetadata = new Map();
const commentMetadata = new Map();

// OAuth token management
let accessToken = null;
let tokenExpiresAt = 0;

async function getAccessToken() {
  if (accessToken && Date.now() < tokenExpiresAt) {
    return accessToken;
  }

  try {
    const auth = Buffer.from(`${CLIENT_ID}:${CLIENT_SECRET}`).toString('base64');
    const response = await axios.post(
      'https://www.reddit.com/api/v1/access_token',
      'grant_type=client_credentials',
      {
        headers: {
          'Authorization': `Basic ${auth}`,
          'Content-Type': 'application/x-www-form-urlencoded',
          'User-Agent': 'script:AskRedditAnalysis:v1.0'
        }
      }
    );

    accessToken = response.data.access_token;
    tokenExpiresAt = Date.now() + (response.data.expires_in * 1000) - 60000; // Expire 1 minute early
    return accessToken;
  } catch (error) {
    console.error('Error getting access token:', error.message);
    throw error;
  }
}

// Create authenticated axios instance
async function getRedditClient() {
  const token = await getAccessToken();
  return axios.create({
    baseURL: REDDIT_API_BASE,
    headers: {
      'Authorization': `Bearer ${token}`,
      'User-Agent': 'script:AskRedditAnalysis:v1.0'
    }
  });
}

async function fetchNewPosts() {
  try {
    const redditClient = await getRedditClient();
    const timestamp = Date.now();
    const response = await redditClient.get(`/r/${SUBREDDIT}/new.json?limit=25&_=${timestamp}`);
    const posts = response.data.data.children;

    for (const post of posts) {
      const postData = post.data;

      const currentState = {
        id: postData.id,
        text: postData.title + (postData.selftext ? '\n' + postData.selftext : ''),
        votes: postData.score,
        timestamp: postData.created_utc,
        datetime: new Date(postData.created_utc * 1000).toISOString()
      };

      const previousState = postMetadata.get(postData.id);
      const hasChanged = !previousState ||
        previousState.votes !== currentState.votes ||
        previousState.text !== currentState.text;

      if (hasChanged) {
        sendToKafka('kritikarana-reddit-questions', {
          ...currentState,
          is_update: !!previousState
        });
        postMetadata.set(postData.id, currentState);
      }

      await new Promise(resolve => setTimeout(resolve, 3000));
      await fetchComments(postData.id);
    }
  } catch (error) {
    console.error('Error fetching posts:', error.message);
    if (error.response && error.response.status === 429) {
      const retryAfter = error.response.headers['retry-after'] || 60;
      console.log(`Rate limited. Waiting ${retryAfter} seconds before retry...`);
      await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
    }
  }
}

async function fetchComments(postId) {
  try {
    const redditClient = await getRedditClient();
    const timestamp = Date.now();
    const response = await redditClient.get(`/r/${SUBREDDIT}/comments/${postId}.json?_=${timestamp}`);
    const comments = response.data[1].data.children;

    for (const comment of comments) {
      const commentData = comment.data;

      if (!commentData.id ||
        commentData.body === '[deleted]' ||
        commentData.body === '[removed]') {
        continue;
      }

      const currentState = {
        answer_id: commentData.id,
        q_id: postId,
        text: commentData.body,
        votes: commentData.score
      };

      const commentKey = `${postId}:${commentData.id}`;
      const previousState = commentMetadata.get(commentKey);
      const hasChanged = !previousState ||
        previousState.votes !== currentState.votes ||
        previousState.text !== currentState.text;

      if (hasChanged) {
        sendToKafka('kritikarana-reddit-answers', {
          ...currentState,
          is_update: !!previousState
        });
        commentMetadata.set(commentKey, currentState);
      }
    }
  } catch (error) {
    console.error(`Error fetching comments for post ${postId}:`, error.message);
  }
}

function sendToKafka(topic, data) {
  producer.send([{
    topic: topic,
    messages: JSON.stringify(data)
  }], (err, data) => {
    if (err) {
      console.error('Error sending to Kafka:', err);
    }
  });
}

function clearOldMetadata() {
  postMetadata.clear();
  commentMetadata.clear();
  console.log('Cleared metadata cache');
}

// Run updates on producer ready
producer.on('ready', () => {
  console.log('Kafka Producer is ready');
  fetchNewPosts();
  setInterval(fetchNewPosts, POLL_INTERVAL);
  setInterval(clearOldMetadata, 24 * 60 * 60 * 1000);
});

producer.on('error', (err) => {
  console.error('Kafka Producer error:', err);
});
