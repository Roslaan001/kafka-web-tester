const express = require('express');
const { Kafka, logLevel } = require('kafkajs');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(express.json());
app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));

let kafkaClient = null;
let adminClient = null;
let producerClient = null;
let consumerClient = null;
let currentBrokers = [];
let consumedMessages = [];
let isConsuming = false;

function getKafka(brokers, clientId = 'kafka-web-tester') {
  return new Kafka({
    clientId,
    brokers: Array.isArray(brokers) ? brokers : [brokers],
    connectionTimeout: 7000,
    requestTimeout: 10000,
    retry: {
      initialRetryTime: 300,
      retries: 3
    },
    logLevel: logLevel.WARN
  });
}

// Connect / Health check endpoint
app.post('/api/connect', async (req, res) => {
  const { brokers } = req.body;
  if (!brokers) {
    return res.status(400).json({ success: false, error: 'Brokers address is required' });
  }

  const brokerList = brokers.split(',').map(b => b.trim()).filter(Boolean);

  try {
    if (adminClient) {
      try { await adminClient.disconnect(); } catch (e) {}
    }
    if (producerClient) {
      try { await producerClient.disconnect(); } catch (e) {}
    }
    if (consumerClient) {
      try { await consumerClient.disconnect(); } catch (e) {}
      isConsuming = false;
    }

    currentBrokers = brokerList;
    kafkaClient = getKafka(brokerList);
    adminClient = kafkaClient.admin();
    
    await adminClient.connect();
    const cluster = await adminClient.describeCluster();
    const topics = await adminClient.listTopics();

    res.json({
      success: true,
      message: 'Connected to Kafka successfully!',
      cluster,
      topics
    });
  } catch (err) {
    console.error('Kafka connect error:', err);
    res.status(500).json({
      success: false,
      error: err.message || 'Failed to connect to Kafka'
    });
  }
});

// List topics
app.get('/api/topics', async (req, res) => {
  try {
    if (!adminClient) {
      return res.status(400).json({ success: false, error: 'Not connected to any Kafka broker' });
    }
    const topics = await adminClient.listTopics();
    const metadata = await adminClient.fetchTopicMetadata({ topics });
    res.json({ success: true, topics, metadata });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Create topic
app.post('/api/topics/create', async (req, res) => {
  const { topic, numPartitions = 1, replicationFactor = 1 } = req.body;
  if (!topic) return res.status(400).json({ success: false, error: 'Topic name required' });

  try {
    if (!adminClient) return res.status(400).json({ success: false, error: 'Not connected' });
    
    const created = await adminClient.createTopics({
      topics: [{
        topic,
        numPartitions: parseInt(numPartitions, 10),
        replicationFactor: parseInt(replicationFactor, 10)
      }]
    });
    
    res.json({ success: true, created, message: `Topic "${topic}" created successfully!` });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Produce message
app.post('/api/produce', async (req, res) => {
  const { topic, message, key } = req.body;
  if (!topic || !message) {
    return res.status(400).json({ success: false, error: 'Topic and message are required' });
  }

  try {
    if (!kafkaClient) {
      return res.status(400).json({ success: false, error: 'Not connected to Kafka' });
    }
    if (!producerClient) {
      producerClient = kafkaClient.producer();
      await producerClient.connect();
    }

    const payload = {
      value: typeof message === 'object' ? JSON.stringify(message) : String(message)
    };
    if (key) {
      payload.key = String(key);
    }

    const recordMetadata = await producerClient.send({
      topic,
      messages: [payload]
    });

    res.json({ success: true, recordMetadata, message: 'Message sent successfully!' });
  } catch (err) {
    console.error('Produce error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// Start consuming topic
app.post('/api/consume/start', async (req, res) => {
  const { topic, fromBeginning = true, groupId = 'web-tester-group-' + Date.now() } = req.body;
  if (!topic) return res.status(400).json({ success: false, error: 'Topic is required' });

  try {
    if (!kafkaClient) return res.status(400).json({ success: false, error: 'Not connected' });

    if (consumerClient) {
      try { await consumerClient.disconnect(); } catch (e) {}
    }

    consumedMessages = [];
    consumerClient = kafkaClient.consumer({ groupId });
    await consumerClient.connect();
    await consumerClient.subscribe({ topic, fromBeginning: Boolean(fromBeginning) });

    isConsuming = true;

    consumerClient.run({
      eachMessage: async ({ topic, partition, message }) => {
        const item = {
          topic,
          partition,
          offset: message.offset,
          key: message.key ? message.key.toString() : null,
          value: message.value ? message.value.toString() : null,
          timestamp: message.timestamp ? new Date(parseInt(message.timestamp, 10)).toISOString() : new Date().toISOString()
        };
        consumedMessages.unshift(item);
        if (consumedMessages.length > 200) consumedMessages.pop();
      }
    });

    res.json({ success: true, message: `Subscribed and listening to topic "${topic}"` });
  } catch (err) {
    isConsuming = false;
    res.status(500).json({ success: false, error: err.message });
  }
});

// Fetch latest consumed messages
app.get('/api/consume/messages', (req, res) => {
  res.json({
    success: true,
    isConsuming,
    messages: consumedMessages
  });
});

// Stop consuming
app.post('/api/consume/stop', async (req, res) => {
  try {
    if (consumerClient) {
      await consumerClient.disconnect();
      isConsuming = false;
    }
    res.json({ success: true, message: 'Consumer stopped' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Kafka Web Tester running at http://0.0.0.0:${PORT}`);
});
