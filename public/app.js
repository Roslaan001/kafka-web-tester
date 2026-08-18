// State
let connected = false;
let currentTopics = [];
let consumeInterval = null;
let isConsuming = false;

// DOM Elements
const brokerInput = document.getElementById('broker-input');
const btnConnect = document.getElementById('btn-connect');
const clusterStatusBadge = document.getElementById('cluster-status-badge');
const topicList = document.getElementById('topic-list');
const targetTopicProduce = document.getElementById('target-topic-produce');
const targetTopicConsume = document.getElementById('target-topic-consume');
const messageKey = document.getElementById('message-key');
const messagePayload = document.getElementById('message-payload');
const btnSendMessage = document.getElementById('btn-send-message');
const produceStatus = document.getElementById('produce-status');
const btnToggleConsume = document.getElementById('btn-toggle-consume');
const btnClearMessages = document.getElementById('btn-clear-messages');
const messagesContainer = document.getElementById('messages-container');
const streamEmptyState = document.getElementById('stream-empty-state');
const consumerDot = document.getElementById('consumer-dot');

const btnOpenCreateTopic = document.getElementById('btn-open-create-topic');
const createTopicBox = document.getElementById('create-topic-box');
const newTopicName = document.getElementById('new-topic-name');
const newTopicPartitions = document.getElementById('new-topic-partitions');
const newTopicReplication = document.getElementById('new-topic-replication');
const btnCancelTopic = document.getElementById('btn-cancel-topic');
const btnSubmitTopic = document.getElementById('btn-submit-topic');

const infoClusterId = document.getElementById('info-cluster-id');
const infoControllerId = document.getElementById('info-controller-id');
const infoBrokersCount = document.getElementById('info-brokers-count');

// Connect to broker
btnConnect.addEventListener('click', async () => {
  const brokers = brokerInput.value.trim();
  if (!brokers) return alert('Please enter broker address');

  btnConnect.disabled = true;
  btnConnect.textContent = 'Connecting...';
  clusterStatusBadge.textContent = 'Connecting...';
  clusterStatusBadge.className = 'badge';

  try {
    const res = await fetch('/api/connect', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ brokers })
    });
    const data = await res.json();

    if (data.success) {
      connected = true;
      clusterStatusBadge.textContent = 'Connected';
      clusterStatusBadge.className = 'badge connected';
      
      if (data.cluster) {
        infoClusterId.textContent = data.cluster.clusterId || 'KRaft';
        infoControllerId.textContent = data.cluster.controller ?? '-';
        infoBrokersCount.textContent = data.cluster.brokers?.length || 1;
      }

      await loadTopics();
    } else {
      throw new Error(data.error);
    }
  } catch (err) {
    alert('Connection Failed: ' + err.message);
    clusterStatusBadge.textContent = 'Error';
    clusterStatusBadge.className = 'badge disconnected';
  } finally {
    btnConnect.disabled = false;
    btnConnect.textContent = 'Connect';
  }
});

// Load Topics
async function loadTopics() {
  try {
    const res = await fetch('/api/topics');
    const data = await res.json();

    if (data.success) {
      currentTopics = data.topics.filter(t => !t.startsWith('__')); // Hide internal topics by default
      renderTopics();
      updateTopicDropdowns();
    }
  } catch (err) {
    console.error('Failed to load topics', err);
  }
}

function renderTopics() {
  if (!currentTopics || currentTopics.length === 0) {
    topicList.innerHTML = '<div class="empty-state-card"><p>No topics found. Create one!</p></div>';
    return;
  }

  topicList.innerHTML = '';
  currentTopics.forEach(topic => {
    const item = document.createElement('div');
    item.className = 'topic-item';
    item.innerHTML = `
      <span class="topic-name">${topic}</span>
      <span class="topic-partitions-tag">Topic</span>
    `;
    item.addEventListener('click', () => {
      targetTopicProduce.value = topic;
      targetTopicConsume.value = topic;
      document.querySelectorAll('.topic-item').forEach(el => el.classList.remove('active'));
      item.classList.add('active');
    });
    topicList.appendChild(item);
  });
}

function updateTopicDropdowns() {
  const options = ['<option value="">(Select a topic)</option>']
    .concat(currentTopics.map(t => `<option value="${t}">${t}</option>`))
    .join('');

  const prevProd = targetTopicProduce.value;
  const prevCons = targetTopicConsume.value;
  
  targetTopicProduce.innerHTML = options;
  targetTopicConsume.innerHTML = options;

  if (currentTopics.includes(prevProd)) targetTopicProduce.value = prevProd;
  else if (currentTopics.length > 0) targetTopicProduce.value = currentTopics[0];

  if (currentTopics.includes(prevCons)) targetTopicConsume.value = prevCons;
  else if (currentTopics.length > 0) targetTopicConsume.value = currentTopics[0];
}

// Create Topic toggle
btnOpenCreateTopic.addEventListener('click', () => {
  createTopicBox.classList.toggle('hidden');
});
btnCancelTopic.addEventListener('click', () => {
  createTopicBox.classList.add('hidden');
});

btnSubmitTopic.addEventListener('click', async () => {
  const topic = newTopicName.value.trim();
  const partitions = parseInt(newTopicPartitions.value, 10) || 1;
  const replication = parseInt(newTopicReplication.value, 10) || 1;

  if (!topic) return alert('Enter topic name');

  try {
    const res = await fetch('/api/topics/create', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ topic, numPartitions: partitions, replicationFactor: replication })
    });
    const data = await res.json();
    if (data.success) {
      newTopicName.value = '';
      createTopicBox.classList.add('hidden');
      await loadTopics();
      targetTopicProduce.value = topic;
      targetTopicConsume.value = topic;
    } else {
      alert('Error creating topic: ' + data.error);
    }
  } catch (err) {
    alert('Error: ' + err.message);
  }
});

// Produce Message
btnSendMessage.addEventListener('click', async () => {
  const topic = targetTopicProduce.value;
  const message = messagePayload.value.trim();
  const key = messageKey.value.trim();

  if (!topic) return alert('Please select or enter a topic');
  if (!message) return alert('Please enter message content');

  produceStatus.textContent = 'Sending...';
  produceStatus.style.color = 'var(--text-secondary)';

  try {
    const res = await fetch('/api/produce', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ topic, message, key })
    });
    const data = await res.json();
    if (data.success) {
      produceStatus.textContent = 'Sent successfully!';
      produceStatus.style.color = 'var(--success)';
      setTimeout(() => { produceStatus.textContent = ''; }, 3000);
    } else {
      throw new Error(data.error);
    }
  } catch (err) {
    produceStatus.textContent = 'Error: ' + err.message;
    produceStatus.style.color = 'var(--danger)';
  }
});

// Consumer Stream
btnToggleConsume.addEventListener('click', async () => {
  if (isConsuming) {
    // Stop
    await fetch('/api/consume/stop', { method: 'POST' });
    isConsuming = false;
    clearInterval(consumeInterval);
    btnToggleConsume.textContent = 'Start Listening';
    btnToggleConsume.className = 'btn btn-secondary';
    consumerDot.classList.remove('active');
  } else {
    // Start
    const topic = targetTopicConsume.value;
    if (!topic) return alert('Select a topic to listen to');

    try {
      const res = await fetch('/api/consume/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ topic, fromBeginning: true })
      });
      const data = await res.json();
      if (!data.success) throw new Error(data.error);

      isConsuming = true;
      btnToggleConsume.textContent = 'Stop Listening';
      btnToggleConsume.className = 'btn btn-primary';
      consumerDot.classList.add('active');
      
      messagesContainer.innerHTML = '';
      consumeInterval = setInterval(fetchMessages, 1000);
    } catch (err) {
      alert('Consumer error: ' + err.message);
    }
  }
});

async function fetchMessages() {
  try {
    const res = await fetch('/api/consume/messages');
    const data = await res.json();
    if (data.success && data.messages) {
      renderMessages(data.messages);
    }
  } catch (err) {
    console.error('Fetch messages error:', err);
  }
}

function renderMessages(messages) {
  if (messages.length === 0) {
    messagesContainer.innerHTML = '<div class="stream-empty"><p>Listening for records on this topic...</p></div>';
    return;
  }

  messagesContainer.innerHTML = messages.map(msg => {
    let formattedVal = msg.value;
    try {
      formattedVal = JSON.stringify(JSON.parse(msg.value), null, 2);
    } catch (e) {}

    return `
      <div class="msg-card">
        <div class="msg-header">
          <span>Topic: <strong>${msg.topic}</strong> (P:${msg.partition} | Offset:${msg.offset})</span>
          <span>${msg.timestamp}</span>
        </div>
        ${msg.key ? `<div class="msg-key">Key: ${msg.key}</div>` : ''}
        <pre class="msg-body">${escapeHtml(formattedVal)}</pre>
      </div>
    `;
  }).join('');
}

btnClearMessages.addEventListener('click', () => {
  messagesContainer.innerHTML = '<div class="stream-empty"><p>Messages cleared.</p></div>';
});

function escapeHtml(str) {
  if (!str) return '';
  return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
