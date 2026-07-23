const express = require('express');
const cors = require('cors');
const path = require('path');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Helper to pause execution for rate limiting
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

app.post('/api/stkpush/bulk', async (req, res) => {
  const { phoneNumbers, amount, reference, description, paymentAccountId } = req.body;

  if (!phoneNumbers || !Array.isArray(phoneNumbers) || phoneNumbers.length === 0) {
    return res.status(400).json({ error: 'Valid phone numbers list is required.' });
  }

  if (!amount || !reference || !paymentAccountId) {
    return res.status(400).json({ error: 'Amount, Reference, and Payment Account ID are required.' });
  }

  const apiKey = process.env.PAYFLOW_API_KEY;
  const apiSecret = process.env.PAYFLOW_API_SECRET;

  if (!apiKey || !apiSecret) {
    return res.status(500).json({ error: 'Server misconfiguration: API credentials missing.' });
  }

  // Set up Server-Sent Events (SSE) stream for live updates
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  const sendEvent = (data) => {
    res.write(`data: ${JSON.stringify(data)}\n\n`);
  };

  const total = phoneNumbers.length;
  sendEvent({ type: 'start', total });

  const BATCH_SIZE = 10; // Max 10 req/sec

  for (let i = 0; i < total; i += BATCH_SIZE) {
    const batch = phoneNumbers.slice(i, i + BATCH_SIZE);
    const batchStartTime = Date.now();

    const promises = batch.map(async (phone) => {
      const formattedPhone = phone.trim().replace(/^\+/, '');
      const payload = {
        payment_account_id: Number(paymentAccountId),
        phone: formattedPhone,
        amount: Number(amount),
        reference,
        description: description || 'Bulk payment'
      };

      try {
        const apiResponse = await fetch('https://payflow.top/api/v2/stkpush.php', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-API-Key': apiKey,
            'X-API-Secret': apiSecret
          },
          body: JSON.stringify(payload)
        });

        const data = await apiResponse.json();

        if (apiResponse.ok) {
          sendEvent({
            type: 'result',
            status: 'success',
            phone: formattedPhone,
            data
          });
        } else {
          sendEvent({
            type: 'result',
            status: 'error',
            phone: formattedPhone,
            error: data.message || 'Payment provider error'
          });
        }
      } catch (err) {
        sendEvent({
          type: 'result',
          status: 'error',
          phone: formattedPhone,
          error: err.message
        });
      }
    });

    await Promise.all(promises);

    // Enforce rate limit (1 second per batch of 10)
    const elapsedTime = Date.now() - batchStartTime;
    if (elapsedTime < 1000 && i + BATCH_SIZE < total) {
      await sleep(1000 - elapsedTime);
    }
  }

  sendEvent({ type: 'done' });
  res.end();
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
