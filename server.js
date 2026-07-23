const express = require('express');
const axios = require('axios');
const path = require('path');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.post('/api/process-bulk', async (req, res) => {
    const { phoneNumbers, amount, reference, description, paymentAccountId } = req.body;

    const apiKey = process.env.PAYFLOW_API_KEY;
    const apiSecret = process.env.PAYFLOW_API_SECRET;

    if (!apiKey || !apiSecret) {
        return res.status(500).json({ error: "API credentials are not configured on the server." });
    }

    if (!Array.isArray(phoneNumbers) || phoneNumbers.length === 0) {
        return res.status(400).json({ error: "No phone numbers provided." });
    }

    // Configure headers for real-time SSE streaming
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no'); // Prevents proxy buffering (Nginx, Cloudflare)

    let isClientConnected = true;
    req.on('close', () => {
        isClientConnected = false;
        console.log('Client disconnected. Halting remaining requests.');
    });

    const sendSSE = (data) => {
        if (isClientConnected) {
            res.write(`data: ${JSON.stringify(data)}\n\n`);
        }
    };

    const validPhones = phoneNumbers.map(p => p.trim()).filter(Boolean);
    sendSSE({ status: 'info', message: `Processing ${validPhones.length} requests concurrently...` });

    // Single request handler function
    const sendStkPush = async (phone, index) => {
        if (!isClientConnected) return;

        sendSSE({ status: 'info', message: `Sending STK Push to ${phone}...` });

        try {
            const response = await axios.post(
                'https://payflow.top/api/v2/stkpush.php',
                {
                    payment_account_id: parseInt(paymentAccountId, 10) || 17,
                    phone: phone,
                    amount: parseFloat(amount),
                    reference: reference ? `${reference}_${index}` : `REF_${Date.now()}_${index}`,
                    description: description || "Bulk Payment"
                },
                {
                    headers: {
                        'X-API-Key': apiKey,
                        'X-API-Secret': apiSecret,
                        'Content-Type': 'application/json'
                    },
                    timeout: 10000 // 10s per request timeout safety
                }
            );

            sendSSE({
                status: 'success',
                phone: phone,
                message: `Success: ${JSON.stringify(response.data)}`
            });
        } catch (error) {
            const errorMsg = error.response ? JSON.stringify(error.response.data) : error.message;
            sendSSE({
                status: 'failure',
                phone: phone,
                message: `Failed: ${errorMsg}`
            });
        }
    };

    // CONCURRENCY CONTROLLER: Adjust MAX_CONCURRENT based on tier limit
    const MAX_CONCURRENT = 5; 
    
    // Process items in chunks of MAX_CONCURRENT simultaneously
    for (let i = 0; i < validPhones.length; i += MAX_CONCURRENT) {
        if (!isClientConnected) break;
        
        const batch = validPhones
            .slice(i, i + MAX_CONCURRENT)
            .map((phone, idx) => sendStkPush(phone, i + idx));

        // Wait for current batch of concurrent requests to complete before firing the next batch
        await Promise.allSettled(batch);
    }

    if (isClientConnected) {
        sendSSE({ status: 'done', message: 'All parallel requests completed.' });
        res.end();
    }
});

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
