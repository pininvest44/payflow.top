const express = require('express');
const axios = require('axios');
const path = require('path');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Helper function to create a delay (essential for the 30 RPM rate limit)
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

app.post('/api/process-bulk', async (req, res) => {
    const { phoneNumbers, amount, reference, description, paymentAccountId } = req.body;
    
    // Retrieve credentials from environment variables for security
    const apiKey = process.env.PAYFLOW_API_KEY;
    const apiSecret = process.env.PAYFLOW_API_SECRET;

    if (!apiKey || !apiSecret) {
        return res.status(500).json({ error: "API credentials are not configured on the server." });
    }

    // Set headers immediately for SSE (Server-Sent Events) to stream logs to frontend
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');

    res.write(`data: ${JSON.stringify({ status: 'info', message: `Starting processing for ${phoneNumbers.length} numbers...` })}\n\n`);

    for (let i = 0; i < phoneNumbers.length; i++) {
        const phone = phoneNumbers[i].trim();
        if (!phone) continue;

        res.write(`data: ${JSON.stringify({ status: 'info', message: `Sending STK Push to ${phone} (${i + 1}/${phoneNumbers.length})...` })}\n\n`);

        try {
            const response = await axios.post('https://payflow.top/api/v2/stkpush.php', {
                payment_account_id: parseInt(paymentAccountId) || 17,
                phone: phone,
                amount: parseFloat(amount),
                reference: reference || `REF_${Date.now()}`,
                description: description || "Bulk Payment"
            }, {
                headers: {
                    'X-API-Key': apiKey,
                    'X-API-Secret': apiSecret,
                    'Content-Type': 'application/json'
                }
            });

            res.write(`data: ${JSON.stringify({ 
                status: 'success', 
                phone: phone, 
                message: `Success: ${JSON.stringify(response.data)}` 
            })}\n\n`);

        } catch (error) {
            const errorMsg = error.response ? JSON.stringify(error.response.data) : error.message;
            res.write(`data: ${JSON.stringify({ 
                status: 'failure', 
                phone: phone, 
                message: `Failed: ${errorMsg}` 
            })}\n\n`);
        }

        // Rate limiter enforcement: 30 requests per minute means 1 request every 2000ms
        if (i < phoneNumbers.length - 1) {
            res.write(`data: ${JSON.stringify({ status: 'info', message: `Waiting 2 seconds to respect rate limit...` })}\n\n`);
            await delay(2000); 
        }
    }

    res.write(`data: ${JSON.stringify({ status: 'done', message: 'Bulk processing completed.' })}\n\n`);
    res.end();
});

app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
