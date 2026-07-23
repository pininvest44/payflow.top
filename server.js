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

    // Set headers for SSE
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');

    let isClientConnected = true;
    req.on('close', () => {
        isClientConnected = false;
        console.log('Client disconnected from SSE stream.');
    });

    const sendSSE = (data) => {
        if (isClientConnected) {
            res.write(`data: ${JSON.stringify(data)}\n\n`);
        }
    };

    // Filter out empty lines
    const validPhones = phoneNumbers.map(p => p.trim()).filter(Boolean);

    sendSSE({ status: 'info', message: `Firing ${validPhones.length} requests simultaneously...` });

    // Single request handler function
    const sendStkPush = async (phone, index) => {
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
                    timeout: 15000
                }
            );

            sendSSE({ 
                status: 'success', 
                phone: phone, 
                message: `Success: ${JSON.stringify(response.data)}` 
            });
            return response.data;

        } catch (error) {
            const errorMsg = error.response ? JSON.stringify(error.response.data) : error.message;
            sendSSE({ 
                status: 'failure', 
                phone: phone, 
                message: `Failed: ${errorMsg}` 
            });
            return null;
        }
    };

    // Promise.allSettled runs all promises in parallel concurrently
    await Promise.allSettled(
        validPhones.map((phone, idx) => sendStkPush(phone, idx))
    );

    if (isClientConnected) {
        sendSSE({ status: 'done', message: 'All parallel requests completed.' });
        res.end();
    }
});
