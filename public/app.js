document.getElementById('stkForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const submitBtn = document.getElementById('submitBtn');
    const logContainer = document.getElementById('logContainer');
    
    const rawPhones = document.getElementById('phoneNumbers').value;
    const phoneNumbers = rawPhones.split('\n').map(p => p.trim()).filter(p => p.length > 0);
    
    if (phoneNumbers.length === 0) {
        alert("Please enter at least one valid phone number.");
        return;
    }

    submitBtn.disabled = true;
    logContainer.innerHTML = '';

    const payload = {
        paymentAccountId: document.getElementById('paymentAccountId').value,
        phoneNumbers: phoneNumbers,
        amount: document.getElementById('amount').value,
        reference: document.getElementById('reference').value,
        description: document.getElementById('description').value
    };

    // Use standard fetch but handle the streaming response line by line
    try {
        const response = await fetch('/api/process-bulk', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        const reader = response.body.getReader();
        const decoder = new TextDecoder();

        while (true) {
            const { value, done } = await reader.read();
            if (done) break;

            const chunk = decoder.decode(value);
            const lines = chunk.split('\n');

            lines.forEach(line => {
                if (line.startsWith('data: ')) {
                    try {
                        const data = JSON.parse(line.replace('data: ', ''));
                        appendLog(data);
                    } catch (e) {
                        // Catch malformed json fragments if stream breaks
                    }
                }
            });
        }
    } catch (err) {
        logContainer.innerHTML += `<div class="log-failure">Network Error: ${err.message}</div>`;
    } finally {
        submitBtn.disabled = false;
    }
});

function appendLog(data) {
    const logContainer = document.getElementById('logContainer');
    let cssClass = 'log-info';
    
    if (data.status === 'success') cssClass = 'log-success';
    if (data.status === 'failure') cssClass = 'log-failure';

    logContainer.innerHTML += `<div class="${cssClass}">[${new Date().toLocaleTimeString()}] ${data.message}</div>`;
    logContainer.scrollTop = logContainer.scrollHeight;
}
