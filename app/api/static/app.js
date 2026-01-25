// OASIS Mobile App Logic

const API_BASE = ""; // Relative path since served from same origin

// State
let files = null;
let pollInterval = null;

// Elements
const dropZone = document.getElementById('drop-zone');
const fileInput = document.getElementById('file-input');
const views = {
    upload: document.getElementById('view-upload'),
    processing: document.getElementById('view-processing'),
    results: document.getElementById('view-results')
};
const progressFill = document.getElementById('progress-fill');
const processMessage = document.getElementById('process-message');
const statusIndicator = document.getElementById('status-indicator');

// Event Listeners
dropZone.addEventListener('click', () => fileInput.click());

fileInput.addEventListener('change', (e) => {
    if (e.target.files.length > 0) {
        handleUpload(e.target.files[0]);
    }
});

function switchView(viewName) {
    Object.values(views).forEach(el => el.classList.remove('active'));
    views[viewName].classList.add('active');
}

// Upload & Analyze Flow
async function handleUpload(file) {
    if (!file) return;

    switchView('processing');
    updateStatus("Uploading...");

    const formData = new FormData();
    formData.append('file', file);

    try {
        const res = await fetch(`${API_BASE}/upload`, {
            method: 'POST',
            body: formData
        });
        
        if (!res.ok) throw new Error("Upload failed");
        
        const data = await res.json();
        console.log("Upload success:", data);
        
        // Start Polling
        startPolling();
        
    } catch (err) {
        alert("Error: " + err.message);
        switchView('upload');
    }
}

function startPolling() {
    pollInterval = setInterval(async () => {
        try {
            const res = await fetch(`${API_BASE}/status`);
            const status = await res.json();
            
            // Update UI
            if (processMessage) processMessage.innerText = status.message;
            if (progressFill) progressFill.style.width = `${status.progress}%`;
            
            if (status.state === 'completed') {
                clearInterval(pollInterval);
                loadResults();
            } else if (status.state === 'error') {
                clearInterval(pollInterval);
                alert("Analysis Error: " + status.message);
                switchView('upload');
            }
            
        } catch (err) {
            console.error("Poll error", err);
        }
    }, 1000);
}

async function loadResults() {
    updateStatus("Loading Results...");
    try {
        const res = await fetch(`${API_BASE}/results`);
        const data = await res.json();
        renderResults(data.results);
        switchView('results');
        updateStatus("Ready");
    } catch (err) {
        alert("Failed to load results");
    }
}

function renderResults(items) {
    const list = document.getElementById('results-list');
    list.innerHTML = '';
    
    // Sort: Non-zero first
    items.sort((a, b) => b.recommended_quantity - a.recommended_quantity);
    
    items.forEach(item => {
        const card = document.createElement('div');
        card.className = 'result-item';
        
        const qtyClass = item.recommended_quantity > 0 ? '' : 'zero';
        
        card.innerHTML = `
            <div class="result-info">
                <h4>${item.product_name}</h4>
                <p>Cur: ${item.current_stocks} | Hist: ${item.historical_avg_order_qty || 0}</p>
            </div>
            <div class="result-qty ${qtyClass}">
                ${item.recommended_quantity}
            </div>
        `;
        list.appendChild(card);
    });
}

function downloadReport() {
    window.location.href = `${API_BASE}/download`;
}

function resetApp() {
    switchView('upload');
    fileInput.value = '';
    progressFill.style.width = '0%';
}

function updateStatus(msg) {
    statusIndicator.innerText = msg;
    if (msg === 'Ready') statusIndicator.classList.remove('active');
    else statusIndicator.classList.add('active');
}
