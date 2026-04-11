const BACKEND_URL = "http://localhost:8000";

const uploadInput = document.getElementById('audio-upload');
const processBtn = document.getElementById('process-btn');
const statusDiv = document.getElementById('status');
const canvasEl = document.getElementById('visual-canvas');

let audioCtx;
let analyzers = {};
let buffers = {};
let sources = {};
let isPlaying = false;

// --- 2D Canvas Setup ---
const ctx = canvasEl.getContext('2d', { alpha: false });

function resizeCanvas() {
    canvasEl.width = window.innerWidth;
    canvasEl.height = window.innerHeight;
    // Initialize background
    ctx.fillStyle = '#050510';
    ctx.fillRect(0, 0, canvasEl.width, canvasEl.height);
}
window.addEventListener('resize', resizeCanvas);
resizeCanvas(); // Initial setup

// --- Painter State ---
// We keep track of positions to draw continuous strokes
const painters = {
    drums: { x: window.innerWidth / 2, y: window.innerHeight / 2, hue: 340 }, // Pink/Red splatters
    bass: { x: window.innerWidth / 2, y: window.innerHeight / 2, hue: 220, angle: 0 },  // Blue thick sweeps
    vocals: { x: 0, y: window.innerHeight / 2, hue: 60, currentX: 0 },         // Yellow flowing waves
    other: { hue: 180 }                                                        // Cyan airbrush/speckles
};

// --- UI Logic ---
uploadInput.addEventListener('change', () => {
    if (uploadInput.files.length > 0) {
        processBtn.disabled = false;
    }
});

processBtn.addEventListener('click', async () => {
    const file = uploadInput.files[0];
    if (!file) return;

    const formData = new FormData();
    formData.append('file', file);

    processBtn.disabled = true;
    statusDiv.innerHTML = '<span class="spinner"></span> Uploading...';

    try {
        const response = await fetch(`${BACKEND_URL}/upload`, {
            method: 'POST',
            body: formData
        });

        if (!response.ok) throw new Error("Upload failed");

        const data = await response.json();
        pollStatus(data.job_id);

    } catch (err) {
        console.error(err);
        statusDiv.textContent = "Error uploading file.";
        processBtn.disabled = false;
    }
});

async function pollStatus(jobId) {
    statusDiv.innerHTML = '<span class="spinner"></span> Separating stems...';

    const interval = setInterval(async () => {
        try {
            const res = await fetch(`${BACKEND_URL}/status/${jobId}`);
            const data = await res.json();

            if (data.status === 'DONE') {
                clearInterval(interval);
                statusDiv.textContent = "Processing complete! Loading audio...";
                loadAudioStems(data.stems);
            } else if (data.status === 'FAILED') {
                clearInterval(interval);
                statusDiv.textContent = "Separation failed.";
                processBtn.disabled = false;
            }
        } catch (err) {
            console.error(err);
        }
    }, 3000);
}

// --- Audio Logic ---
async function loadAudioStems(stemsData) {
    if (!audioCtx) {
        audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    }

    const stemNames = Object.keys(stemsData);
    let loadedCount = 0;
    statusDiv.textContent = `Loading stems (0/${stemNames.length})...`;

    for (const stemName of stemNames) {
        try {
            const url = `${BACKEND_URL}${stemsData[stemName]}`;
            const response = await fetch(url);
            const arrayBuffer = await response.arrayBuffer();
            const audioBuffer = await audioCtx.decodeAudioData(arrayBuffer);

            buffers[stemName] = audioBuffer;
            loadedCount++;
            statusDiv.textContent = `Loading stems (${loadedCount}/${stemNames.length})...`;

            const analyzer = audioCtx.createAnalyser();
            analyzer.fftSize = 512;
            analyzer.smoothingTimeConstant = 0.8;
            analyzers[stemName] = analyzer;

        } catch (err) {
            console.error(`Failed to load stem: ${stemName}`, err);
        }
    }

    statusDiv.innerHTML = 'Ready. <button id="play-btn">Paint Canvas</button>';
    document.getElementById('play-btn').addEventListener('click', playAllStems);
}

function playAllStems() {
    if (audioCtx.state === 'suspended') {
        audioCtx.resume();
    }

    if (isPlaying) return;
    isPlaying = true;

    // Clear canvas for a fresh painting
    ctx.fillStyle = '#050510';
    ctx.fillRect(0, 0, canvasEl.width, canvasEl.height);

    const startTime = audioCtx.currentTime + 0.1;

    for (const [stemName, buffer] of Object.entries(buffers)) {
        const source = audioCtx.createBufferSource();
        source.buffer = buffer;

        const analyzer = analyzers[stemName];
        source.connect(analyzer);
        analyzer.connect(audioCtx.destination); // Ensure audio plays to speakers

        source.start(startTime);
        sources[stemName] = source;
    }

    statusDiv.textContent = "Painting...";

    // Reset painter positions
    painters.bass.x = canvasEl.width / 2;
    painters.bass.y = canvasEl.height / 2;
    painters.vocals.currentX = 0;

    requestAnimationFrame(paintFrame);
}

function getFrequencyData(analyzer) {
    if (!analyzer) return { average: 0, peak: 0 };
    const dataArray = new Uint8Array(analyzer.frequencyBinCount);
    analyzer.getByteFrequencyData(dataArray);
    let sum = 0;
    let peak = 0;
    for (let i = 0; i < dataArray.length; i++) {
        sum += dataArray[i];
        if (dataArray[i] > peak) peak = dataArray[i];
    }
    return { average: sum / dataArray.length, peak: peak };
}

// --- Painting Loop ---
function paintFrame() {
    if (isPlaying) {
        requestAnimationFrame(paintFrame);
    }

    // We do NOT clear the canvas, letting strokes accumulate!
    // Optional: Add a *very* slight fade to dark over a long time, but for a true "painting", 
    // we let it build up permanently. We will just draw on top.

    ctx.globalCompositeOperation = 'screen'; // Blends colors nicely

    // --- Drums: Sharp, erratic splatters ---
    const drumData = getFrequencyData(analyzers['drums']);
    if (drumData.peak > 150) {
        const numSplats = Math.floor(drumData.peak / 20);
        ctx.fillStyle = `hsla(${painters.drums.hue + (Math.random() * 40 - 20)}, 100%, 60%, ${drumData.peak / 255})`;

        // Jump to random area on loud beats
        if (drumData.peak > 220) {
            painters.drums.x = Math.random() * canvasEl.width;
            painters.drums.y = Math.random() * canvasEl.height;
        }

        for (let i = 0; i < numSplats; i++) {
            const ox = (Math.random() - 0.5) * drumData.peak;
            const oy = (Math.random() - 0.5) * drumData.peak;
            const radius = Math.random() * (drumData.peak / 30);

            ctx.beginPath();
            ctx.arc(painters.drums.x + ox, painters.drums.y + oy, radius, 0, Math.PI * 2);
            ctx.fill();
        }
    }

    // --- Bass: Slow, thick, sweeping background strokes ---
    const bassData = getFrequencyData(analyzers['bass']);
    if (bassData.average > 40) {
        ctx.beginPath();
        ctx.moveTo(painters.bass.x, painters.bass.y);

        // Move slowly in a wandering path
        painters.bass.angle += (Math.random() - 0.5) * 0.5;
        const speed = bassData.average / 10;
        painters.bass.x += Math.cos(painters.bass.angle) * speed;
        painters.bass.y += Math.sin(painters.bass.angle) * speed;

        // Wrap around screen
        if (painters.bass.x < 0) painters.bass.x = canvasEl.width;
        if (painters.bass.x > canvasEl.width) painters.bass.x = 0;
        if (painters.bass.y < 0) painters.bass.y = canvasEl.height;
        if (painters.bass.y > canvasEl.height) painters.bass.y = 0;

        ctx.lineTo(painters.bass.x, painters.bass.y);
        // Draw thick stroke
        ctx.strokeStyle = `hsla(${painters.bass.hue}, 80%, 40%, 0.1)`;
        ctx.lineWidth = bassData.average * 2;
        ctx.lineCap = 'round';
        ctx.stroke();
    }

    // --- Vocals: Continuous flowing wave across screen ---
    const vocalData = getFrequencyData(analyzers['vocals']);
    if (vocalData.average > 10) {
        const prevX = painters.vocals.currentX;
        painters.vocals.currentX += 2 + (vocalData.average / 50); // move right

        if (painters.vocals.currentX > canvasEl.width) {
            painters.vocals.currentX = 0; // reset to left
            painters.vocals.y = Math.random() * canvasEl.height; // pick new Y
        }

        const waveY = painters.vocals.y + Math.sin(painters.vocals.currentX * 0.05) * vocalData.average * 1.5;

        ctx.beginPath();
        // To make a continuous line, we'd need to store the last drawn point,
        // but drawing tiny segments works if speed is consistent.
        ctx.arc(painters.vocals.currentX, waveY, vocalData.average / 10, 0, Math.PI * 2);
        ctx.fillStyle = `hsla(${painters.vocals.hue}, 100%, 70%, 0.5)`;
        ctx.fill();
    }

    // --- Other: Fine detail airbrush / dust ---
    const otherData = getFrequencyData(analyzers['other']);
    if (otherData.average > 20) {
        for (let i = 0; i < 5; i++) {
            const x = Math.random() * canvasEl.width;
            const y = Math.random() * canvasEl.height;
            ctx.fillStyle = `hsla(${painters.other.hue + (Math.random() * 60)}, 100%, 80%, ${otherData.average / 500})`;
            ctx.fillRect(x, y, 2, 2);
        }
    }
}
