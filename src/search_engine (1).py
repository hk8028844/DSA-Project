"""
CORD-19 ENHANCED ULTRA-FAST Search Engine - Integrated Version
Maximum Performance with Smart Caching, Metadata, Real Snippets, and Autocomplete
HTML Frontend Embedded in Python Backend - Single File Solution

Installation:
pip install flask flask-cors ujson

Run:
python cord19_search_integrated.py
Browser will open automatically!
"""

from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import ujson as json
import speech_recognition as sr
import io
import base64
import time
import math
from pathlib import Path
from collections import defaultdict
import re
from functools import lru_cache
import threading

app = Flask(__name__)
CORS(app)

# ============================================================================
# EMBEDDED HTML FRONTEND
# ============================================================================

HTML_FRONTEND = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CORD-19 Enhanced Search Engine</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: Arial, sans-serif; background: #fff; }
        
        .header { 
            padding: 20px 40px; 
            display: none; 
            align-items: center; 
            gap: 20px; 
            border-bottom: 1px solid #ebebeb; 
        }
        .logo { font-size: 24px; font-weight: bold; }
        .logo span:nth-child(1) { color: #4285f4; }
        .logo span:nth-child(2) { color: #ea4335; }
        .logo span:nth-child(3) { color: #fbbc04; }
        .logo span:nth-child(4) { color: #4285f4; }
        .logo span:nth-child(5) { color: #34a853; }
        .logo span:nth-child(6) { color: #ea4335; }
        
        .search-container { 
            max-width: 700px;
            margin: 160px auto 40px; 
            padding: 0 20px; 
            text-align: center;
        }
        .search-container.results-mode { margin-top: 20px; }
        
        .logo-large { 
            font-size: 90px; 
            font-weight: 100;
            margin-bottom: 30px; 
            margin-top: 100px;
            margin-left: 10px;
            letter-spacing: -2px; 
        }
        .logo-large.hidden { display: none; }
        
        .search-box { 
            position: relative; 
            width: 110%; 
            max-width: 900px; 
            margin: 10px auto; 
            
        }
        .search-input {  
            width: 100%; 
            height: 55px;
            padding: 14px 50px 14px 45px; 
            border: 1px solid #dfe1e5; 
            border-radius: 36px; 
            font-size: 16px; 
            outline: none; 
            transition: box-shadow 0.2s; 
        }
        .search-input:hover, .search-input:focus { 
            box-shadow: 0 1px 6px rgba(32,33,36,.28); 
            border-color: rgba(223,225,229,0); 
        }
        .search-icon { 
            position: absolute; 
            left: 15px; 
            top: 50%; 
            margin-top:-3px;
            margin-right:-1px;
            transform: translateY(-50%); 
            color: #9aa0a6; 
            font-size: 30px; 
        }
        .search-btn { 
            position: absolute; 
            margin-right:-10px;
            right: 30px; 
            top: 50%; 
            transform: translateY(-50%); 
            background: #4285f4; 
            color: white; 
            border: none; 
            padding: 8px 20px; 
            border-radius: 24px; 
            cursor: pointer; 
            font-size: 14px; 
            font-weight: 500; 
        }
        .search-btn:hover { background: #3367d6; }
         .mic-btn {
            position: absolute;
            right: 110px;
            top: 50%;
            transform: translateY(-50%);
            background: white;
            color: #5f6368;
            border: none;
            width: 28px;
            height: 28px;
            border-radius: 50%;
            cursor: pointer;
            font-size: 20px;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: all 0.2s;
            margin-right: 3px;
        }
        .mic-btn:hover {
            background: #f1f3f4;
        }
        .mic-btn.recording {
            color: #ea4335;
            animation: pulse 1.5s infinite;
        }
        .mic-btn.processing {
            color: #4285f4;
        }
        @keyframes pulse {
            0%, 100% { transform: translateY(-50%) scale(1); }
            50% { transform: translateY(-50%) scale(1.1); }
        }
        
        .suggestions-dropdown { 
            position: absolute; 
            top: calc(100% + 4px);
            left: 0; 
            right: 0; 
            background: white; 
            border: 1px solid #dfe1e5; 
            border-radius: 8px; 
            box-shadow: 0 4px 6px rgba(32,33,36,.28);
            max-height: 400px;
            overflow-y: auto;
            display: none;
            z-index: 1000;
        }
        .suggestions-dropdown.active { display: block; }
        .suggestion-item {
            padding: 10px 20px;
            cursor: pointer;
            font-size: 16px;
            color: #202124;
            display: flex;
            align-items: center;
            gap: 10px;
            transition: background 0.1s;
        }
        .suggestion-item:hover, .suggestion-item.selected {
            background: #f1f3f4;
        }
        .suggestion-text {
            flex: 1;
        }
        .suggestion-term {
            font-weight: 600;
        }
        
        .loading { 
            display: none; 
            margin: 40px auto; 
            width: 40px; 
            height: 40px; 
            border: 4px solid #f3f3f3; 
            border-top: 4px solid #4285f4; 
            border-radius: 50%; 
            animation: spin 1s linear infinite; 
        }
        .loading.active { display: block; }
        @keyframes spin { 
            0% { transform: rotate(0deg); } 
            100% { transform: rotate(360deg); } 
        }
        
        .results-container { 
            max-width: 700px; 
            margin: 0 auto; 
            padding: 20px; 
            display: none; 
        }
        .results-container.active { display: block; }
        .results-info { 
            color: #70757a; 
            font-size: 14px; 
            margin-bottom: 20px; 
        }
        
        .result-item { margin-bottom: 32px; }
        .result-url { 
            font-size: 14px; 
            color: #202124; 
            margin-bottom: 4px; 
            display: flex; 
            align-items: center; 
            gap: 8px; 
        }
        .result-domain { color: #5f6368; }
        .result-title { 
            font-size: 20px; 
            color: #1a0dab; 
            margin-bottom: 4px; 
            cursor: pointer; 
            line-height: 1.3; 
            font-weight: 400; 
        }
        .result-title:hover { text-decoration: underline; }
        .result-snippet { 
            font-size: 14px; 
            color: #4d5156; 
            line-height: 1.58; 
        }
        .result-snippet mark { 
            background: #fff3cd; 
            font-weight: 600; 
            padding: 2px 4px; 
            border-radius: 2px; 
        }
        .result-metadata { 
            font-size: 12px; 
            color: #70757a; 
            margin-top: 8px; 
            display: flex; 
            gap: 12px; 
            flex-wrap: wrap; 
        }
        .result-authors { 
            font-size: 13px; 
            color: #5f6368; 
            margin-top: 4px; 
        }
        
        .no-results { 
            text-align: center; 
            padding: 40px; 
            color: #70757a; 
            display: none; 
        }
        .no-results.active { display: block; }
        .no-results h2 { 
            font-size: 20px; 
            margin-bottom: 8px; 
            color: #202124; 
        }
        
        .error-message { 
            background: #fce8e6; 
            color: #c5221f; 
            padding: 12px 16px; 
            border-radius: 4px; 
            margin: 20px auto; 
            max-width: 700px; 
            display: none; 
        }
        .error-message.active { display: block; }
        
        .speed-badge { 
            background: #34a853; 
            color: white; 
            padding: 4px 8px; 
            border-radius: 12px; 
            font-size: 11px; 
            font-weight: bold; 
            margin-left: 10px; 
        }
        
        .open-file-btn {
            display: inline-block;
            background: #4285f4;
            color: white;
            padding: 6px 12px;
            border-radius: 4px;
            font-size: 12px;
            cursor: pointer;
            border: none;
            margin-top: 8px;
            transition: background 0.2s;
        }
        .open-file-btn:hover {
            background: #3367d6;
        }
        .open-file-btn:active {
            background: #2851a3;
        }
    </style>
</head>
<body>
     

    <div class="search-container" id="searchContainer">
        <div class="logo-large" id="logoLarge">
            <span style="color: #4285f4;">C</span><span style="color: #ea4335;">O</span><span style="color: #fbbc04;">R</span><span style="color: #4285f4;">D</span><span style="color: #34a853;">-</span><span style="color: #ea4335;">19</span>
        </div>
        <div class="search-box">
            <span class="search-icon">⌕ </span>
            <input type="text" class="search-input" id="searchInput" placeholder="Search CORD-19 research papers..." maxlength="100" autocomplete="off" />
           <button class="mic-btn" id="micBtn" title="Voice search">
            <svg class="mic-icon" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                <path d="M12 14c1.66 0 3-1.34 3-3V5c0-1.66-1.34-3-3-3S9 3.34 9 5v6c0 1.66 1.34 3 3 3z" fill="currentColor"/>
                <path d="M17 11c0 2.76-2.24 5-5 5s-5-2.24-5-5H5c0 3.53 2.61 6.43 6 6.92V21h2v-3.08c3.39-.49 6-3.39 6-6.92h-2z" fill="currentColor"/>
            </svg>
            </button>
            <button class="search-btn" id="searchBtn">Search</button>
            <div class="suggestions-dropdown" id="suggestionsDropdown"></div>
        </div>
    </div>
    <h6 style="text-align: center;"><br><br><br><br><br><br><br><br><br><br><br><br><br><br><br><br><br><br><br><br><br><br><br>© 2025  Developed by BESE-15B  DSA Project</h6>

    <div class="loading" id="loading"></div>
    <div class="error-message" id="errorMessage"></div>
    
    <div class="results-container" id="resultsContainer">
        <div class="results-info" id="resultsInfo"></div>
        <div id="resultsContent"></div>
    </div>
    
    <div class="no-results" id="noResults">
        <h2>No results found</h2>
        <p>Try different keywords or check your spelling</p>
    </div>

    <script>
        const API_BASE = window.location.origin;
        const FILE_OPENER_API = "http://localhost:8081";
        
        console.log(" Frontend loaded successfully!");
        const els = {
                    input: document.getElementById("searchInput"),
                    btn: document.getElementById("searchBtn"),
                    micBtn: document.getElementById("micBtn"),
                    loading: document.getElementById("loading"),
                    container: document.getElementById("resultsContainer"),
                    content: document.getElementById("resultsContent"),
                    info: document.getElementById("resultsInfo"),
                    noResults: document.getElementById("noResults"),
                    error: document.getElementById("errorMessage"),
                    searchContainer: document.getElementById("searchContainer"),
                    logo: document.getElementById("logoLarge"),
                    header: document.getElementById("header"),
                    suggestions: document.getElementById("suggestionsDropdown")
                };

        let suggestionTimeout = null;
        let currentSuggestions = [];
        let selectedSuggestionIndex = -1;
        let mediaRecorder = null;
        let audioChunks = [];
        let isRecording = false;

               els.btn.addEventListener("click", search);
        els.input.addEventListener("keypress", e => { 
            if (e.key === "Enter") search(); 
        });
        
        els.micBtn.addEventListener("click", toggleRecording);
        
        els.input.addEventListener("input", handleInput);
        els.input.addEventListener("keydown", handleKeyDown);
        els.input.addEventListener("blur", () => {
            setTimeout(() => hideSuggestions(), 250);
        });

        function handleInput(e) {
            const query = e.target.value.trim();
            
            if (query.length < 1) {
                hideSuggestions();
                return;
            }

            clearTimeout(suggestionTimeout);
            suggestionTimeout = setTimeout(() => fetchSuggestions(query), 150);
        }
        function handleKeyDown(e) {
            if (!els.suggestions.classList.contains('active')) return;

            if (e.key === 'ArrowDown') {
                e.preventDefault();
                selectedSuggestionIndex = Math.min(selectedSuggestionIndex + 1, currentSuggestions.length - 1);
                updateSuggestionSelection();
            } else if (e.key === 'ArrowUp') {
                e.preventDefault();
                selectedSuggestionIndex = Math.max(selectedSuggestionIndex - 1, -1);
                updateSuggestionSelection();
            } else if (e.key === 'Enter' && selectedSuggestionIndex >= 0) {
                e.preventDefault();
                selectSuggestion(currentSuggestions[selectedSuggestionIndex].text);
            } else if (e.key === 'Escape') {
                hideSuggestions();
            }
        }

        async function fetchSuggestions(query) {
            try {
                const res = await fetch(`${API_BASE}/suggest`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ query })
                });
                
                if (!res.ok) throw new Error();
                
                const data = await res.json();
                currentSuggestions = data.suggestions || [];
                selectedSuggestionIndex = -1;
                displaySuggestions(currentSuggestions);
            } catch (err) {
                console.error("Suggestion fetch failed:", err);
                hideSuggestions();
            }
        }

        function displaySuggestions(suggestions) {
            if (!suggestions || suggestions.length === 0) {
                hideSuggestions();
                return;
            }

            els.suggestions.innerHTML = suggestions.map((sug, idx) => `
                <div class="suggestion-item" data-index="${idx}">
                    <span class="suggestion-text">${highlightMatch(escapeHtml(sug.text), els.input.value)}</span>
                </div>
            `).join('');

            // Add click event listeners to each suggestion item
            els.suggestions.querySelectorAll('.suggestion-item').forEach((item, idx) => {
                item.addEventListener('mousedown', (e) => {
                    e.preventDefault(); // Prevent input blur
                    selectSuggestion(suggestions[idx].text);
                });
            });

            els.suggestions.classList.add('active');
        }

        function highlightMatch(text, query) {
            const words = query.toLowerCase().split(' ');
            const lastWord = words[words.length - 1];
            
            if (!lastWord) return text;
            
            const regex = new RegExp(`(${escapeRegex(lastWord)})`, 'gi');
            return text.replace(regex, '<span class="suggestion-term">$1</span>');
        }

        function updateSuggestionSelection() {
            const items = els.suggestions.querySelectorAll('.suggestion-item');
            items.forEach((item, idx) => {
                if (idx === selectedSuggestionIndex) {
                    item.classList.add('selected');
                    item.scrollIntoView({ block: 'nearest' });
                } else {
                    item.classList.remove('selected');
                }
            });
        }

        function selectSuggestion(text) {
            els.input.value = text;
            hideSuggestions();
            els.input.focus();
            search();
        }

        function hideSuggestions() {
            els.suggestions.classList.remove('active');
            currentSuggestions = [];
            selectedSuggestionIndex = -1;
        }
         async function toggleRecording() {
            if (isRecording) {
                stopRecording();
            } else {
                startRecording();
            }
        }

        async function startRecording() {
            try {
                const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
                
                mediaRecorder = new MediaRecorder(stream, {
                    mimeType: 'audio/webm'
                });
                
                audioChunks = [];
                
                mediaRecorder.ondataavailable = (event) => {
                    if (event.data.size > 0) {
                        audioChunks.push(event.data);
                    }
                };
                
                mediaRecorder.onstop = async () => {
                    const audioBlob = new Blob(audioChunks, { type: 'audio/webm' });
                    await processAudio(audioBlob);
                    
                    // Stop all tracks
                    stream.getTracks().forEach(track => track.stop());
                };
                
                mediaRecorder.start();
                isRecording = true;
                els.micBtn.classList.add('recording');
                els.micBtn.title = 'Stop recording';
                
                showToast('Listening... Click again to stop', 'info');
                
            } catch (err) {
                console.error('Microphone access error:', err);
                showToast('Error: Could not access microphone', 'error');
            }
        }

        function stopRecording() {
            if (mediaRecorder && isRecording) {
                mediaRecorder.stop();
                isRecording = false;
                els.micBtn.classList.remove('recording');
                els.micBtn.classList.add('processing');
                els.micBtn.title = 'Voice search';
            }
        }

        async function processAudio(audioBlob) {
            try {
                showToast(' Processing audio...', 'info');
                
                // Convert blob to base64
                const reader = new FileReader();
                reader.readAsDataURL(audioBlob);
                
                reader.onloadend = async () => {
                    const base64Audio = reader.result;
                    
                    // Convert webm to wav using Web Audio API
                    const audioContext = new (window.AudioContext || window.webkitAudioContext)();
                    const arrayBuffer = await audioBlob.arrayBuffer();
                    const audioBuffer = await audioContext.decodeAudioData(arrayBuffer);
                    
                    // Convert to WAV
                    const wavBlob = await audioBufferToWav(audioBuffer);
                    const wavReader = new FileReader();
                    
                    wavReader.onloadend = async () => {
                        const wavBase64 = wavReader.result;
                        
                        // Send to backend
                        const res = await fetch(`${API_BASE}/transcribe`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ audio: wavBase64 })
                        });
                        
                        const data = await res.json();
                        
                        els.micBtn.classList.remove('processing');
                        
                        if (data.success && data.text) {
                            els.input.value = data.text;
                            showToast('✓ Transcribed successfully!', 'success');
                            els.input.focus();
                            
                            // Auto-search after 1 second
                            setTimeout(() => search(), 1000);
                        } else {
                            showToast(data.error || 'Could not transcribe audio', 'error');
                        }
                    };
                    
                    wavReader.readAsDataURL(wavBlob);
                };
                
            } catch (err) {
                console.error('Audio processing error:', err);
                els.micBtn.classList.remove('processing');
                showToast('Error processing audio', 'error');
            }
        }

        function audioBufferToWav(audioBuffer) {
            const numChannels = audioBuffer.numberOfChannels;
            const sampleRate = audioBuffer.sampleRate;
            const format = 1; // PCM
            const bitDepth = 16;
            
            const bytesPerSample = bitDepth / 8;
            const blockAlign = numChannels * bytesPerSample;
            
            const data = audioBuffer.getChannelData(0);
            const dataLength = data.length * bytesPerSample;
            const buffer = new ArrayBuffer(44 + dataLength);
            const view = new DataView(buffer);
            
            // WAV header
            writeString(view, 0, 'RIFF');
            view.setUint32(4, 36 + dataLength, true);
            writeString(view, 8, 'WAVE');
            writeString(view, 12, 'fmt ');
            view.setUint32(16, 16, true);
            view.setUint16(20, format, true);
            view.setUint16(22, numChannels, true);
            view.setUint32(24, sampleRate, true);
            view.setUint32(28, sampleRate * blockAlign, true);
            view.setUint16(32, blockAlign, true);
            view.setUint16(34, bitDepth, true);
            writeString(view, 36, 'data');
            view.setUint32(40, dataLength, true);
            
            // Write audio data
            const volume = 0.8;
            let offset = 44;
            for (let i = 0; i < data.length; i++) {
                const sample = Math.max(-1, Math.min(1, data[i]));
                view.setInt16(offset, sample < 0 ? sample * 0x8000 : sample * 0x7FFF, true);
                offset += 2;
            }
            
            return new Blob([buffer], { type: 'audio/wav' });
        }

        function writeString(view, offset, string) {
            for (let i = 0; i < string.length; i++) {
                view.setUint8(offset + i, string.charCodeAt(i));
            }
        }

        async function search() {
            const query = els.input.value.trim();
            if (!query) { 
                showError("Enter a search query"); 
                return; 
            }

            hideSuggestions();
            hideAll();
            els.loading.classList.add("active");
            els.logo.classList.add("hidden");
            els.searchContainer.classList.add("results-mode");
            els.header.style.display = "flex";

            try {
                const res = await fetch(`${API_BASE}/search`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ query })
                });
                
                if (!res.ok) throw new Error();
                
                const data = await res.json();
                displayResults(data);
            } catch (err) {
                console.error("Search failed:", err);
                showError("Search failed. Make sure the backend server is running.");
                reset();
            }
        }

        function displayResults(data) {
            hideAll();
            
            if (!data.results || data.results.length === 0) { 
                els.noResults.classList.add("active"); 
                return; 
            }

            els.info.textContent = `About ${data.total_results.toLocaleString()} results (${data.search_time} seconds)`;
            els.content.innerHTML = "";
            
            data.results.forEach(r => {
                const div = document.createElement("div");
                div.className = "result-item";
                div.innerHTML = `
                    <div class="result-url">
                        <span class="result-domain"> ${escapeHtml(r.source_type)}</span>
                        <span>› ${escapeHtml(r.doc_id)}</span>
                    </div>
                    <div class="result-title">${escapeHtml(r.title)}</div>
                    <div class="result-snippet">${r.snippet}</div>
                    <div class="result-authors">Authors: ${escapeHtml(r.authors)}</div>
                    <div class="result-metadata">
                        <span>Relevance: ${(r.score * 100).toFixed(1)}%</span>
                        <span>Matched: ${r.matched_terms}/${r.total_query_terms} terms</span>
                    </div>
                    <button class="open-file-btn" onclick="openFile('${escapeHtml(r.doc_id)}', '${escapeHtml(r.source_type)}')">
                         Open File
                    </button>
                `;
                els.content.appendChild(div);
            });
            
            els.container.classList.add("active");
        }

        function showError(msg) {
            hideAll();
            els.error.textContent = msg;
            els.error.classList.add("active");
            setTimeout(() => els.error.classList.remove("active"), 5000);
        }

        function hideAll() {
            ["loading", "container", "noResults", "error"].forEach(k => 
                els[k].classList.remove("active")
            );
        }

        function reset() {
            els.logo.classList.remove("hidden");
            els.searchContainer.classList.remove("results-mode");
            els.header.style.display = "none";
            hideAll();
        }

        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }

        function escapeRegex(str) {
            return str.replace(/[.*+?^${}()|[\]\\\\]/g, '\\\\$&');
        }

        async function openFile(docId, sourceType) {
            try {
                console.log(` Opening file: ${docId} (${sourceType})`);
                
                // Open file in new browser tab
                const url = `${FILE_OPENER_API}/view-file?doc_id=${encodeURIComponent(docId)}&source_type=${encodeURIComponent(sourceType)}`;
                window.open(url, '_blank');
                
                showToast(`Opening ${docId}.json in new tab...`);
                
            } catch (err) {
                console.error('Error opening file:', err);
                showToast('Error: Could not open file', 'error');
            }
        }
        function showToast(message, type = 'success') {
            const toast = document.createElement('div');
            toast.style.position = 'fixed';
            toast.style.bottom = '20px';
            toast.style.right = '20px';
            toast.style.background = type === 'error' ? '#c5221f' : (type === 'info' ? '#5f6368' : '#34a853');
            toast.style.color = 'white';
            toast.style.padding = '12px 20px';
            toast.style.borderRadius = '4px';
            toast.style.boxShadow = '0 2px 8px rgba(0,0,0,0.3)';
            toast.style.zIndex = '10000';
            toast.style.fontSize = '14px';
            toast.textContent = message;
            
            document.body.appendChild(toast);
            
            setTimeout(() => {
                toast.style.opacity = '0';
                toast.style.transition = 'opacity 0.3s';
                setTimeout(() => toast.remove(), 300);
            }, 3000);
        }

        window.addEventListener("load", () => els.input.focus());
    </script>
</body>
</html>
"""

# ============================================================================
# ENHANCED ULTRA-FAST SEARCH ENGINE CLASS
# ============================================================================

class EnhancedSearchEngine:
    def __init__(self, base_path):
        self.base_path = Path(base_path)
        self.lexicon = {}
        self.hot_cache = {}
        self.cold_cache = {}
        self.metadata_cache = {}
        self.forward_index_cache = {}
        self.cache_lock = threading.Lock()
        
        self.stopwords = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
            'of', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during',
            'is', 'was', 'are', 'were', 'been', 'be', 'have', 'has', 'had'
        }
        
        self.hot_prefixes = [
            'cov', 'vir', 'cor', 'vac', 'inf', 'dis', 'tre', 'sym', 'dia',
            'res', 'pat', 'imm', 'tra', 'stu', 'tes', 'dat', 'ana', 'met',
            'pro', 'pol', 'med', 'hea', 'cli', 'epi', 'the', 'can', 'eff'
        ]
        
        self.query_expansions = {
            'covid': ['covid', 'covid-19', 'coronavirus', 'sars-cov-2'],
            'coronavirus': ['coronavirus', 'covid', 'covid-19', 'sars-cov-2'],
            'sars': ['sars', 'sars-cov-2', 'coronavirus'],
            'vaccine': ['vaccine', 'vaccination', 'immunization'],
            'treatment': ['treatment', 'therapy', 'therapeutic'],
            'symptom': ['symptom', 'symptoms', 'clinical'],
            'diagnosis': ['diagnosis', 'diagnostic', 'detection'],
            'transmission': ['transmission', 'spread', 'contagion'],
            'infection': ['infection', 'infected', 'infectious'],
            'pandemic': ['pandemic', 'epidemic', 'outbreak']
        }
        
        self.suggestion_cache = {}
        
        print("🚀 Initializing ENHANCED ULTRA-FAST Search Engine...")
        self.load_lexicon()
        self.preload_hot_buckets()
        print("✅ Search engine ready for maximum speed!\n")
    
    def load_lexicon(self):
        start = time.time()
        lexicon_path = self.base_path / "lexicon" / "cord19_lexicon.json"
        
        with open(lexicon_path, 'rb') as f:
            self.lexicon = json.load(f)
        
        self.sorted_terms = sorted(
            [(term, info['frequency']) for term, info in self.lexicon.items()],
            key=lambda x: x[1],
            reverse=True
        )
        
        duration = time.time() - start
        print(f"✓ Lexicon loaded: {len(self.lexicon):,} terms ({duration:.2f}s)")
    
    @lru_cache(maxsize=1000)
    def get_suggestions(self, query, max_suggestions=6):
        query_lower = query.lower().strip()
        
        if len(query_lower) < 1:
            return []
        
        words = query_lower.split()
        if not words:
            return []
        
        last_word = words[-1]
        
        if last_word in self.suggestion_cache:
            prefix_suggestions = self.suggestion_cache[last_word]
        else:
            prefix_suggestions = []
            for term, freq in self.sorted_terms:
                if term.lower().startswith(last_word):
                    prefix_suggestions.append({'term': term, 'frequency': freq})
                    if len(prefix_suggestions) >= 8:
                        break
            
            self.suggestion_cache[last_word] = prefix_suggestions
        
        suggestions = []
        for item in prefix_suggestions[:max_suggestions]:
            full_suggestion = ' '.join(words[:-1] + [item['term']])
            suggestions.append({
                'text': full_suggestion.strip(),
                'term': item['term'],
                'frequency': item['frequency']
            })
        
        return suggestions
    
    def preload_hot_buckets(self):
        start = time.time()
        print(f"Pre-loading {len(self.hot_prefixes)} hot buckets...")
        
        loaded = 0
        for prefix in self.hot_prefixes:
            bucket_data = self.load_bucket_fast(prefix)
            if bucket_data:
                self.hot_cache[prefix] = bucket_data
                loaded += 1
        
        duration = time.time() - start
        print(f"✓ Hot cache ready: {loaded} buckets ({duration:.2f}s)")
    
    def get_prefix_bucket(self, term):
        term_lower = term.lower()
        
        if not term_lower or not term_lower[0].isalpha():
            return '000'
        
        prefix = term_lower[:3].ljust(3, '_')
        return prefix
    
    @lru_cache(maxsize=1000)
    def load_bucket_fast(self, prefix):
        backward_index_path = self.base_path / "backward_indexing" / f"{prefix}.json"
        
        if not backward_index_path.exists():
            return {}
        
        try:
            with open(backward_index_path, 'rb') as f:
                bucket_data = json.load(f)
            return bucket_data.get('terms', {})
        except:
            return {}
    
    def get_term_documents(self, term):
        prefix = self.get_prefix_bucket(term)
        
        if prefix in self.hot_cache:
            bucket_data = self.hot_cache[prefix]
            return bucket_data.get(term, {})
        
        if prefix in self.cold_cache:
            bucket_data = self.cold_cache[prefix]
            return bucket_data.get(term, {})
        
        bucket_data = self.load_bucket_fast(prefix)
        
        with self.cache_lock:
            self.cold_cache[prefix] = bucket_data
        
        return bucket_data.get(term, {})
    
    @lru_cache(maxsize=500)
    def load_document_metadata(self, doc_id, source_type):
        if doc_id in self.metadata_cache:
            return self.metadata_cache[doc_id]
        
        source_dir = 'pdf_json' if source_type == 'PDF' else 'pmc_json'
        doc_path = self.base_path.parent / "2020-05-01" / source_dir / f"{doc_id}.json"
        
        try:
            with open(doc_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            metadata = {
                'title': 'Untitled Document',
                'abstract': '',
                'authors': []
            }
            
            if 'metadata' in data and 'title' in data['metadata']:
                metadata['title'] = data['metadata']['title']
            
            if 'abstract' in data:
                if isinstance(data['abstract'], list):
                    abstract_parts = []
                    for item in data['abstract']:
                        if isinstance(item, dict) and 'text' in item:
                            abstract_parts.append(item['text'])
                    metadata['abstract'] = ' '.join(abstract_parts)
                elif isinstance(data['abstract'], str):
                    metadata['abstract'] = data['abstract']
            
            if 'metadata' in data and 'authors' in data['metadata']:
                authors = []
                for author in data['metadata']['authors'][:5]:
                    if isinstance(author, dict):
                        first = author.get('first', '')
                        last = author.get('last', '')
                        if first or last:
                            authors.append(f"{first} {last}".strip())
                metadata['authors'] = authors
            
            self.metadata_cache[doc_id] = metadata
            return metadata
            
        except Exception as e:
            return {
                'title': f"Document {doc_id}",
                'abstract': 'Abstract not available.',
                'authors': []
            }
    
    def generate_snippet(self, doc_id, source_type, query_terms, max_length=200):
        metadata = self.load_document_metadata(doc_id, source_type)
        
        abstract = metadata.get('abstract', '')
        if abstract:
            abstract_lower = abstract.lower()
            for term in query_terms:
                if term in abstract_lower:
                    pos = abstract_lower.find(term)
                    start = max(0, pos - 50)
                    end = min(len(abstract), pos + max_length - 50)
                    
                    snippet = abstract[start:end]
                    
                    if start > 0:
                        snippet = '...' + snippet
                    if end < len(abstract):
                        snippet = snippet + '...'
                    
                    for qterm in query_terms:
                        pattern = re.compile(re.escape(qterm), re.IGNORECASE)
                        snippet = pattern.sub(f'<mark>{qterm}</mark>', snippet)
                    
                    return snippet
        
        if abstract:
            snippet = abstract[:max_length]
            if len(abstract) > max_length:
                snippet += '...'
            return snippet
        
        return 'No abstract available.'
    
    def expand_query(self, query_terms):
        expanded = set(query_terms)
        
        for term in query_terms:
            if term in self.query_expansions:
                expanded.update(self.query_expansions[term])
        
        return list(expanded)
    
    @lru_cache(maxsize=500)
    def tokenize_query(self, query):
        query = query.lower()
        words = re.findall(r'\b[a-z][a-z0-9\-]*[a-z0-9]\b|\b[a-z]\b', query)
        tokens = tuple(w for w in words if w not in self.stopwords)
        return tokens
    
    def search(self, query, max_results=20, use_expansion=True):
        search_start = time.time()
        
        query_terms = list(self.tokenize_query(query))
        
        if not query_terms:
            return {
                'query': query,
                'total_results': 0,
                'search_time': 0.0,
                'results': []
            }
        
        if use_expansion:
            expanded_terms = self.expand_query(query_terms)
        else:
            expanded_terms = query_terms
        
        doc_scores = {}
        doc_matched_terms = {}
        doc_source_types = {}
        
        total_docs = 50000
        
        for term in expanded_terms:
            doc_hits = self.get_term_documents(term)
            
            if not doc_hits:
                continue
            
            docs_with_term = len(doc_hits)
            idf = math.log(total_docs / (docs_with_term + 1))
            
            for doc_id, hit_data in doc_hits.items():
                frequency = hit_data.get('frequency', 1)
                tf = math.log(1 + frequency)
                score = tf * idf
                
                source_type = hit_data.get('source_type', 'PDF')
                
                if doc_id in doc_scores:
                    doc_scores[doc_id] += score
                    doc_matched_terms[doc_id] += 1
                else:
                    doc_scores[doc_id] = score
                    doc_matched_terms[doc_id] = 1
                    doc_source_types[doc_id] = source_type
        
        total_terms = len(query_terms)
        for doc_id in doc_scores.keys():
            completeness = doc_matched_terms[doc_id] / total_terms
            doc_scores[doc_id] *= (1.0 + completeness * 2.0)
        
        import heapq
        top_docs = heapq.nlargest(max_results, doc_scores.items(), key=lambda x: x[1])
        
        max_score = top_docs[0][1] if top_docs else 1.0
        
        results = []
        for doc_id, score in top_docs:
            source_type = doc_source_types.get(doc_id, 'PDF')
            metadata = self.load_document_metadata(doc_id, source_type)
            snippet = self.generate_snippet(doc_id, source_type, query_terms)
            
            results.append({
                'doc_id': doc_id,
                'title': metadata['title'],
                'snippet': snippet,
                'score': round(score / max_score, 3),
                'source_type': source_type,
                'matched_terms': doc_matched_terms[doc_id],
                'total_query_terms': total_terms,
                'authors': ', '.join(metadata['authors'][:3]) if metadata['authors'] else 'Unknown'
            })
        
        search_time = time.time() - search_start
        
        return {
            'query': query,
            'total_results': len(results),
            'search_time': round(search_time, 3),
            'results': results,
            'expanded_query': use_expansion and len(expanded_terms) > len(query_terms)
        }
    
    def get_cache_stats(self):
        return {
            'hot_cache_size': len(self.hot_cache),
            'cold_cache_size': len(self.cold_cache),
            'metadata_cache_size': len(self.metadata_cache),
            'forward_cache_size': len(self.forward_index_cache),
            'total_cached_buckets': len(self.hot_cache) + len(self.cold_cache)
        }


# ============================================================================
# GLOBAL ENGINE INSTANCE
# ============================================================================

BASE_PATH = r"res"
search_engine = EnhancedSearchEngine(BASE_PATH)


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.route('/', methods=['GET'])
def serve_frontend():
    """Serve embedded HTML frontend"""
    return Response(HTML_FRONTEND, mimetype='text/html')


@app.route('/search', methods=['POST', 'OPTIONS'])
def search():
    """Search endpoint"""
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    try:
        data = request.get_json()
        query = data.get('query', '').strip()
        use_expansion = data.get('expand', True)
        
        if not query:
            return jsonify({'error': 'Query is required'}), 400
        
        print(f"🔍 Search: {query}")
        
        results = search_engine.search(query, max_results=20, use_expansion=use_expansion)
        
        print(f"⚡ {results['search_time']}s - {results['total_results']} results")
        
        return jsonify(results), 200
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/suggest', methods=['POST', 'OPTIONS'])
def suggest():
    """Autocomplete suggestions endpoint"""
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    try:
        data = request.get_json()
        query = data.get('query', '').strip()
        
        if not query or len(query) < 1:
            return jsonify({'suggestions': []}), 200
        
        suggestions = search_engine.get_suggestions(query, max_suggestions=6)
        
        return jsonify({'suggestions': suggestions}), 200
        
    except Exception as e:
        print(f"❌ Suggestion error: {e}")
        return jsonify({'suggestions': []}), 200


@app.route('/health', methods=['GET'])
def health():
    """Health check"""
    stats = search_engine.get_cache_stats()
    
    return jsonify({
        'status': 'healthy',
        'service': 'CORD-19 Enhanced Ultra-Fast Search Engine',
        'version': '4.0.0 (Integrated)',
        'cache_stats': stats
    }), 200

@app.route('/transcribe', methods=['POST', 'OPTIONS'])
def transcribe():
    """Speech-to-text transcription endpoint"""
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    try:
        data = request.get_json()
        audio_data = data.get('audio', '')
        
        if not audio_data:
            return jsonify({'error': 'No audio data provided'}), 400
        
        # Remove the data URL prefix if present
        if ',' in audio_data:
            audio_data = audio_data.split(',')[1]
        
        # Decode base64 audio
        audio_bytes = base64.b64decode(audio_data)
        
        # Initialize recognizer
        recognizer = sr.Recognizer()
        
        # Convert to AudioFile
        audio_file = sr.AudioFile(io.BytesIO(audio_bytes))
        
        with audio_file as source:
            audio = recognizer.record(source)
        
        # Recognize speech using Google Speech Recognition
        text = recognizer.recognize_google(audio)
        
        print(f"🎤 Transcribed: {text}")
        
        return jsonify({
            'text': text,
            'success': True
        }), 200
        
    except sr.UnknownValueError:
        print("❌ Could not understand audio")
        return jsonify({
            'error': 'Could not understand audio',
            'success': False
        }), 200
        
    except sr.RequestError as e:
        print(f"❌ Speech recognition error: {e}")
        return jsonify({
            'error': 'Speech recognition service error',
            'success': False
        }), 500
        
    except Exception as e:
        print(f"❌ Transcription error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'error': str(e),
            'success': False
        }), 500


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    
    
    import webbrowser
    from threading import Timer
    
    def open_browser():
        frontend_url = "http://localhost:8080"
        print(f" Opening browser: {frontend_url}\n")
        webbrowser.open(frontend_url)
    
    print(" Server: http://localhost:8080")
    print(" Frontend: http://localhost:8080/")
    print("="*60 + "\n")
    
    Timer(0.5, open_browser).start()
    
    try:
        from waitress import serve
        print("✓ Using Waitress (production server)\n")
        serve(app, host='0.0.0.0', port=8080, threads=4)
    except ImportError:
        print("⚠ Using Flask dev server (install waitress for better performance)\n")
        app.run(host='0.0.0.0', port=8080, debug=False, threaded=True)