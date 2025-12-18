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
from semantic_search import FastSemanticSearch

app = Flask(__name__)
CORS(app)

# Embedded frontend
HTML_FRONTEND = """
<!DOCTYPE html>
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
            font-size: 16px; 
            color: #70757a; 
        }
        
        .filters { 
            display: flex; 
            gap: 12px; 
            margin-bottom: 20px; 
            flex-wrap: wrap; 
        }
        .filter-btn { 
            padding: 8px 16px; 
            border: 1px solid #dfe1e5; 
            border-radius: 20px; 
            background: white; 
            cursor: pointer; 
            font-size: 13px; 
            color: #5f6368; 
            transition: all 0.2s; 
        }
        .filter-btn:hover { 
            border-color: #4285f4; 
            color: #4285f4; 
        }
        .filter-btn.active { 
            background: #4285f4; 
            color: white; 
            border-color: #4285f4; 
        }
        
        @media (max-width: 768px) {
            .search-container { margin-top: 80px; }
            .logo-large { font-size: 60px; }
            .search-input { font-size: 14px; }
            .result-title { font-size: 18px; }
        }
        
        .voice-status {
            text-align: center;
            margin-top: 10px;
            font-size: 14px;
            color: #5f6368;
            min-height: 20px;
        }
    </style>
</head>
<body>
    <div class="header">
        <div class="logo">
            <span>S</span><span>e</span><span>a</span><span>r</span><span>c</span><span>h</span>
        </div>
    </div>

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

    <div class="loading" id="loading"></div>
    
    <div class="results-container" id="resultsContainer">
        <div class="results-info" id="resultsInfo"></div>
        <div id="results"></div>
    </div>

    <script>
        let currentSuggestionIndex = -1;
        let suggestions = [];
        let mediaRecorder;
        let audioChunks = [];

        const searchInput = document.getElementById('searchInput');
        const searchBtn = document.getElementById('searchBtn');
        const micBtn = document.getElementById('micBtn');
        const voiceStatus = document.getElementById('voiceStatus');
        const suggestionsDropdown = document.getElementById('suggestionsDropdown');
        const loading = document.getElementById('loading');
        const resultsContainer = document.getElementById('resultsContainer');
        const results = document.getElementById('results');
        const resultsInfo = document.getElementById('resultsInfo');
        const searchContainer = document.getElementById('searchContainer');
        const logoLarge = document.getElementById('logoLarge');

        // Search functionality
        searchBtn.addEventListener('click', performSearch);
        searchInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter' && currentSuggestionIndex === -1) {
                performSearch();
            }
        });

        // Autocomplete
        let suggestionTimeout;
        searchInput.addEventListener('input', () => {
            clearTimeout(suggestionTimeout);
            const query = searchInput.value.trim();
            
            if (query.length > 0) {
                suggestionTimeout = setTimeout(() => fetchSuggestions(query), 150);
            } else {
                hideSuggestions();
            }
        });

        // Keyboard navigation for suggestions
        searchInput.addEventListener('keydown', (e) => {
            if (!suggestionsDropdown.classList.contains('active')) return;
            
            if (e.key === 'ArrowDown') {
                e.preventDefault();
                currentSuggestionIndex = Math.min(currentSuggestionIndex + 1, suggestions.length - 1);
                updateSuggestionSelection();
            } else if (e.key === 'ArrowUp') {
                e.preventDefault();
                currentSuggestionIndex = Math.max(currentSuggestionIndex - 1, -1);
                updateSuggestionSelection();
            } else if (e.key === 'Enter' && currentSuggestionIndex >= 0) {
                e.preventDefault();
                selectSuggestion(currentSuggestionIndex);
            } else if (e.key === 'Escape') {
                hideSuggestions();
            }
        });

        // Click outside to close suggestions
        document.addEventListener('click', (e) => {
            if (!searchInput.contains(e.target) && !suggestionsDropdown.contains(e.target)) {
                hideSuggestions();
            }
        });

        async function fetchSuggestions(query) {
            try {
                const response = await fetch('/suggest', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ query })
                });
                
                const data = await response.json();
                suggestions = data.suggestions || [];
                displaySuggestions(suggestions);
            } catch (error) {
                console.error('Suggestion error:', error);
            }
        }

        function displaySuggestions(suggestionList) {
            if (suggestionList.length === 0) {
                hideSuggestions();
                return;
            }

            suggestionsDropdown.innerHTML = suggestionList.map((item, index) => `
                <div class="suggestion-item" data-index="${index}">
                    <span class="search-icon" style="font-size: 16px;"></span>
                    <span class="suggestion-text">${escapeHtml(item.text)}</span>
                </div>
            `).join('');

            suggestionsDropdown.classList.add('active');
            currentSuggestionIndex = -1;

            // Add click handlers
            suggestionsDropdown.querySelectorAll('.suggestion-item').forEach((item, index) => {
                item.addEventListener('click', () => selectSuggestion(index));
            });
        }

        function updateSuggestionSelection() {
            const items = suggestionsDropdown.querySelectorAll('.suggestion-item');
            items.forEach((item, index) => {
                item.classList.toggle('selected', index === currentSuggestionIndex);
            });

            if (currentSuggestionIndex >= 0) {
                searchInput.value = suggestions[currentSuggestionIndex].text;
            }
        }

        function selectSuggestion(index) {
            if (index >= 0 && index < suggestions.length) {
                searchInput.value = suggestions[index].text;
                hideSuggestions();
                performSearch();
            }
        }

        function hideSuggestions() {
            suggestionsDropdown.classList.remove('active');
            suggestionsDropdown.innerHTML = '';
            currentSuggestionIndex = -1;
        }

        async function performSearch() {
            const query = searchInput.value.trim();
            if (!query) return;

            hideSuggestions();
            loading.classList.add('active');
            resultsContainer.classList.remove('active');
            searchContainer.classList.add('results-mode');
            logoLarge.classList.add('hidden');

            try {
                const response = await fetch('/search', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ query, expand: true })
                });

                const data = await response.json();
                displayResults(data);
            } catch (error) {
                console.error('Search error:', error);
                results.innerHTML = '<div class="no-results">An error occurred. Please try again.</div>';
            } finally {
                loading.classList.remove('active');
                resultsContainer.classList.add('active');
            }
        }

        function displayResults(data) {
            if (data.total_results === 0) {
                results.innerHTML = '<div class="no-results">No results found. Try different keywords.</div>';
                resultsInfo.textContent = '';
                return;
            }

            resultsInfo.textContent = `About ${data.total_results} results (${data.search_time} seconds)`;

            results.innerHTML = data.results.map(result => `
                <div class="result-item">
                    <div class="result-url">
                        <span class="result-domain">📄 ${result.source_type}</span>
                        <span>› ${result.doc_id}</span>
                    </div>
                    <div class="result-title" onclick="openDocument('${result.doc_id}', '${result.source_type}')" style="cursor: pointer;">${escapeHtml(result.title)}</div>
                    <div class="result-snippet">${result.snippet}</div>
                    ${result.authors ? `<div class="result-authors">Authors: ${escapeHtml(result.authors)}</div>` : ''}
                    <div class="result-metadata">
                        <span>Score: ${result.score}</span>
                        <span>Matched: ${result.matched_terms}/${result.total_query_terms} terms</span>
                    </div>
                </div>
            `).join('');
        }

        // Open document in new tab via file_opener service
        async function openDocument(docId, sourceType) {
            try {
                // Open in new tab using the file viewer
                const url = `http://localhost:8081/view-file?doc_id=${docId}&source_type=${sourceType}`;
                window.open(url, '_blank');
            } catch (error) {
                console.error('Error opening document:', error);
                alert('Failed to open document. Make sure the file opener service is running on port 8081.');
            }
        }

        // Voice search functionality - IMPROVED
        let recognition = null;
        let isListening = false;

        // Try to use Web Speech API for better recognition
        if ('webkitSpeechRecognition' in window || 'SpeechRecognition' in window) {
            const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
            recognition = new SpeechRecognition();
            recognition.continuous = false;
            recognition.interimResults = false;
            recognition.lang = 'en-US';
            recognition.maxAlternatives = 1;

            recognition.onstart = () => {
                isListening = true;
                micBtn.classList.add('recording');
                voiceStatus.textContent = 'Listening... Speak now!';
            };

            recognition.onresult = (event) => {
                const transcript = event.results[0][0].transcript;
                searchInput.value = transcript;
                voiceStatus.textContent = `Heard: "${transcript}"`;
                setTimeout(() => {
                    performSearch();
                    voiceStatus.textContent = '';
                }, 1000);
            };

            recognition.onerror = (event) => {
                console.error('Speech recognition error:', event.error);
                voiceStatus.textContent = `Error: ${event.error}. Try again.`;
                setTimeout(() => voiceStatus.textContent = '', 3000);
                isListening = false;
                micBtn.classList.remove('recording');
            };

            recognition.onend = () => {
                isListening = false;
                micBtn.classList.remove('recording');
            };
        }

        micBtn.addEventListener('click', toggleVoiceRecording);

        async function toggleVoiceRecording() {
            // Use Web Speech API if available (MUCH better for voice recognition)
            if (recognition) {
                if (isListening) {
                    recognition.stop();
                    voiceStatus.textContent = '';
                } else {
                    try {
                        recognition.start();
                    } catch (error) {
                        console.error('Recognition error:', error);
                        voiceStatus.textContent = 'Could not start voice recognition';
                        setTimeout(() => voiceStatus.textContent = '', 3000);
                    }
                }
            } else {
                // Fallback to old method if Web Speech API not available
                if (micBtn.classList.contains('recording')) {
                    stopRecording();
                } else {
                    startRecording();
                }
            }
        }

        // Fallback recording method (less accurate)
        async function startRecording() {
            try {
                const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
                mediaRecorder = new MediaRecorder(stream);
                audioChunks = [];

                mediaRecorder.ondataavailable = (event) => {
                    audioChunks.push(event.data);
                };

                mediaRecorder.onstop = async () => {
                    const audioBlob = new Blob(audioChunks, { type: 'audio/wav' });
                    await transcribeAudio(audioBlob);
                    stream.getTracks().forEach(track => track.stop());
                };

                mediaRecorder.start();
                micBtn.classList.add('recording');
                voiceStatus.textContent = 'Listening...';
            } catch (error) {
                console.error('Microphone error:', error);
                voiceStatus.textContent = 'Microphone access denied';
            }
        }

        function stopRecording() {
            if (mediaRecorder && mediaRecorder.state === 'recording') {
                mediaRecorder.stop();
                micBtn.classList.remove('recording');
                micBtn.classList.add('processing');
                voiceStatus.textContent = 'Processing...';
            }
        }

        async function transcribeAudio(audioBlob) {
            try {
                const reader = new FileReader();
                reader.readAsDataURL(audioBlob);
                
                reader.onloadend = async () => {
                    const base64Audio = reader.result;
                    
                    const response = await fetch('/transcribe', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ audio: base64Audio })
                    });

                    const data = await response.json();
                    
                    if (data.success && data.text) {
                        searchInput.value = data.text;
                        voiceStatus.textContent = `Heard: "${data.text}"`;
                        setTimeout(() => {
                            performSearch();
                            voiceStatus.textContent = '';
                        }, 1000);
                    } else {
                        voiceStatus.textContent = data.error || 'Could not understand audio';
                        setTimeout(() => voiceStatus.textContent = '', 3000);
                    }
                };
            } catch (error) {
                console.error('Transcription error:', error);
                voiceStatus.textContent = 'Transcription failed';
                setTimeout(() => voiceStatus.textContent = '', 3000);
            } finally {
                micBtn.classList.remove('processing');
            }
        }

        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
    </script>
</body>
</html>
"""


# ============================================================================
# ENHANCED SEARCH ENGINE CLASS
# ============================================================================

class EnhancedSearchEngine:
    def __init__(self, base_path):
        self.base_path = Path(base_path)
        
        self.lexicon = {}
        self.sorted_terms = []
        
        # Build term prefix tree for fast lookups
        self.term_prefix_tree = {}
        
        self.hot_cache = {}
        self.cold_cache = {}
        self.metadata_cache = {}
        self.forward_index_cache = {}
        self.suggestion_cache = {}
        
        self.cache_lock = threading.Lock()
        
        self.hot_prefixes = [
            'cov', 'vir', 'cor', 'inf', 'sar', 'res', 'cov', 'dis', 'pat',
            'imm', 'ant', 'hos', 'dia', 'tes', 'cli', 'tre', 'vac', 'the',
            'pan', 'out', 'tra', 'sym', 'iso', 'qua', 'pol', 'pub', 'hea'
        ]
        print("\n[SEMANTIC] Initializing semantic search...")
        self.semantic = FastSemanticSearch(base_path)
        print("[SEMANTIC] ✓ Ready\n")
        
        self._initialize()
    
    def _initialize(self):
        print("\n" + "="*60)
        print("Initializing Enhanced Search Engine...")
        print("="*60 + "\n")
        
        self.load_lexicon()
        self.build_prefix_tree()
        self.preload_hot_buckets()
        
        print("\n" + "="*60)
        print("✓ Search Engine Ready")
        print("="*60 + "\n")
    
    def load_lexicon(self):
        start = time.time()
        print("Loading lexicon...")
        
        lexicon_path = self.base_path / "lexicon" / "cord19_lexicon.json"
        if not lexicon_path.exists():
            raise FileNotFoundError(f"Lexicon not found: {lexicon_path}")
        
        with open(lexicon_path, 'rb') as f:
            self.lexicon = json.load(f)
        
        self.sorted_terms = sorted(
            [(term, info['frequency']) for term, info in self.lexicon.items()],
            key=lambda x: x[1],
            reverse=True
        )
        
        duration = time.time() - start
        print(f"Lexicon loaded: {len(self.lexicon):,} terms ({duration:.2f}s)")
    
    def build_prefix_tree(self):
        """Build a prefix tree for O(log n) suggestion lookups"""
        start = time.time()
        print("Building prefix tree for fast suggestions...")
        
        for term, freq in self.sorted_terms:
            term_lower = term.lower()
            # Store by increasing prefix lengths for efficient lookup
            for i in range(1, min(len(term_lower) + 1, 10)):
                prefix = term_lower[:i]
                if prefix not in self.term_prefix_tree:
                    self.term_prefix_tree[prefix] = []
                # Only keep top terms for each prefix to save memory
                if len(self.term_prefix_tree[prefix]) < 20:
                    self.term_prefix_tree[prefix].append({'term': term, 'frequency': freq})
        
        duration = time.time() - start
        print(f"Prefix tree built: {len(self.term_prefix_tree):,} prefixes ({duration:.2f}s)")
    
    def get_suggestions(self, query, max_suggestions=6):
        """Fast suggestion lookup using prefix tree"""
        query_lower = query.lower().strip()
        
        if len(query_lower) < 1:
            return []
        
        words = query_lower.split()
        if not words:
            return []
        
        last_word = words[-1]
        
        # Check cache first
        if last_word in self.suggestion_cache:
            prefix_suggestions = self.suggestion_cache[last_word]
        else:
            # Use prefix tree for O(1) lookup instead of linear search
            prefix_suggestions = self.term_prefix_tree.get(last_word, [])
            
            # If no exact prefix match, try to find closest matches
            if not prefix_suggestions and len(last_word) > 1:
                # Try shorter prefixes
                for i in range(len(last_word) - 1, 0, -1):
                    shorter_prefix = last_word[:i]
                    prefix_suggestions = self.term_prefix_tree.get(shorter_prefix, [])
                    if prefix_suggestions:
                        # Filter to only include terms that start with our original prefix
                        prefix_suggestions = [
                            item for item in prefix_suggestions 
                            if item['term'].lower().startswith(last_word)
                        ]
                        break
            
            # Cache the results
            self.suggestion_cache[last_word] = prefix_suggestions[:8]
            prefix_suggestions = self.suggestion_cache[last_word]
        
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
            bucket_data = self._load_bucket_from_disk(prefix)
            if bucket_data:
                self.hot_cache[prefix] = bucket_data
                loaded += 1
        
        duration = time.time() - start
        print(f"Hot cache ready: {loaded} buckets ({duration:.2f}s)")
    
    def get_prefix_bucket(self, term):
        term_lower = term.lower()
        
        if not term_lower or not term_lower[0].isalpha():
            return '000'
        
        prefix = term_lower[:3].ljust(3, '_')
        return prefix
    
    def _load_bucket_from_disk(self, prefix):
        """Load bucket from disk without caching decorator"""
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
        
        # Check hot cache first
        if prefix in self.hot_cache:
            bucket_data = self.hot_cache[prefix]
            return bucket_data.get(term, {})
        
        # Check cold cache
        if prefix in self.cold_cache:
            bucket_data = self.cold_cache[prefix]
            return bucket_data.get(term, {})
        
        # Load from disk and cache
        bucket_data = self._load_bucket_from_disk(prefix)
        
        with self.cache_lock:
            self.cold_cache[prefix] = bucket_data
        
        return bucket_data.get(term, {})
    
    def load_forward_index(self, doc_id, source_type='PDF'):
        cache_key = f"{source_type}_{doc_id}"
        
        if cache_key in self.forward_index_cache:
            return self.forward_index_cache[cache_key]
        
        # Map source types to actual folder names
        folder_map = {
            'PDF': 'pdf_json',
            'PMC': 'pmc_json'
        }
        folder_name = folder_map.get(source_type, 'pdf_json')
        forward_dir = self.base_path / "forward_indexing" / folder_name
        forward_path = forward_dir / f"{doc_id}.json"
        
        if not forward_path.exists():
            return {}
        
        try:
            with open(forward_path, 'rb') as f:
                forward_data = json.load(f)
            
            with self.cache_lock:
                self.forward_index_cache[cache_key] = forward_data
            
            return forward_data
        except:
            return {}
    
    def load_document_metadata(self, doc_id, source_type='PDF'):
        cache_key = f"{source_type}_{doc_id}"
        
        if cache_key in self.metadata_cache:
            return self.metadata_cache[cache_key]
        
        try:
            metadata_dir = self.base_path / "metadata"
            metadata_file = metadata_dir / f"{doc_id}.json"
            
            if not metadata_file.exists():
                return {
                    'title': f"Document {doc_id}",
                    'abstract': 'Abstract not available.',
                    'authors': []
                }
            
            with open(metadata_file, 'rb') as f:
                data = json.load(f)
            
            metadata = {
                'title': data.get('metadata', {}).get('title', f"Document {doc_id}"),
                'abstract': data.get('abstract', [{}])[0].get('text', 'Abstract not available.'),
                'authors': []
            }
            
            if 'metadata' in data and 'authors' in data['metadata']:
                authors = []
                for author in data['metadata']['authors'][:5]:
                    if isinstance(author, dict):
                        first = author.get('first', '')
                        last = author.get('last', '')
                        if first or last:
                            authors.append(f"{first} {last}".strip())
                metadata['authors'] = authors
            
            self.metadata_cache[cache_key] = metadata
            return metadata
            
        except Exception as e:
            print(f"Could not load {doc_id}: {e}")
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
            if term in self.lexicon:
                term_info = self.lexicon[term]
                
                synonyms = term_info.get('synonyms', [])
                for syn in synonyms[:2]:
                    expanded.add(syn)
                
                related = term_info.get('related_terms', [])
                for rel in related[:1]:
                    expanded.add(rel)
        
        return list(expanded)
    
    def search(self, query, max_results=20, use_expansion=True):
        search_start = time.time()
        
        query_lower = query.lower().strip()
        query_terms = [t for t in re.findall(r'\b\w+\b', query_lower) if len(t) > 1]
        
        if not query_terms:
            return {
                'query': query,
                'total_results': 0,
                'search_time': 0.0,
                'results': []
            }
        
        # OPTIMIZATION 1: Fast semantic expansion with caching
        if use_expansion:
            expansion_map = self.semantic.expand_query(query_terms)
            expanded_terms = []
            for original, exp_list in expansion_map.items():
                expanded_terms.extend(exp_list)
            expanded_terms = list(dict.fromkeys(expanded_terms))
            
            # Log expansion
            if len(expanded_terms) > len(query_terms):
                print(f"[SEMANTIC] {len(query_terms)} → {len(expanded_terms)} terms")
        else:
            expanded_terms = query_terms
        
        # OPTIMIZATION 2: Pre-allocate with proper types for speed
        doc_scores = {}
        doc_matched_terms = {}
        doc_source_types = {}
        doc_term_matches = defaultdict(set)  # Track which terms matched
        
        total_docs = 50000
        
        # OPTIMIZATION 3: Batch process terms for better cache locality
        term_docs_batch = {}
        for term in expanded_terms:
            doc_hits = self.get_term_documents(term)
            if doc_hits:
                term_docs_batch[term] = (doc_hits, len(doc_hits))
        
        # OPTIMIZATION 4: Single pass scoring with semantic boost
        for term, (doc_hits, docs_with_term) in term_docs_batch.items():
            idf = math.log(total_docs / (docs_with_term + 1))
            
            # Determine if this is an original term or semantic expansion
            is_original = term in query_terms
            
            # SEMANTIC SCORING: Calculate semantic similarity for expanded terms
            semantic_boost = 1.5 if is_original else 1.0
            
            # If it's a semantic expansion, apply semantic score
            if not is_original:
                # Find which original term this came from
                for orig_term in query_terms:
                    if term in expansion_map.get(orig_term, []):
                        # Apply semantic similarity score
                        semantic_sim = self.semantic.semantic_score(orig_term, term)
                        semantic_boost = 1.0 + (semantic_sim * 0.3)  # Up to 30% boost for strong synonyms
                        break
            
            for doc_id, hit_data in doc_hits.items():
                frequency = hit_data.get('frequency', 1)
                tf = math.log(1 + frequency)
                
                # OPTIMIZATION 5: Inline calculation with semantic boost
                score = tf * idf * semantic_boost
                
                source_type = hit_data.get('source_type', 'PDF')
                
                if doc_id in doc_scores:
                    doc_scores[doc_id] += score
                    doc_matched_terms[doc_id] += 1
                    doc_term_matches[doc_id].add(term)
                else:
                    doc_scores[doc_id] = score
                    doc_matched_terms[doc_id] = 1
                    doc_source_types[doc_id] = source_type
                    doc_term_matches[doc_id].add(term)
        
        # OPTIMIZATION 6: Vectorized completeness calculation
        total_terms = len(query_terms)
        original_term_bonus = 2.0
        semantic_term_bonus = 0.5
        
        for doc_id, score in doc_scores.items():
            # Count how many original terms matched
            original_matches = sum(1 for t in query_terms if t in doc_term_matches[doc_id])
            semantic_matches = doc_matched_terms[doc_id] - original_matches
            
            # Weighted completeness: original terms count more
            completeness = (original_matches * original_term_bonus + semantic_matches * semantic_term_bonus) / (total_terms * original_term_bonus)
            completeness = min(completeness, 1.0)  # Cap at 1.0
            
            # Apply completeness boost
            doc_scores[doc_id] *= (1.0 + completeness * 2.0)
        
        # OPTIMIZATION 7: Use heapq for efficient top-k selection
        import heapq
        if len(doc_scores) <= max_results:
            top_docs = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)
        else:
            top_docs = heapq.nlargest(max_results, doc_scores.items(), key=lambda x: x[1])
        
        max_score = top_docs[0][1] if top_docs else 1.0
        
        # OPTIMIZATION 8: Batch load metadata and generate results
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
                'authors': ', '.join(metadata['authors'][:3]) if metadata['authors'] else 'Unknown',
                'semantic_boost': len(doc_term_matches[doc_id]) > len([t for t in query_terms if t in doc_term_matches[doc_id]])
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
            'total_cached_buckets': len(self.hot_cache) + len(self.cold_cache),
            'semantic_enabled': True
        }


# ============================================================================
# GLOBAL ENGINE INSTANCE
# ============================================================================

import os
import sys

# Get the directory where the script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Go up one level to the project root
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
# Set BASE_PATH to the res folder
BASE_PATH = os.path.join(PROJECT_ROOT, "res")

search_engine = EnhancedSearchEngine(BASE_PATH)


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.route('/', methods=['GET'])
def home():
    """Serve the embedded HTML frontend"""
    return Response(HTML_FRONTEND, mimetype='text/html')


@app.route('/api', methods=['GET'])
def api_info():
    """API info"""
    return jsonify({
        'service': 'CORD-19 Search Engine API',
        'version': '4.0.0',
        'status': 'running',
        'endpoints': {
            '/search': 'POST - Search documents',
            '/suggest': 'POST - Get autocomplete suggestions',
            '/transcribe': 'POST - Speech-to-text',
            '/health': 'GET - Health check'
        }
    }), 200


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
        
        print(f"Search: {query}")
        
        results = search_engine.search(query, max_results=20, use_expansion=use_expansion)
        
        print(f"{results['search_time']}s - {results['total_results']} results")
        
        return jsonify(results), 200
        
    except Exception as e:
        print(f"Error: {e}")
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
        print(f"Suggestion error: {e}")
        return jsonify({'suggestions': []}), 200


@app.route('/health', methods=['GET'])
def health():
    """Health check"""
    stats = search_engine.get_cache_stats()
    
    return jsonify({
        'status': 'healthy',
        'service': 'CORD-19 Enhanced Ultra-Fast Search Engine',
        'version': '4.0.0',
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
        
        print(f"Transcribed: {text}")
        
        return jsonify({
            'text': text,
            'success': True
        }), 200
        
    except sr.UnknownValueError:
        print("Could not understand audio")
        return jsonify({
            'error': 'Could not understand audio',
            'success': False
        }), 200
        
    except sr.RequestError as e:
        print(f"Speech recognition error: {e}")
        return jsonify({
            'error': 'Speech recognition service error',
            'success': False
        }), 500
        
    except Exception as e:
        print(f"Transcription error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'error': str(e),
            'success': False
        }), 500


# Main

if __name__ == '__main__':
    print("=" * 60)
    print("CORD-19 Search Engine API Server")
    print("=" * 60)
    print("Server: http://localhost:8080")
    print("API Docs: http://localhost:8080/")
    print("=" * 60 + "\n")
    
    try:
        from waitress import serve
        print("Using Waitress (production server)\n")
        serve(app, host='0.0.0.0', port=8080, threads=4)
    except ImportError:
        print("Using Flask dev server (install waitress for better performance)\n")
        app.run(host='0.0.0.0', port=8080, debug=False, threaded=True)