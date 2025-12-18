import pickle
import os
from pathlib import Path
from collections import defaultdict
import time
from functools import lru_cache

# Optional imports for full embeddings
try:
    import numpy as np
    import gensim.downloader as api
    EMBEDDINGS_AVAILABLE = True
except ImportError:
    EMBEDDINGS_AVAILABLE = False
    print("[Warning] numpy/gensim not installed. Full embeddings unavailable.")
    print("[Info] FastSemanticSearch will still work!")


class SemanticSearchEngine:
    """
    Efficient semantic search implementation using word embeddings.
    Handles synonym expansion and semantic similarity matching.
    """
    
    def __init__(self, base_path, model_name='glove-wiki-gigaword-100'):
        """
        Initialize semantic search engine
        
        Args:
            base_path: Base directory for index files
            model_name: Pre-trained embedding model name
                       Options: 'glove-wiki-gigaword-50' (faster)
                               'glove-wiki-gigaword-100' (balanced)
                               'glove-wiki-gigaword-200' (more accurate)
        """
        if not EMBEDDINGS_AVAILABLE:
            raise ImportError(
                "numpy and gensim are required for SemanticSearchEngine. "
                "Install with: pip install numpy gensim\n"
                "Or use FastSemanticSearch instead (no dependencies needed)"
            )
        
        self.base_path = Path(base_path)
        self.model_name = model_name
        self.embeddings = None
        self.word_vectors = {}
        self.similarity_cache = {}
        
        # Performance settings
        self.min_similarity = 0.6 
        self.max_expansions = 3    
        
        print(f"[Semantic Search] Initializing with {model_name}...")
        self._load_or_download_embeddings()
        print(f"[Semantic Search] Ready with {len(self.word_vectors)} vectors")
    
    def _load_or_download_embeddings(self):
        """Load pre-trained embeddings or download if not available"""
        cache_file = self.base_path / 'semantic' / f'{self.model_name}.pkl'
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Try to load from cache
        if cache_file.exists():
            print(f"[Semantic Search] Loading cached embeddings...")
            try:
                with open(cache_file, 'rb') as f:
                    self.word_vectors = pickle.load(f)
                print(f"[Semantic Search] Loaded {len(self.word_vectors)} cached vectors")
                return
            except Exception as e:
                print(f"[Semantic Search] Cache load failed: {e}, downloading...")
        
        # Download and cache
        print(f"[Semantic Search] Downloading {self.model_name}...")
        print(f"[Semantic Search] This is a one-time download (~100MB)...")
        
        try:
            self.embeddings = api.load(self.model_name)
            
            # Convert to dict for faster access
            print(f"[Semantic Search] Converting to optimized format...")
            for word in self.embeddings.index_to_key:
                self.word_vectors[word.lower()] = self.embeddings[word]
            
            # Save to cache
            print(f"[Semantic Search] Caching embeddings...")
            with open(cache_file, 'wb') as f:
                pickle.dump(self.word_vectors, f, protocol=pickle.HIGHEST_PROTOCOL)
            
            print(f"[Semantic Search] Cached {len(self.word_vectors)} vectors")
            
        except Exception as e:
            print(f"[Semantic Search] ERROR: Could not load embeddings: {e}")
            print(f"[Semantic Search] Semantic search will be disabled")
            self.word_vectors = {}
    
    def get_vector(self, word):
        """Get embedding vector for a word"""
        word = word.lower()
        return self.word_vectors.get(word)
    
    @lru_cache(maxsize=10000)
    def compute_similarity(self, word1, word2):
        """
        Compute cosine similarity between two words
        Cached for performance
        """
        v1 = self.get_vector(word1)
        v2 = self.get_vector(word2)
        
        if v1 is None or v2 is None:
            return 0.0
        
        # Cosine similarity
        dot_product = np.dot(v1, v2)
        norm1 = np.linalg.norm(v1)
        norm2 = np.linalg.norm(v2)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return dot_product / (norm1 * norm2)
    
    def find_similar_words(self, word, top_k=5):
        
        if not self.word_vectors:
            return []
        
        word_lower = word.lower()
        word_vec = self.get_vector(word_lower)
        
        if word_vec is None:
            return []
        
        # Use cache key
        cache_key = f"{word_lower}_{top_k}"
        if cache_key in self.similarity_cache:
            return self.similarity_cache[cache_key]
        
        # Compute similarities efficiently
        start = time.time()
        
        target_len = len(word_lower)
        candidates = []
        
        for w, vec in self.word_vectors.items():
            # Skip same word
            if w == word_lower:
                continue
            
            # Length filter for efficiency
            if abs(len(w) - target_len) > 4:
                continue
            
            # Skip very short/long words
            if len(w) < 3 or len(w) > 20:
                continue
            
            candidates.append(w)
            
            # Limit candidates for speed
            if len(candidates) > 2000:
                break
        
        # Compute similarities in batch
        similarities = []
        for candidate in candidates:
            sim = self.compute_similarity(word_lower, candidate)
            if sim >= self.min_similarity:
                similarities.append((candidate, sim))
        
        # Sort and get top-k
        similarities.sort(key=lambda x: x[1], reverse=True)
        similar_words = [w for w, _ in similarities[:top_k]]
        
        # Cache result
        self.similarity_cache[cache_key] = similar_words
        
        elapsed = time.time() - start
        if elapsed > 0.1: 
            print(f"[Semantic Search] find_similar_words('{word}') took {elapsed:.3f}s")
        
        return similar_words
    
    def expand_query(self, query_terms):
        """
        Expand query with semantically similar terms
        Optimized for multi-word queries
        
        Args:
            query_terms: List of query terms
            
        Returns:
            Dictionary mapping original terms to expanded terms
        """
        if not self.word_vectors:
            return {term: [term] for term in query_terms}
        
        start = time.time()
        expanded = {}
        
        for term in query_terms:
            # Start with original term
            expanded[term] = [term]
            
            # Find similar words
            similar = self.find_similar_words(term, top_k=self.max_expansions)
            
            # Add to expanded list
            for sim_word in similar:
                if sim_word not in expanded[term]:
                    expanded[term].append(sim_word)
        
        elapsed = time.time() - start
        
        # Log expansion details
        total_expanded = sum(len(terms) for terms in expanded.values())
        print(f"[Semantic Search] Expanded {len(query_terms)} terms to {total_expanded} in {elapsed:.3f}s")
        for term, exp_terms in expanded.items():
            if len(exp_terms) > 1:
                print(f"  '{term}' -> {exp_terms[1:]}")
        
        return expanded
    
    def semantic_score(self, query_term, document_term):
        """
        Compute semantic similarity score between query and document terms
        
        Returns:
            Float between 0 and 1 (1 = exact match, >0.6 = semantically similar)
        """
        # Exact match gets perfect score
        if query_term.lower() == document_term.lower():
            return 1.0
        
        # Otherwise compute similarity
        return max(0.0, self.compute_similarity(query_term, document_term))
    
    def rank_with_semantics(self, query_terms, document_terms, base_score=1.0):
        """
        Enhance ranking score with semantic similarity
        
        Args:
            query_terms: List of terms in query
            document_terms: List of terms in document
            base_score: Base TF-IDF or frequency score
            
        Returns:
            Enhanced score incorporating semantic similarity
        """
        if not query_terms or not document_terms:
            return base_score
        
        # Convert to sets for efficiency
        query_set = set(t.lower() for t in query_terms)
        doc_set = set(t.lower() for t in document_terms)
        
        # Exact match boost
        exact_matches = len(query_set & doc_set)
        exact_score = exact_matches / len(query_set)
        
        # Semantic similarity boost
        semantic_score = 0.0
        for q_term in query_set:
            if q_term in doc_set:
                # Already counted in exact matches
                continue
            
            # Find best semantic match in document
            max_sim = 0.0
            for d_term in doc_set:
                sim = self.compute_similarity(q_term, d_term)
                max_sim = max(max_sim, sim)
            
            if max_sim >= self.min_similarity:
                semantic_score += max_sim
        
        # Normalize semantic score
        if len(query_set) > exact_matches:
            semantic_score /= (len(query_set) - exact_matches)
        else:
            semantic_score = 0.0
        
        # Combine scores
        combined_score = base_score * (1.0 + exact_score * 1.5 + semantic_score * 0.5)
        
        return combined_score
    
    def get_stats(self):
        """Get semantic search statistics"""
        return {
            'model': self.model_name,
            'vocabulary_size': len(self.word_vectors),
            'similarity_cache_size': len(self.similarity_cache),
            'min_similarity': self.min_similarity,
            'max_expansions': self.max_expansions
        }


# Optimized lightweight version for faster loading
class FastSemanticSearch:
    """
    Lightweight semantic search using only a subset of embeddings
    Faster initialization, suitable for production
    """
    
    def __init__(self, base_path):
        """Initialize with minimal embeddings"""
        self.base_path = Path(base_path)
        self.word_vectors = {}
        self.similarity_cache = {}

        self.synonyms = self._build_medical_synonyms()
        
        print(f"[Fast Semantic] Ready with {len(self.synonyms)} synonym groups")
    
    def _build_medical_synonyms(self):
        """
        Build domain-specific synonym dictionary
        For CORD-19, focus on medical/scientific terms
        """
        synonyms = {
            'covid': ['coronavirus', 'sars-cov-2', 'covid-19', 'pandemic'],
            'coronavirus': ['covid', 'sars-cov-2', 'covid-19'],
            'virus': ['viral', 'virion', 'pathogen'],
            'disease': ['illness', 'disorder', 'condition', 'syndrome'],
            'treatment': ['therapy', 'intervention', 'medication'],
            'vaccine': ['vaccination', 'immunization', 'inoculation'],
            'symptom': ['symptoms', 'manifestation', 'sign'],
            'patient': ['patients', 'case', 'individual'],
            'study': ['research', 'investigation', 'analysis'],
            'protein': ['proteins', 'polypeptide'],
            'cell': ['cellular', 'cells'],
            'infection': ['infected', 'infectious'],
            'antibody': ['antibodies', 'immunoglobulin'],
            'mortality': ['death', 'fatality', 'deaths'],
            'diagnosis': ['diagnostic', 'detection'],
            'immune': ['immunity', 'immunological'],
            'respiratory': ['pulmonary', 'lung'],
            'transmission': ['spread', 'contagion'],
            'epidemic': ['outbreak', 'pandemic'],
            'hospital': ['hospitalization', 'clinical', 'medical'],
        }
        
        # Make bidirectional
        expanded = {}
        for key, values in synonyms.items():
            expanded[key] = values
            for value in values:
                if value not in expanded:
                    expanded[value] = [key] + [v for v in values if v != value]
        
        return expanded
    
    def expand_query(self, query_terms):
        """Fast query expansion using synonym dictionary"""
        expanded = {}
        
        for term in query_terms:
            term_lower = term.lower()
            expanded[term] = [term]
            
            # Add synonyms if available
            if term_lower in self.synonyms:
                synonyms = self.synonyms[term_lower][:3]  # Limit to 3
                expanded[term].extend(synonyms)
        
        return expanded
    
    def semantic_score(self, query_term, document_term):
        """Simple semantic scoring"""
        query_lower = query_term.lower()
        doc_lower = document_term.lower()
        
        if query_lower == doc_lower:
            return 1.0
        
        # Check if they're synonyms
        if query_lower in self.synonyms:
            if doc_lower in self.synonyms[query_lower]:
                return 0.8
        
        return 0.0


# Functions

def semantic_search():
    """Test semantic search functionality"""
    print("\n" + "="*60)
    print("Testing Semantic Search Module")
    print("="*60 + "\n")
    
    # Use fast version
    ss = FastSemanticSearch(Path.cwd())
    
    # Query expansion
    test_queries = [
        ['covid'],
        ['virus', 'infection'],
        ['vaccine', 'treatment'],
        ['respiratory', 'disease']
    ]
    
    for query in test_queries:
        print(f"\nQuery: {query}")
        expanded = ss.expand_query(query)
        for term, exp in expanded.items():
            print(f"  '{term}' -> {exp}")
    
    # Semantic scoring
    print("\n" + "-"*60)
    print("Semantic Similarity Scores:")
    print("-"*60)
    
    test_pairs = [
        ('covid', 'covid'),
        ('covid', 'coronavirus'),
        ('disease', 'illness'),
        ('virus', 'pathogen')
    ]
    
    for term1, term2 in test_pairs:
        score = ss.semantic_score(term1, term2)
        print(f"  '{term1}' <-> '{term2}': {score:.2f}")
    
    print("\n" + "="*60 + "\n")


if __name__ == '__main__':
    semantic_search()