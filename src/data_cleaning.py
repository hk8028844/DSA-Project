import os
import re
import json
from pathlib import Path
from collections import Counter
import PyPDF2
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.stem import PorterStemmer, WordNetLemmatizer
import nltk
import string

# Download required NLTK data (run once)
def download_nltk_resources():
    """Download all required NLTK resources"""
    resources = {
        'punkt': 'tokenizers/punkt',
        'punkt_tab': 'tokenizers/punkt_tab',
        'stopwords': 'corpora/stopwords',
        'wordnet': 'corpora/wordnet',
        'averaged_perceptron_tagger': 'taggers/averaged_perceptron_tagger',
        'omw-1.4': 'corpora/omw-1.4'
    }
    
    for resource_name, resource_path in resources.items():
        try:
            nltk.data.find(resource_path)
        except LookupError:
            print(f"📥 Downloading {resource_name}...")
            try:
                nltk.download(resource_name, quiet=True)
                print(f"   ✅ {resource_name} downloaded successfully")
            except Exception as e:
                print(f"   ⚠️  Warning: Could not download {resource_name}: {e}")

# Download resources at startup
print("🔧 Checking NLTK resources...")
download_nltk_resources()
print("✅ NLTK setup complete\n")


class AdvancedPDFCleaner:
    def __init__(self, input_dir, output_dir):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.stemmer = PorterStemmer()
        self.lemmatizer = WordNetLemmatizer()
        
        # Create output directory structure
        self.output_dir.mkdir(parents=True, exist_ok=True)
        (self.output_dir / 'metadata').mkdir(exist_ok=True)
        
        # Standard English stop words
        self.stop_words = set(stopwords.words('english'))
        
        # Add common academic/technical stop words
        self.stop_words.update([
            'also', 'may', 'however', 'would', 'could', 'one', 'two', 'first', 
            'second', 'third', 'fig', 'figure', 'table', 'et', 'al', 'pp', 
            'vol', 'no', 'doi', 'http', 'https', 'www', 'com', 'org', 'edu'
        ])
        
        # Domain-specific important terms (astronomy, space, math, graphs, CS)
        self.domain_terms = {
            # Astronomy & Space
            'galaxy', 'galaxies', 'star', 'stars', 'planet', 'planets', 'orbit', 'orbital',
            'solar', 'lunar', 'cosmic', 'cosmology', 'space', 'telescope', 'universe',
            'black', 'hole', 'blackhole', 'nebula', 'supernova', 'quasar', 'pulsar',
            'asteroid', 'comet', 'meteor', 'light', 'year', 'parsec', 'redshift',
            'spectroscopy', 'photometry', 'magnitude', 'luminosity', 'mass',
            'density', 'gravity', 'gravitational', 'dark', 'matter', 'energy',
            'radiation', 'electromagnetic', 'wavelength', 'spectrum', 'celestial',
            'constellation', 'astronomical', 'astrophysics', 'exoplanet',
            
            # Mathematics
            'equation', 'equations', 'theorem', 'theorems', 'proof', 'lemma',
            'function', 'functions', 'derivative', 'derivatives', 'integral', 'integrals',
            'matrix', 'matrices', 'vector', 'vectors', 'scalar', 'tensor',
            'polynomial', 'exponential', 'logarithm', 'logarithmic', 'trigonometric',
            'calculus', 'algebra', 'algebraic', 'geometry', 'geometric', 'topology',
            'differential', 'partial', 'summation', 'series', 'convergence',
            'optimization', 'probability', 'statistics', 'statistical', 'distribution',
            'variance', 'mean', 'median', 'deviation', 'correlation',
            
            # Graphs & Graph Theory
            'graph', 'graphs', 'node', 'nodes', 'vertex', 'vertices', 'edge', 'edges',
            'directed', 'undirected', 'weighted', 'tree', 'trees', 'path', 'paths',
            'cycle', 'cycles', 'adjacency', 'connectivity', 'degree', 'spanning',
            'subgraph', 'clique', 'planar', 'bipartite', 'traversal', 'isomorphic',
            
            # Computer Science
            'algorithm', 'algorithms', 'data', 'structure', 'structures', 'complexity',
            'compute', 'computation', 'computational', 'binary', 'search', 'sort',
            'sorting', 'tree', 'heap', 'stack', 'queue', 'linked', 'list',
            'hash', 'hashing', 'index', 'indexing', 'pointer', 'array', 'recursive',
            'iteration', 'dynamic', 'programming', 'greedy', 'optimization',
            'machine', 'learning', 'neural', 'network', 'networks', 'deep',
            'artificial', 'intelligence', 'model', 'models', 'training', 'testing',
            'database', 'query', 'sql', 'parallel', 'distributed', 'system', 'systems',
            'software', 'hardware', 'memory', 'cache', 'processor', 'cpu', 'gpu',
            'code', 'coding', 'syntax', 'semantic', 'compile', 'runtime'
        }
        
        # Statistics tracker
        self.stats = {
            'total_files': 0,
            'successful': 0,
            'failed': 0,
            'total_words': 0,
            'unique_words': set(),
            'vocabulary': Counter()
        }
        
    def extract_text_from_pdf(self, pdf_path):
        """Extract text from PDF file with better handling"""
        text = ""
        metadata = {'pages': 0, 'title': pdf_path.stem}
        
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                metadata['pages'] = len(pdf_reader.pages)
                
                # Try to extract metadata
                if pdf_reader.metadata:
                    metadata['title'] = pdf_reader.metadata.get('/Title', pdf_path.stem)
                
                for page_num, page in enumerate(pdf_reader.pages):
                    try:
                        page_text = page.extract_text()
                        if page_text:
                            text += page_text + "\n"
                    except Exception as e:
                        print(f"    Warning: Error on page {page_num + 1}: {str(e)}")
                        continue
                        
        except Exception as e:
            print(f"    ❌ Error reading PDF: {str(e)}")
            return None, None
            
        return text, metadata
    
    def advanced_tokenization(self, text):
        """Advanced tokenization with sentence preservation"""
        # Split into sentences first (important for context)
        sentences = sent_tokenize(text)
        
        all_tokens = []
        sentence_boundaries = []  # Track where sentences end
        
        for sentence in sentences:
            # Tokenize each sentence
            tokens = word_tokenize(sentence)
            all_tokens.extend(tokens)
            sentence_boundaries.append(len(all_tokens))
        
        return all_tokens, sentence_boundaries
    
    def is_valid_token(self, token):
        """Determine if a token should be kept"""
        # Must be at least 2 characters
        if len(token) < 2:
            return False
        
        # Check if it's a domain-specific term (always keep)
        if token.lower() in self.domain_terms:
            return True
        
        # Check if it's a stop word (skip unless domain term)
        if token.lower() in self.stop_words:
            return False
        
        # Skip pure numbers
        if token.isdigit():
            return False
        
        # Skip tokens that are just punctuation
        if all(c in string.punctuation for c in token):
            return False
        
        # Skip very long tokens (likely garbage)
        if len(token) > 45:
            return False
        
        # Keep tokens with at least one letter
        if any(c.isalpha() for c in token):
            return True
        
        return False
    
    def normalize_token(self, token):
        """Normalize a token using lemmatization"""
        # Convert to lowercase
        token = token.lower()
        
        # Remove possessives
        token = re.sub(r"'s$", '', token)
        
        # Lemmatize (better than stemming for search engines)
        # Try noun first, then verb
        lemma = self.lemmatizer.lemmatize(token, pos='n')
        if lemma == token:
            lemma = self.lemmatizer.lemmatize(token, pos='v')
        
        return lemma
    
    def clean_text(self, text):
        """Comprehensive text cleaning optimized for search engine indexing"""
        if not text:
            return "", []
        
        # Step 1: Initial cleaning
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        
        # Remove email addresses
        text = re.sub(r'\S+@\S+', '', text)
        
        # Remove citation patterns like [1], [2-5], (Smith et al., 2020)
        text = re.sub(r'\[[\d,\-\s]+\]', '', text)
        text = re.sub(r'\([^)]*\d{4}[^)]*\)', '', text)
        
        # Replace hyphens with spaces (except in compound words)
        text = re.sub(r'(?<!\w)-|-(?!\w)', ' ', text)
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Step 2: Advanced tokenization
        tokens, sentence_boundaries = self.advanced_tokenization(text)
        
        # Step 3: Filter and normalize tokens
        cleaned_tokens = []
        for token in tokens:
            # Remove punctuation from token edges
            token = token.strip(string.punctuation)
            
            if not token:
                continue
            
            # Check if valid
            if self.is_valid_token(token):
                # Normalize
                normalized = self.normalize_token(token)
                cleaned_tokens.append(normalized)
                
                # Update vocabulary stats
                self.stats['vocabulary'][normalized] += 1
                self.stats['unique_words'].add(normalized)
        
        # Step 4: Create final cleaned text
        cleaned_text = ' '.join(cleaned_tokens)
        
        return cleaned_text, cleaned_tokens
    
    def save_metadata(self, filename, metadata, word_count, unique_words):
        """Save metadata for each document (useful for ranking later)"""
        meta_file = self.output_dir / 'metadata' / f"{filename.replace('.txt', '_meta.json')}"
        
        metadata_full = {
            'filename': filename,
            'original_title': metadata.get('title', ''),
            'pages': metadata.get('pages', 0),
            'word_count': word_count,
            'unique_word_count': unique_words,
            'avg_words_per_page': word_count / metadata.get('pages', 1)
        }
        
        with open(meta_file, 'w', encoding='utf-8') as f:
            json.dump(metadata_full, f, indent=2)
    
    def process_single_file(self, pdf_file):
        """Process a single PDF file with extensive cleaning"""
        pdf_path = self.input_dir / pdf_file
        
        # Skip if not a PDF
        if not pdf_file.lower().endswith('.pdf'):
            return False
        
        print(f"\n📄 Processing: {pdf_file}")
        self.stats['total_files'] += 1
        
        # Extract text
        raw_text, metadata = self.extract_text_from_pdf(pdf_path)
        if raw_text is None or not raw_text.strip():
            print(f"    ❌ Failed to extract text or file is empty")
            self.stats['failed'] += 1
            return False
        
        print(f"    📖 Extracted {len(raw_text)} characters from {metadata['pages']} pages")
        
        # Clean text
        cleaned_text, tokens = self.clean_text(raw_text)
        
        if not cleaned_text or len(tokens) < 10:
            print(f"    ⚠️  Warning: Insufficient content after cleaning ({len(tokens)} tokens)")
            self.stats['failed'] += 1
            return False
        
        # Update statistics
        self.stats['total_words'] += len(tokens)
        unique_in_doc = len(set(tokens))
        
        print(f"    ✨ Cleaned: {len(tokens)} words ({unique_in_doc} unique)")
        
        # Save cleaned text
        output_filename = pdf_file.replace('.pdf', '.txt')
        output_path = self.output_dir / output_filename
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(cleaned_text)
            
            # Save metadata
            self.save_metadata(output_filename, metadata, len(tokens), unique_in_doc)
            
            print(f"    ✅ Saved to: {output_filename}")
            self.stats['successful'] += 1
            return True
            
        except Exception as e:
            print(f"    ❌ Error saving file: {str(e)}")
            self.stats['failed'] += 1
            return False
    
    def process_all_files(self):
        """Process all PDF files in input directory"""
        # Get list of PDF files
        pdf_files = sorted([f.name for f in self.input_dir.glob('*.pdf')])
        
        if not pdf_files:
            print(f"❌ No PDF files found in {self.input_dir}")
            return
        
        print("\n" + "="*70)
        print(f"🚀 ADVANCED PDF CLEANING FOR SEARCH ENGINE")
        print("="*70)
        print(f"📂 Input:  {self.input_dir}")
        print(f"📁 Output: {self.output_dir}")
        print(f"📚 Found {len(pdf_files)} PDF files")
        print("="*70)
        
        # Process each file
        for i, pdf_file in enumerate(pdf_files, 1):
            print(f"\n[{i}/{len(pdf_files)}]", end=" ")
            self.process_single_file(pdf_file)
        
        # Save overall statistics
        self.save_global_statistics()
        
        # Print summary
        self.print_summary()
    
    def save_global_statistics(self):
        """Save global statistics about the entire dataset"""
        stats_file = self.output_dir / 'dataset_statistics.json'
        
        # Get top 50 most common words
        top_words = dict(self.stats['vocabulary'].most_common(50))
        
        stats_data = {
            'total_files_processed': self.stats['total_files'],
            'successful': self.stats['successful'],
            'failed': self.stats['failed'],
            'total_words': self.stats['total_words'],
            'unique_words': len(self.stats['unique_words']),
            'avg_words_per_document': self.stats['total_words'] / max(self.stats['successful'], 1),
            'top_50_words': top_words
        }
        
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats_data, f, indent=2)
        
        print(f"\n💾 Saved global statistics to: dataset_statistics.json")
    
    def print_summary(self):
        """Print detailed summary"""
        print("\n" + "="*70)
        print("📊 PROCESSING COMPLETE - SUMMARY")
        print("="*70)
        print(f"✅ Successfully processed:  {self.stats['successful']}")
        print(f"❌ Failed:                  {self.stats['failed']}")
        print(f"📝 Total words:             {self.stats['total_words']:,}")
        print(f"🔤 Unique words (lexicon):  {len(self.stats['unique_words']):,}")
        print(f"📄 Avg words per document:  {self.stats['total_words'] / max(self.stats['successful'], 1):.0f}")
        print("="*70)
        print(f"\n📁 Output directory:  {self.output_dir}")
        print(f"📁 Metadata directory: {self.output_dir / 'metadata'}")
        print("\n✨ Your dataset is ready for lexicon generation!")
        print("="*70)


def main():
    # Configure paths
    INPUT_DIR = r"D:\Coding\Python\downloads"
    OUTPUT_DIR = r"D:\Coding\Python\downloads_cleaned"
    
    print("\n🔍 ADVANCED SEARCH ENGINE DATA CLEANER")
    print("="*70)
    
    # Create cleaner and process files
    cleaner = AdvancedPDFCleaner(INPUT_DIR, OUTPUT_DIR)
    cleaner.process_all_files()
    
    print("\n🎉 All done! You can now proceed to lexicon generation.")


if __name__ == "__main__":
    main()