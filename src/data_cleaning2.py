import os
import re
import json
from pathlib import Path
import pdfplumber
from typing import Set, Dict, List
from collections import Counter

class PDFToCleanTXT:
    def __init__(self, input_dir: str, output_dir: str):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Progress tracking file
        self.progress_file = self.output_dir / "processing_progress.json"
        self.frequency_file = self.output_dir / "word_frequencies.json"
        
        self.processed_files = self.load_progress()
        
        # Domain-specific valid words
        self.domain_keywords = self.get_domain_keywords()
        
        # Valid word pattern: only letters, 2-25 chars
        self.valid_word_pattern = re.compile(r'^[a-zA-Z]{2,25}$')
        
        # Global word frequency counter
        self.word_frequency = self.load_frequencies()
        
        # Word to ID mapping
        self.word_to_id = {}
        self.next_id = 1
    
    def get_domain_keywords(self) -> Set[str]:
        """Domain-specific keywords for space, astronomy, graph theory, CS, and math"""
        return {
            # Space & Astronomy
            'space', 'astronomy', 'star', 'stars', 'planet', 'planets', 'galaxy', 'galaxies',
            'universe', 'cosmic', 'cosmos', 'orbit', 'orbital', 'satellite', 'satellites',
            'moon', 'moons', 'solar', 'stellar', 'interstellar', 'nebula', 'nebulae',
            'asteroid', 'asteroids', 'comet', 'comets', 'meteor', 'meteors', 'meteorite',
            'celestial', 'astronomical', 'telescope', 'telescopes', 'observatory', 'light',
            'gravity', 'gravitational', 'radiation', 'emission', 'spectrum', 'spectral',
            'redshift', 'blueshift', 'photon', 'photons', 'quasar', 'quasars', 'pulsar',
            'supernova', 'supernovae', 'neutron', 'black', 'hole', 'dark', 'matter',
            'energy', 'mass', 'distance', 'velocity', 'acceleration', 'rotation',
            
            # Graph Theory
            'graph', 'graphs', 'node', 'nodes', 'vertex', 'vertices', 'edge', 'edges',
            'directed', 'undirected', 'weighted', 'unweighted', 'connected', 'disconnected',
            'path', 'paths', 'cycle', 'cycles', 'tree', 'trees', 'forest', 'spanning',
            'degree', 'adjacent', 'adjacency', 'neighbor', 'neighbors', 'subgraph',
            'complete', 'bipartite', 'planar', 'acyclic', 'traversal', 'search',
            'depth', 'breadth', 'shortest', 'minimum', 'maximum', 'optimal',
            'connectivity', 'component', 'components', 'clique', 'coloring', 'matching',
            
            # Computer Science
            'algorithm', 'algorithms', 'data', 'structure', 'structures', 'complexity',
            'time', 'polynomial', 'exponential', 'logarithmic', 'linear', 'quadratic',
            'sorting', 'searching', 'optimization', 'computation', 'computational',
            'binary', 'array', 'arrays', 'list', 'lists', 'queue', 'queues', 'stack',
            'heap', 'hash', 'hashing', 'table', 'dictionary', 'set', 'sets',
            'recursion', 'recursive', 'iteration', 'iterative', 'dynamic', 'programming',
            'greedy', 'divide', 'conquer', 'backtracking', 'branch', 'bound',
            'memory', 'space', 'efficient', 'problem', 'problems', 'solution', 'solutions',
            'input', 'output', 'process', 'processing', 'parallel', 'distributed',
            'network', 'networks', 'routing', 'flow', 'bandwidth', 'latency',
            
            # Mathematics
            'matrix', 'matrices', 'vector', 'vectors', 'scalar', 'dimension', 'dimensional',
            'algebra', 'algebraic', 'linear', 'nonlinear', 'equation', 'equations',
            'function', 'functions', 'derivative', 'derivatives', 'integral', 'integrals',
            'calculus', 'differential', 'numerical', 'analysis', 'theorem', 'theorems',
            'proof', 'proofs', 'lemma', 'corollary', 'axiom', 'axioms', 'conjecture',
            'number', 'numbers', 'integer', 'integers', 'real', 'complex', 'rational',
            'prime', 'factor', 'factors', 'multiple', 'divisor', 'modulo', 'modular',
            'probability', 'statistics', 'statistical', 'distribution', 'random',
            'variable', 'variables', 'variance', 'deviation', 'mean', 'median', 'mode',
            'set', 'sets', 'subset', 'union', 'intersection', 'complement', 'cardinality',
            'relation', 'relations', 'mapping', 'bijection', 'injection', 'surjection',
            'geometric', 'geometry', 'topology', 'topological', 'metric', 'distance',
            'limit', 'limits', 'convergence', 'divergence', 'sequence', 'series',
            'sum', 'product', 'addition', 'multiplication', 'division', 'subtraction',
            'power', 'exponent', 'logarithm', 'exponential', 'base', 'coefficient',
            
            # Common technical terms
            'method', 'methods', 'approach', 'technique', 'techniques', 'model', 'models',
            'system', 'systems', 'type', 'types', 'property', 'properties', 'characteristic',
            'value', 'values', 'parameter', 'parameters', 'result', 'results', 'conclusion',
            'theory', 'theoretical', 'practical', 'application', 'applications', 'case',
            'example', 'examples', 'instance', 'general', 'specific', 'particular',
            'definition', 'defined', 'denote', 'notation', 'symbol', 'symbols',
            'represent', 'representation', 'form', 'expression', 'formula', 'formulas',
            'calculate', 'calculation', 'compute', 'computed', 'determine', 'find',
            'obtain', 'derived', 'given', 'consider', 'assume', 'suppose', 'let',
            'show', 'demonstrate', 'prove', 'verify', 'check', 'test', 'measure',
            'order', 'size', 'length', 'width', 'height', 'area', 'volume', 'rate',
            'ratio', 'proportion', 'percentage', 'total', 'partial', 'complete',
            'approximate', 'exact', 'estimate', 'precision', 'accuracy', 'error',
            'bound', 'bounds', 'upper', 'lower', 'range', 'domain', 'codomain',
            'finite', 'infinite', 'discrete', 'continuous', 'smooth', 'regular',
            'simple', 'compound', 'single', 'multiple', 'unique', 'common', 'rare',
            'state', 'states', 'condition', 'conditions', 'constraint', 'constraints',
            'requirement', 'requirements', 'satisfy', 'satisfies', 'holds', 'valid',
            'true', 'false', 'correct', 'incorrect', 'positive', 'negative', 'zero',
            'operation', 'operations', 'operator', 'operators', 'apply', 'applied',
            'perform', 'execute', 'implement', 'implementation', 'design', 'construct'
        }
    
    def load_progress(self) -> Set[str]:
        """Load list of already processed files"""
        if self.progress_file.exists():
            with open(self.progress_file, 'r') as f:
                return set(json.load(f))
        return set()
    
    def save_progress(self):
        """Save processing progress"""
        with open(self.progress_file, 'w') as f:
            json.dump(list(self.processed_files), f, indent=2)
    
    def load_frequencies(self) -> Counter:
        """Load word frequencies from previous runs"""
        if self.frequency_file.exists():
            with open(self.frequency_file, 'r') as f:
                freq_dict = json.load(f)
                return Counter(freq_dict)
        return Counter()
    
    def save_frequencies(self):
        """Save word frequencies"""
        with open(self.frequency_file, 'w') as f:
            json.dump(dict(self.word_frequency), f, indent=2)
    
    def is_valid_word(self, word: str) -> bool:
        """Check if word is domain-relevant and valid"""
        # Must match pattern: only letters, 2-25 chars
        if not self.valid_word_pattern.match(word):
            return False
        
        word_lower = word.lower()
        
        # Must be in domain keywords
        if word_lower not in self.domain_keywords:
            return False
        
        # Check for unusual capitalization (combined words)
        caps = sum(1 for c in word if c.isupper())
        if caps > 1 and not word.isupper():
            return False
        
        return True
    
    def clean_text(self, text: str) -> str:
        """Remove artifacts and clean text"""
        if not text:
            return ""
        
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        
        # Remove emails
        text = re.sub(r'\S+@\S+', '', text)
        
        # Remove citations
        text = re.sub(r'\[\d+\]|\(\d{4}\)|\[\w+,?\s*\d{4}\]', '', text)
        
        # Remove page numbers
        text = re.sub(r'^\s*\d+\s*$', '', text, flags=re.MULTILINE)
        
        # Remove headers/footers
        text = re.sub(r'(?i)(page|chapter)\s+\d+', '', text)
        
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def extract_valid_words(self, text: str) -> List[str]:
        """Extract and validate words from text"""
        # Split into words
        words = re.findall(r'\b[a-zA-Z]{2,25}\b', text)
        
        # Filter valid words
        valid_words = []
        for word in words:
            if self.is_valid_word(word):
                word_lower = word.lower()
                valid_words.append(word_lower)
                self.word_frequency[word_lower] += 1
        
        return valid_words
    
    def extract_text_from_pdf(self, pdf_path: Path) -> List[str]:
        """Extract valid words from PDF"""
        valid_words = []
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        cleaned = self.clean_text(text)
                        if cleaned:
                            words = self.extract_valid_words(cleaned)
                            valid_words.extend(words)
            
            return valid_words
            
        except Exception as e:
            print(f"  ✗ Error: {e}")
            return []
    
    def save_temp_text(self, pdf_name: str, words: List[str]):
        """Save temporary text file with all extracted words"""
        if not words:
            return
        
        output_file = self.output_dir / f"{Path(pdf_name).stem}.txt"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(' '.join(words))
    
    def assign_word_ids(self):
        """Assign unique IDs to words with frequency > 2"""
        # Filter words with frequency > 2
        filtered_words = {word: freq for word, freq in self.word_frequency.items() if freq > 2}
        
        # Sort alphabetically for consistent ordering
        sorted_words = sorted(filtered_words.keys())
        
        # Assign IDs
        for word in sorted_words:
            self.word_to_id[word] = self.next_id
            self.next_id += 1
    
    def cleanup_txt_files(self):
        """Remove words with frequency <= 2 from all TXT files"""
        print("\n[CLEANUP] Filtering TXT files (removing words with freq ≤ 2)...")
        
        txt_files = list(self.output_dir.glob("*.txt"))
        cleaned_count = 0
        
        for txt_file in txt_files:
            try:
                # Read existing words
                with open(txt_file, 'r', encoding='utf-8') as f:
                    words = f.read().split()
                
                # Filter words with frequency > 2
                filtered_words = [w for w in words if w in self.word_to_id]
                
                if filtered_words:
                    # Rewrite file with filtered words
                    with open(txt_file, 'w', encoding='utf-8') as f:
                        f.write(' '.join(filtered_words))
                    cleaned_count += 1
                else:
                    # Remove empty files
                    txt_file.unlink()
                    print(f"  ✗ Removed empty file: {txt_file.name}")
                    
            except Exception as e:
                print(f"  ✗ Error cleaning {txt_file.name}: {e}")
        
        print(f"  ✓ Cleaned {cleaned_count} TXT files")
    
    def save_vocabulary(self):
        """Save vocabulary with IDs and frequencies"""
        vocab_file = self.output_dir / "vocabulary.txt"
        vocab_json = self.output_dir / "vocabulary.json"
        
        # Create structured data
        vocab_data = []
        for word, word_id in sorted(self.word_to_id.items(), key=lambda x: x[1]):
            vocab_data.append({
                'id': word_id,
                'word': word,
                'frequency': self.word_frequency[word]
            })
        
        # Save as formatted text
        with open(vocab_file, 'w', encoding='utf-8') as f:
            f.write("VOCABULARY - Domain-Specific Words (Frequency > 2)\n")
            f.write("=" * 70 + "\n\n")
            f.write(f"{'ID':<8} {'WORD':<25} {'FREQUENCY':<10}\n")
            f.write("-" * 70 + "\n")
            
            for item in vocab_data:
                f.write(f"{item['id']:<8} {item['word']:<25} {item['frequency']:<10}\n")
            
            f.write("\n" + "=" * 70 + "\n")
            f.write(f"Total unique words: {len(vocab_data)}\n")
        
        # Save as JSON
        with open(vocab_json, 'w', encoding='utf-8') as f:
            json.dump(vocab_data, f, indent=2)
        
        print(f"\n✓ Vocabulary saved:")
        print(f"  - {vocab_file.name}")
        print(f"  - {vocab_json.name}")
        print(f"  - Total unique words: {len(vocab_data)}")
    
    def process_pdfs(self):
        """Process all PDFs in input directory with batch saving"""
        pdf_files = list(self.input_dir.glob("*.pdf"))
        total = len(pdf_files)
        
        if total == 0:
            print("No PDF files found in input directory.")
            return
        
        print(f"Found {total} PDF files")
        print(f"Domain filters: Space, Astronomy, Graph Theory, CS, Math")
        print(f"Already processed: {len(self.processed_files)}")
        print("=" * 70)
        
        # Process each PDF and save immediately
        print("\n[PHASE 1] Extracting words and saving TXT files...")
        processed_count = 0
        skipped_count = 0
        
        for idx, pdf_path in enumerate(pdf_files, 1):
            if pdf_path.name in self.processed_files:
                skipped_count += 1
                continue
            
            print(f"  [{idx}/{total}] {pdf_path.name}")
            
            # Extract words
            words = self.extract_text_from_pdf(pdf_path)
            
            if words:
                # Save immediately (with all words, cleanup later)
                self.save_temp_text(pdf_path.name, words)
                print(f"    ✓ Saved {len(words)} words")
                processed_count += 1
            else:
                print(f"    ✗ No valid words extracted")
            
            # Mark as processed
            self.processed_files.add(pdf_path.name)
            
            # Save progress and frequencies after each file
            self.save_progress()
            self.save_frequencies()
        
        print(f"\n  Newly processed: {processed_count}")
        print(f"  Skipped: {skipped_count}")
        
        # Assign IDs to words with frequency > 2
        print("\n[PHASE 2] Filtering words (frequency > 2) and assigning IDs...")
        self.assign_word_ids()
        print(f"  ✓ Found {len(self.word_to_id)} unique words with freq > 2")
        
        # Cleanup all TXT files (remove words with freq <= 2)
        self.cleanup_txt_files()
        
        # Save vocabulary
        self.save_vocabulary()
        
        print(f"\n{'='*70}")
        print(f"Processing complete!")
        print(f"Files processed: {processed_count}")
        print(f"Unique words (freq > 2): {len(self.word_to_id)}")
        print(f"Output directory: {self.output_dir}")
        print(f"{'='*70}")


def main():
    """Main execution function"""
    INPUT_DIR = r"D:/Coding/DSA/data_raw"
    OUTPUT_DIR = r"D:/Coding/DSA2/data_cleaned"
    
    converter = PDFToCleanTXT(INPUT_DIR, OUTPUT_DIR)
    converter.process_pdfs()


if __name__ == "__main__":
    main()