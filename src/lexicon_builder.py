import os
import re
import json
from pathlib import Path
from typing import Set, Dict, List, Optional
import multiprocessing as mp

class LexiconBuilder:
    """Build lexicons from scratch - clean and efficient"""
    
    def __init__(self, cleaned_data_dir: str, output_dir: str, batch_size: int = 200):
        self.cleaned_data_dir = Path(cleaned_data_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.batch_size = batch_size
        
        # Output files
        self.lexicon_file = self.output_dir / "lexicon.json"
        self.stats_file = self.output_dir / "lexicon_stats.json"
        
        # Load dictionary for validation
        self.english_words = self._load_dictionary()
        
        # Pattern: 2-30 letters only
        self.word_pattern = re.compile(r'\b[a-zA-Z]{2,30}\b')
        
        # Stats tracking
        self.stats = {
            'total_files': 0,
            'unique_words': 0,
            'processing_time': 0
        }
    
    def _load_dictionary(self) -> Set[str]:
        """Load English dictionary for word validation"""
        try:
            import nltk
            from nltk.corpus import words
            try:
                word_list = set(w.lower() for w in words.words())
            except LookupError:
                print("Downloading dictionary...")
                nltk.download('words', quiet=True)
                word_list = set(w.lower() for w in words.words())
            
            print(f"✓ Dictionary loaded: {len(word_list):,} words")
            return word_list
        except:
            print("⚠ No dictionary - using heuristics only")
            return set()
    
    def _is_gibberish(self, word: str) -> bool:
        """Detect gibberish using pattern analysis"""
        vowels = set('aeiou')
        
        # Too many consonants in a row (>5)
        consonant_streak = max_consonants = 0
        for c in word:
            if c not in vowels:
                consonant_streak += 1
                max_consonants = max(max_consonants, consonant_streak)
            else:
                consonant_streak = 0
        if max_consonants > 5:
            return True
        
        # Too many vowels in a row (>4)
        vowel_streak = max_vowels = 0
        for c in word:
            if c in vowels:
                vowel_streak += 1
                max_vowels = max(max_vowels, vowel_streak)
            else:
                vowel_streak = 0
        if max_vowels > 4:
            return True
        
        # Same char repeated 3+ times
        for i in range(len(word) - 2):
            if word[i] == word[i+1] == word[i+2]:
                return True
        
        # No vowels in words >3 chars
        if len(word) > 3 and not any(c in vowels for c in word):
            return True
        
        return False
    
    def _is_valid_word(self, word: str) -> bool:
        """Check if word should be included"""
        word = word.lower()
        
        # Basic checks
        if not (2 <= len(word) <= 30) or not word.isalpha():
            return False
        
        # If in dictionary, accept
        if self.english_words and word in self.english_words:
            return True
        
        # Not in dictionary - check if gibberish
        if self._is_gibberish(word):
            return False
        
        # Accept technical terms and domain vocabulary
        return True
    
    def _process_file(self, txt_file: Path) -> Set[str]:
        """Extract valid words from one file"""
        valid_words = set()
        
        try:
            with open(txt_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Find all words and filter
            words = self.word_pattern.findall(content)
            for word in words:
                word_lower = word.lower()
                if self._is_valid_word(word_lower):
                    valid_words.add(word_lower)
        except Exception as e:
            print(f"⚠ Error: {txt_file.name}: {e}")
        
        return valid_words
    
    def _get_txt_files(self) -> List[Path]:
        """Get all text files from data directory"""
        exclude = {'vocabulary.txt', 'processing_progress.json'}
        return [f for f in self.cleaned_data_dir.glob("*.txt") 
                if f.name not in exclude]
    
    def build(self):
        """Build lexicon from all text files"""
        print("\n" + "="*70)
        print("BUILDING LEXICON FROM SCRATCH")
        print("="*70)
        
        txt_files = self._get_txt_files()
        if not txt_files:
            print("⚠ No text files found!")
            return
        
        print(f"Files: {len(txt_files):,}")
        print(f"Batch size: {self.batch_size}")
        print(f"Workers: {max(1, mp.cpu_count()-1)}")
        print("="*70)
        
        # Process files in batches
        all_words = set()
        num_workers = max(1, mp.cpu_count() - 1)
        total_batches = (len(txt_files) + self.batch_size - 1) // self.batch_size
        
        import time
        start_time = time.time()
        
        for i in range(0, len(txt_files), self.batch_size):
            batch = txt_files[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            
            print(f"\n[Batch {batch_num}/{total_batches}] {len(batch)} files...")
            
            with mp.Pool(num_workers) as pool:
                results = pool.map(self._process_file, batch)
            
            for word_set in results:
                all_words.update(word_set)
            
            print(f"  ✓ Unique words: {len(all_words):,}")
        
        elapsed = time.time() - start_time
        
        # Create lexicon with sequential IDs
        sorted_words = sorted(all_words)
        lexicon = {word: idx for idx, word in enumerate(sorted_words, start=1)}
        
        # Save lexicon
        print(f"\n[Saving]")
        with open(self.lexicon_file, 'w', encoding='utf-8') as f:
            json.dump(lexicon, f, indent=2, ensure_ascii=False)
        
        size_mb = self.lexicon_file.stat().st_size / (1024 * 1024)
        print(f"  ✓ Saved: {self.lexicon_file.name} ({size_mb:.2f} MB)")
        
        # Save stats
        self.stats['total_files'] = len(txt_files)
        self.stats['unique_words'] = len(lexicon)
        self.stats['processing_time'] = elapsed
        
        with open(self.stats_file, 'w', encoding='utf-8') as f:
            json.dump(self.stats, f, indent=2)
        
        print("\n" + "="*70)
        print(f"✓ COMPLETE: {len(lexicon):,} unique words")
        print(f"✓ Time: {elapsed:.2f}s ({elapsed/60:.2f} min)")
        print(f"✓ Speed: {elapsed/len(txt_files):.3f}s per file")
        print("="*70 + "\n")


def main():
    """Build lexicon from cleaned data"""
    CLEANED_DATA_DIR = r"D:\Coding\DSA\data_cleaned"
    OUTPUT_DIR = r"D:\Coding\DSA2\res\lexicon"
    BATCH_SIZE = 200
    
    builder = LexiconBuilder(
        cleaned_data_dir=CLEANED_DATA_DIR,
        output_dir=OUTPUT_DIR,
        batch_size=BATCH_SIZE
    )
    builder.build()


if __name__ == "__main__":
    main()