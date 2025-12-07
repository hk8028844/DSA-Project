"""
File 3: 3_lexicon_builder.py
Fast Lexicon Builder - Builds from existing processed_terms_cycle files
Batch processing with 10K word chunks
"""

import json
import csv
from pathlib import Path
from datetime import datetime
from collections import defaultdict

class FastLexiconBuilder:
    """Fast lexicon builder with batch processing."""
    
    BATCH_SIZE = 10000
    
    def __init__(self, base_path: str, output_dir: str):
        self.base_path = Path(base_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.all_terms_freq = defaultdict(int)
        
    def find_cycle_files(self):
        """Find all processed_terms_cycle*.json files."""
        res_dir = self.base_path / 'res'
        cycle_files = list(res_dir.glob('processed_terms_cycle*.json'))
        return sorted(cycle_files)
    
    def load_all_cycles(self):
        """Load all cycle files and merge terms."""
        cycle_files = self.find_cycle_files()
        
        if not cycle_files:
            print(f"✗ No cycle files found in {self.base_path / 'res'}")
            return False
        
        print(f"Found {len(cycle_files)} cycle file(s)")
        
        for cycle_file in cycle_files:
            print(f"Loading {cycle_file.name}...", end='')
            
            with open(cycle_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            term_frequencies = data.get('term_frequencies', {})
            
            for term, freq in term_frequencies.items():
                self.all_terms_freq[term] += freq
            
            print(f" {len(term_frequencies):,} terms")
        
        print(f"✓ Total unique terms: {len(self.all_terms_freq):,}")
        return True
    
    def build_lexicon_batched(self):
        """Build lexicon in batches."""
        if not self.all_terms_freq:
            return {}
        
        print("Building lexicon...")
        
        # Sort by frequency
        sorted_terms = sorted(self.all_terms_freq.items(), 
                            key=lambda x: x[1], reverse=True)
        
        lexicon = {}
        
        # Process in batches
        total = len(sorted_terms)
        for i in range(0, total, self.BATCH_SIZE):
            batch = sorted_terms[i:i+self.BATCH_SIZE]
            
            for idx, (term, freq) in enumerate(batch, start=i+1):
                lexicon[term] = {
                    'term_id': idx,
                    'term': term,
                    'frequency': freq
                }
            
            if (i + self.BATCH_SIZE) % 50000 == 0:
                print(f"  {i+self.BATCH_SIZE:,}/{total:,} terms processed", end='\r')
        
        print(f"✓ Built lexicon: {len(lexicon):,} terms")
        return lexicon
    
    def save_csv(self, lexicon):
        """Save lexicon to CSV in batches."""
        lexicon_dir = self.output_dir / "lexicon"
        lexicon_dir.mkdir(parents=True, exist_ok=True)
        
        output_path = lexicon_dir / "cord19_lexicon.csv"
        
        print(f"Saving CSV...", end='')
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['term_id', 'term', 'frequency'])
            
            sorted_lex = sorted(lexicon.values(), key=lambda x: x['term_id'])
            
            for entry in sorted_lex:
                writer.writerow([
                    entry['term_id'],
                    entry['term'],
                    entry['frequency']
                ])
        
        print(f" done")
        return str(output_path)
    
    def save_json(self, lexicon):
        """Save lexicon to JSON."""
        lexicon_dir = self.output_dir / "lexicon"
        output_path = lexicon_dir / "cord19_lexicon.json"
        
        print(f"Saving JSON...", end='')
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(lexicon, f, indent=2, ensure_ascii=False)
        
        print(f" done")
        return str(output_path)
    
    def save_term_list(self, lexicon):
        """Save simple term list."""
        lexicon_dir = self.output_dir / "lexicon"
        output_path = lexicon_dir / "terms_by_frequency.txt"
        
        print(f"Saving term list...", end='')
        
        sorted_terms = sorted(lexicon.values(), 
                            key=lambda x: x['frequency'], reverse=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            for entry in sorted_terms:
                f.write(f"{entry['term']}\n")
        
        print(f" done")
        return str(output_path)
    
    def get_stats(self, lexicon):
        """Get statistics."""
        if not lexicon:
            return {}
        
        total_freq = sum(e['frequency'] for e in lexicon.values())
        frequencies = [e['frequency'] for e in lexicon.values()]
        
        return {
            'total_unique_terms': len(lexicon),
            'total_occurrences': total_freq,
            'avg_frequency': total_freq / len(lexicon),
            'median_frequency': sorted(frequencies)[len(frequencies)//2],
            'max_frequency': max(frequencies),
            'min_frequency': min(frequencies)
        }
    
    def display_top_terms(self, lexicon, n=50):
        """Display top terms."""
        print(f"\nTop {n} Terms:")
        print("-" * 60)
        
        sorted_lex = sorted(lexicon.values(), 
                          key=lambda x: x['frequency'], reverse=True)
        
        for i, entry in enumerate(sorted_lex[:n], 1):
            print(f"{i:3d}. {entry['term']:30s} {entry['frequency']:>10,}")


def main():
    """Main execution."""
    BASE_PATH = r"D:\Hamza\cord-19_2020-05-01"
    OUTPUT_DIR = r"D:\Hamza\cord-19_2020-05-01\res"
    
    print("\n" + "="*60)
    print("Fast Lexicon Builder")
    print("="*60)
    print(f"Started: {datetime.now().strftime('%H:%M:%S')}")
    print("="*60 + "\n")
    
    builder = FastLexiconBuilder(BASE_PATH, OUTPUT_DIR)
    
    # Load cycle files
    if not builder.load_all_cycles():
        print("\n✗ No cycle files found!")
        print("Expected location: D:\\Hamza\\cord-19_2020-05-01\\res\\")
        print("Run text preprocessor first to generate cycle files.")
        return
    
    # Build lexicon
    lexicon = builder.build_lexicon_batched()
    
    if not lexicon:
        print("✗ Failed to build lexicon")
        return
    
    # Save outputs
    print("\nSaving outputs:")
    csv_path = builder.save_csv(lexicon)
    json_path = builder.save_json(lexicon)
    txt_path = builder.save_term_list(lexicon)
    
    # Statistics
    stats = builder.get_stats(lexicon)
    
    print("\n" + "="*60)
    print("Statistics:")
    print("="*60)
    print(f"Total unique terms:  {stats['total_unique_terms']:,}")
    print(f"Total occurrences:   {stats['total_occurrences']:,}")
    print(f"Average frequency:   {stats['avg_frequency']:.2f}")
    print(f"Median frequency:    {stats['median_frequency']}")
    print(f"Max frequency:       {stats['max_frequency']:,}")
    print(f"Min frequency:       {stats['min_frequency']}")
    
    builder.display_top_terms(lexicon, 50)
    
    print("\n" + "="*60)
    print("Output Files:")
    print("="*60)
    print(f"CSV:  {csv_path}")
    print(f"JSON: {json_path}")
    print(f"TXT:  {txt_path}")
    print("="*60)
    print(f"Completed: {datetime.now().strftime('%H:%M:%S')}")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()