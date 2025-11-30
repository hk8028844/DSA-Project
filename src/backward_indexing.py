import os
import json
import time
from pathlib import Path
from typing import Dict, List, Set
from collections import defaultdict
import string

class BackwardIndexBuilder:
    """builds inverted index from forward indexes - distributes into barrel files"""
    
    def __init__(self, forward_index_dir: str, lexicon_path: str, output_dir: str, batch_size: int = 100):
        self.forward_index_dir = Path(forward_index_dir)
        self.lexicon_path = Path(lexicon_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.batch_size = batch_size
        
        # load lexicon to map word_id back to words
        print("Loading lexicon...")
        with open(self.lexicon_path, 'r', encoding='utf-8') as f:
            self.lexicon = json.load(f)
        
        # create reverse mapping: id -> word
        self.id_to_word = {word_id: word for word, word_id in self.lexicon.items()}
        print(f"✓ Loaded {len(self.lexicon):,} words")
        
        # inverted index: word_id -> {doc_name -> [positions]}
        self.inverted_index = defaultdict(lambda: defaultdict(list))
        
        # generate all barrel names (aaa to zzz)
        self.barrel_names = self._generate_barrel_names()
        print(f"✓ Will create {len(self.barrel_names)} barrel files")
        
        # stats
        self.stats = {
            'files_processed': 0,
            'total_words_indexed': 0,
            'barrels_created': 0,
            'processing_time': 0
        }
    
    def _generate_barrel_names(self) -> List[str]:
        """generate all possible 3-letter combinations (aaa-zzz)"""
        letters = string.ascii_lowercase
        barrels = []
        for a in letters:
            for b in letters:
                for c in letters:
                    barrels.append(f"{a}{b}{c}")
        return barrels
    
    def _get_barrel_name(self, word: str) -> str:
        """determine which barrel a word belongs to based on first 3 chars"""
        # pad short words with 'a'
        word_padded = (word + 'aaa')[:3].lower()
        return word_padded
    
    def _get_forward_index_files(self) -> List[Path]:
        """get all forward index JSON files"""
        files = list(self.forward_index_dir.glob("*.json"))
        # exclude stats file
        return [f for f in files if f.name != "indexing_stats.json"]
    
    def _process_forward_index(self, forward_file: Path):
        """read forward index and flip it into inverted structure"""
        try:
            with open(forward_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # get document name and its forward index
            doc_name = list(data.keys())[0]
            forward_index = data[doc_name]
            
            # flip the mapping: for each [word_id, position]
            for word_id, position in forward_index:
                # add to inverted index
                self.inverted_index[word_id][doc_name].append(position)
            
            return True
            
        except Exception as e:
            print(f"⚠ Error processing {forward_file.name}: {e}")
            return False
    
    def _save_barrels(self):
        """distribute inverted index into barrel files"""
        print("\n[Distributing into barrels]")
        
        # organize by barrels
        barrels = defaultdict(dict)
        
        for word_id, doc_positions in self.inverted_index.items():
            # get the actual word
            if word_id not in self.id_to_word:
                continue
            
            word = self.id_to_word[word_id]
            barrel_name = self._get_barrel_name(word)
            
            # add to appropriate barrel
            barrels[barrel_name][str(word_id)] = dict(doc_positions)
        
        print(f"  ✓ Organized into {len(barrels)} barrels")
        
        # save each barrel
        saved_count = 0
        for barrel_name, barrel_data in barrels.items():
            if not barrel_data:  # skip empty barrels
                continue
            
            output_file = self.output_dir / f"inverted_{barrel_name}.json"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(barrel_data, f, indent=2)
            
            saved_count += 1
            
            if saved_count % 100 == 0:
                print(f"  ✓ Saved {saved_count} barrels...")
        
        print(f"  ✓ Saved {saved_count} barrel files")
        self.stats['barrels_created'] = saved_count
    
    def build_inverted_index(self):
        """main function to build inverted index from forward indexes"""
        print("\n" + "=" * 80)
        print("BUILDING BACKWARD (INVERTED) INDEX")
        print("=" * 80)
        
        forward_files = self._get_forward_index_files()
        if not forward_files:
            print("⚠ No forward index files found!")
            return
        
        print(f"Forward index files: {len(forward_files):,}")
        print(f"Batch size: {self.batch_size}")
        print("=" * 80)
        
        start_time = time.time()
        
        # process all forward indexes in batches
        total_batches = (len(forward_files) + self.batch_size - 1) // self.batch_size
        successful = 0
        
        for i in range(0, len(forward_files), self.batch_size):
            batch = forward_files[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            
            print(f"\n[Batch {batch_num}/{total_batches}] Processing {len(batch)} files...")
            
            for forward_file in batch:
                if self._process_forward_index(forward_file):
                    successful += 1
            
            print(f"  ✓ Batch complete - Total processed: {successful:,}")
            print(f"  ✓ Unique words in index: {len(self.inverted_index):,}")
        
        print(f"\n[All forward indexes processed]")
        print(f"  ✓ Files processed: {successful:,}")
        print(f"  ✓ Unique words indexed: {len(self.inverted_index):,}")
        
        # save to barrels
        self._save_barrels()
        
        elapsed = time.time() - start_time
        
        # update stats
        self.stats['files_processed'] = successful
        self.stats['total_words_indexed'] = len(self.inverted_index)
        self.stats['processing_time'] = elapsed
        
        # save stats
        stats_file = self.output_dir / "backward_indexing_stats.json"
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(self.stats, f, indent=2)
        
        # clear memory
        self.inverted_index.clear()
        
        print("\n" + "=" * 80)
        print("✓ BACKWARD INDEXING COMPLETE!")
        print(f"Files processed: {successful:,}")
        print(f"Barrels created: {self.stats['barrels_created']}")
        print(f"Time: {elapsed:.2f}s ({elapsed/60:.2f} min)")
        print("=" * 80)
    
    def verify_barrels(self, num_samples: int = 3):
        """check random barrels to verify structure"""
        print("\n" + "=" * 80)
        print("VERIFYING BARREL FILES")
        print("=" * 80)
        
        barrel_files = list(self.output_dir.glob("inverted_*.json"))
        if not barrel_files:
            print("⚠ No barrel files found!")
            return
        
        print(f"Total barrels: {len(barrel_files)}")
        print(f"Checking {num_samples} samples...\n")
        
        import random
        samples = random.sample(barrel_files, min(num_samples, len(barrel_files)))
        
        for idx, barrel_file in enumerate(samples, 1):
            print(f"[Sample {idx}] {barrel_file.name}")
            
            with open(barrel_file, 'r', encoding='utf-8') as f:
                barrel_data = json.load(f)
            
            print(f"  Words in barrel: {len(barrel_data)}")
            
            # show first few entries
            for i, (word_id, doc_positions) in enumerate(list(barrel_data.items())[:3], 1):
                word = self.id_to_word.get(int(word_id), "unknown")
                num_docs = len(doc_positions)
                print(f"    {i}. Word '{word}' (ID: {word_id}) → {num_docs} documents")
                
                # show one document's positions
                first_doc = list(doc_positions.keys())[0]
                positions = doc_positions[first_doc][:5]  # first 5 positions
                print(f"       → in '{first_doc}': positions {positions}...")
            
            if len(barrel_data) > 3:
                print(f"    ... and {len(barrel_data) - 3} more words\n")
        
        print("=" * 80)
    
    def get_barrel_stats(self):
        """show distribution statistics across barrels"""
        print("\n" + "=" * 80)
        print("BARREL DISTRIBUTION STATS")
        print("=" * 80)
        
        barrel_files = list(self.output_dir.glob("inverted_*.json"))
        if not barrel_files:
            print("⚠ No barrel files found!")
            return
        
        sizes = []
        word_counts = []
        
        for barrel_file in barrel_files:
            size_kb = barrel_file.stat().st_size / 1024
            sizes.append(size_kb)
            
            with open(barrel_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                word_counts.append(len(data))
        
        print(f"Total barrels: {len(barrel_files)}")
        print(f"Total size: {sum(sizes)/1024:.2f} MB")
        print(f"Avg barrel size: {sum(sizes)/len(sizes):.2f} KB")
        print(f"Min barrel size: {min(sizes):.2f} KB")
        print(f"Max barrel size: {max(sizes):.2f} KB")
        print(f"\nAvg words per barrel: {sum(word_counts)/len(word_counts):.0f}")
        print(f"Min words per barrel: {min(word_counts)}")
        print(f"Max words per barrel: {max(word_counts)}")
        print("=" * 80)
    
    def run(self):
        """main entry point"""
        print("\n" + "=" * 80)
        print("BACKWARD INDEX BUILDER")
        print("=" * 80)
        print(f"Forward indexes: {self.forward_index_dir}")
        print(f"Lexicon: {self.lexicon_path}")
        print(f"Output: {self.output_dir}")
        print("=" * 80)
        
        if not self.forward_index_dir.exists():
            print(f"⚠ Error: {self.forward_index_dir} doesn't exist!")
            return
        
        if not self.lexicon_path.exists():
            print(f"⚠ Error: {self.lexicon_path} doesn't exist!")
            return
        
        # build inverted index
        self.build_inverted_index()
        
        # verify results
        self.verify_barrels(num_samples=3)
        
        # show stats
        self.get_barrel_stats()
        
        print("\n" + "=" * 80)
        print("✓ ALL DONE!")
        print("=" * 80 + "\n")


def main():
    """example usage"""
    FORWARD_INDEX_DIR = r"D:\Coding\DSA2\res\forward_indexing"
    LEXICON_PATH = r"D:\Coding\DSA2\res\lexicon\lexicon.json"
    OUTPUT_DIR = r"D:\Coding\DSA2\res\backward_indexing"
    BATCH_SIZE = 100
    
    builder = BackwardIndexBuilder(FORWARD_INDEX_DIR, LEXICON_PATH, OUTPUT_DIR, BATCH_SIZE)
    builder.run()


if __name__ == "__main__":
    main()