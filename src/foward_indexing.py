import os
import re
import json
import time
import random
from pathlib import Path
from typing import Dict, List, Tuple
import multiprocessing as mp

class ForwardIndexBuilder:
    """builds forward indexes from cleaned text files using lexicon"""
    
    def __init__(self, cleaned_data_dir: str, lexicon_path: str, output_dir: str, batch_size: int = 100):
        self.cleaned_data_dir = Path(cleaned_data_dir)
        self.lexicon_path = Path(lexicon_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.batch_size = batch_size
        
        # load lexicon once
        print("Loading lexicon...")
        with open(self.lexicon_path, 'r', encoding='utf-8') as f:
            self.lexicon = json.load(f)
        print(f"✓ Loaded {len(self.lexicon):,} words")
        
        # match words: 2-30 letters only
        self.word_pattern = re.compile(r'\b[a-zA-Z]{2,30}\b')
        
        # stats
        self.stats = {
            'files_processed': 0,
            'total_word_positions': 0,
            'words_not_in_lexicon': 0,
            'processing_time': 0
        }
    
    def _process_single_file(self, txt_file: Path) -> Tuple[str, bool]:
        """process one file, save immediately, then clear from memory"""
        try:
            # read cleaned text
            with open(txt_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # extract all words
            words = self.word_pattern.findall(content)
            
            # build forward index: [word_id, position] pairs
            forward_index = []
            words_skipped = 0
            
            for position, word in enumerate(words):
                word_lower = word.lower()
                
                # look up word in lexicon
                if word_lower in self.lexicon:
                    word_id = self.lexicon[word_lower]
                    forward_index.append([word_id, position])
                else:
                    words_skipped += 1
            
            # create output structure
            doc_name = txt_file.stem  # filename without extension
            output_data = {doc_name: forward_index}
            
            # save immediately to disk with compact format
            output_file = self.output_dir / f"{doc_name}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                # write custom format: each [word_id, position] on one line
                f.write('{\n')
                f.write(f'  "{doc_name}": [\n')
                for i, (word_id, pos) in enumerate(forward_index):
                    comma = ',' if i < len(forward_index) - 1 else ''
                    f.write(f'    [{word_id}, {pos}]{comma}\n')
                f.write('  ]\n')
                f.write('}')
            
            # clear variables to free memory immediately
            del content, words, forward_index, output_data
            
            return doc_name, True
            
        except Exception as e:
            print(f"⚠ Error processing {txt_file.name}: {e}")
            return txt_file.name, False
    
    def _get_txt_files(self) -> List[Path]:
        """get all text files to process"""
        exclude = {'vocabulary.txt', 'processing_progress.json'}
        return [f for f in self.cleaned_data_dir.glob("*.txt") 
                if f.name not in exclude]
    
    def build_indexes(self):
        """build forward indexes for all files"""
        print("\n" + "=" * 80)
        print("BUILDING FORWARD INDEXES")
        print("=" * 80)
        
        txt_files = self._get_txt_files()
        if not txt_files:
            print("⚠ No text files found!")
            return
        
        print(f"Files to process: {len(txt_files):,}")
        print(f"Batch size: {self.batch_size}")
        print(f"Output dir: {self.output_dir}")
        print("=" * 80)
        
        start_time = time.time()
        
        # process in batches
        total_batches = (len(txt_files) + self.batch_size - 1) // self.batch_size
        successful = 0
        failed = 0
        
        for i in range(0, len(txt_files), self.batch_size):
            batch = txt_files[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            
            print(f"\n[Batch {batch_num}/{total_batches}] Processing {len(batch)} files...")
            
            # process files in batch
            for txt_file in batch:
                doc_name, success = self._process_single_file(txt_file)
                if success:
                    successful += 1
                else:
                    failed += 1
            
            print(f"  ✓ Batch complete - Success: {successful}, Failed: {failed}")
        
        elapsed = time.time() - start_time
        
        # update stats
        self.stats['files_processed'] = successful
        self.stats['processing_time'] = elapsed
        
        # save stats
        stats_file = self.output_dir / "indexing_stats.json"
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(self.stats, f, indent=2)
        
        print("\n" + "=" * 80)
        print("✓ FORWARD INDEXING COMPLETE!")
        print(f"Files processed: {successful:,}")
        print(f"Failed: {failed:,}")
        print(f"Time: {elapsed:.2f}s ({elapsed/60:.2f} min)")
        print(f"Avg: {elapsed/successful:.3f}s per file")
        print("=" * 80)
    
    def test_loading_speed(self, num_samples: int = 10):
        """test loading speed of random forward index files"""
        print("\n" + "=" * 80)
        print("TESTING LOADING SPEED")
        print("=" * 80)
        
        # get all index files
        index_files = list(self.output_dir.glob("*.json"))
        if not index_files:
            print("⚠ No index files found!")
            return
        
        # exclude stats file
        index_files = [f for f in index_files if f.name != "indexing_stats.json"]
        
        if len(index_files) < num_samples:
            num_samples = len(index_files)
        
        print(f"Total index files: {len(index_files):,}")
        print(f"Testing with: {num_samples} random samples")
        print("-" * 80)
        
        # pick random files
        sample_files = random.sample(index_files, num_samples)
        
        load_times = []
        sizes = []
        
        for idx, file_path in enumerate(sample_files, 1):
            # measure file size
            file_size = file_path.stat().st_size / 1024  # KB
            sizes.append(file_size)
            
            # measure load time
            start = time.time()
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            elapsed = time.time() - start
            load_times.append(elapsed)
            
            # get some stats about the index
            doc_name = list(data.keys())[0]
            num_positions = len(data[doc_name])
            
            print(f"{idx:2d}. {file_path.name}")
            print(f"    Size: {file_size:.2f} KB | Positions: {num_positions:,} | Load time: {elapsed*1000:.2f}ms")
        
        # calculate averages
        avg_load = sum(load_times) / len(load_times)
        avg_size = sum(sizes) / len(sizes)
        min_load = min(load_times)
        max_load = max(load_times)
        
        print("-" * 80)
        print("SUMMARY:")
        print(f"  Avg load time: {avg_load*1000:.2f}ms")
        print(f"  Min load time: {min_load*1000:.2f}ms")
        print(f"  Max load time: {max_load*1000:.2f}ms")
        print(f"  Avg file size: {avg_size:.2f} KB")
        print("=" * 80)
    
    def verify_random_samples(self, num_samples: int = 3):
        """verify some random indexes look correct"""
        print("\n" + "=" * 80)
        print("VERIFYING RANDOM SAMPLES")
        print("=" * 80)
        
        index_files = list(self.output_dir.glob("*.json"))
        index_files = [f for f in index_files if f.name != "indexing_stats.json"]
        
        if not index_files:
            print("⚠ No index files found!")
            return
        
        if len(index_files) < num_samples:
            num_samples = len(index_files)
        
        samples = random.sample(index_files, num_samples)
        
        for idx, file_path in enumerate(samples, 1):
            print(f"\n[Sample {idx}] {file_path.name}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            doc_name = list(data.keys())[0]
            forward_index = data[doc_name]
            
            print(f"  Document: {doc_name}")
            print(f"  Total positions: {len(forward_index):,}")
            
            # show first 10 entries
            print(f"  First 10 entries:")
            for i, (word_id, position) in enumerate(forward_index[:10], 1):
                print(f"    {i:2d}. [word_id: {word_id:6d}, position: {position:4d}]")
            
            if len(forward_index) > 10:
                print(f"    ... and {len(forward_index) - 10:,} more")
        
        print("\n" + "=" * 80)
    
    def run(self):
        """main entry point"""
        print("\n" + "=" * 80)
        print("FORWARD INDEX BUILDER")
        print("=" * 80)
        print(f"Cleaned data: {self.cleaned_data_dir}")
        print(f"Lexicon: {self.lexicon_path}")
        print(f"Output: {self.output_dir}")
        print("=" * 80)
        
        if not self.cleaned_data_dir.exists():
            print(f"⚠ Error: {self.cleaned_data_dir} doesn't exist!")
            return
        
        if not self.lexicon_path.exists():
            print(f"⚠ Error: {self.lexicon_path} doesn't exist!")
            return
        
        # build indexes
        self.build_indexes()
        
        # test loading speed
        self.test_loading_speed(num_samples=10)
        
        # verify some samples
        self.verify_random_samples(num_samples=3)
        
        print("\n" + "=" * 80)
        print("✓ ALL DONE!")
        print("=" * 80 + "\n")


def main():
    """example usage"""
    CLEANED_DATA_DIR = r"D:\Coding\DSA\data_cleaned"
    LEXICON_PATH = r"D:\Coding\DSA2\res\lexicon\lexicon.json"
    OUTPUT_DIR = r"D:\Coding\DSA2\res\forward_indexing"
    BATCH_SIZE = 100
    
    builder = ForwardIndexBuilder(CLEANED_DATA_DIR, LEXICON_PATH, OUTPUT_DIR, BATCH_SIZE)
    builder.run()


if __name__ == "__main__":
    main()