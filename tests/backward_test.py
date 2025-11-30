import json
import random
from pathlib import Path
from typing import List

class BackwardIndexTester:
    """Quick tester for backward/inverted index JSON files"""
    
    def __init__(self, backward_index_dir: str):
        self.index_dir = Path(backward_index_dir)
        self.passed = 0
        self.failed = 0
    
    def test(self, name: str, condition: bool, details: str = ""):
        """Log test result"""
        status = "✓" if condition else "✗"
        print(f"{status} {name}{': ' + details if details else ''}")
        if condition:
            self.passed += 1
        else:
            self.failed += 1
    
    def _get_inverted_files(self) -> List[Path]:
        """Get all inverted_* JSON files"""
        return list(self.index_dir.glob("inverted_*.json"))
    
    def _load_json(self, filepath: Path) -> dict:
        """Safely load a JSON file"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            raise Exception(f"Failed to load {filepath.name}: {e}")
    
    def test_random_files(self, sample_size: int = 10):
        """Test random inverted index files"""
        print("\n" + "="*60)
        print("BACKWARD INDEX (INVERTED) TESTS")
        print("="*60 + "\n")
        
        # Get all inverted files
        inv_files = self._get_inverted_files()
        
        if not inv_files:
            self.test("Files Found", False, "No inverted_*.json files found")
            return
        
        self.test("Files Found", True, f"{len(inv_files):,} files")
        
        # Sample random files
        sample_count = min(sample_size, len(inv_files))
        samples = random.sample(inv_files, sample_count)
        
        print(f"\nTesting {sample_count} random files...\n")
        
        for i, filepath in enumerate(samples, 1):
            print(f"[File {i}/{sample_count}] {filepath.name}")
            
            # Test 1: Can load JSON
            try:
                data = self._load_json(filepath)
                self.test(f"  Load JSON", True)
            except Exception as e:
                self.test(f"  Load JSON", False, str(e))
                continue
            
            # Test 2: Has expected structure (dict)
            is_dict = isinstance(data, dict)
            self.test(f"  Structure (dict)", is_dict)
            
            if not is_dict:
                continue
            
            # Test 3: Not empty
            not_empty = len(data) > 0
            self.test(f"  Not Empty", not_empty, f"{len(data)} entries")
            
            # Test 4: Keys are word IDs (numeric strings)
            if data:
                sample_keys = list(data.keys())[:3]
                valid_keys = all(k.isdigit() for k in sample_keys)
                self.test(f"  Valid Keys (word IDs)", valid_keys, 
                         f"e.g. {sample_keys[0]}")
                
                # Test 5: Values are valid (could be lists or dicts)
                sample_vals = [data[k] for k in sample_keys]
                first_val = sample_vals[0] if sample_vals else None
                
                if isinstance(first_val, list):
                    valid_vals = all(isinstance(v, list) for v in sample_vals)
                    self.test(f"  Valid Values (lists)", valid_vals, 
                             f"e.g. {len(first_val)} doc IDs")
                elif isinstance(first_val, dict):
                    valid_vals = all(isinstance(v, dict) for v in sample_vals)
                    self.test(f"  Valid Values (dicts)", valid_vals,
                             f"e.g. {len(first_val)} entries")
                elif isinstance(first_val, int):
                    valid_vals = all(isinstance(v, int) for v in sample_vals)
                    self.test(f"  Valid Values (counts)", valid_vals,
                             f"e.g. count={first_val}")
                else:
                    self.test(f"  Valid Values", False, 
                             f"unexpected type: {type(first_val)}")
            
            print()
        
        # Summary
        print("="*60)
        total = self.passed + self.failed
        print(f"Results: {self.passed}/{total} passed")
        
        if self.failed == 0:
            print("✓ ALL TESTS PASSED!")
        else:
            print(f"⚠ {self.failed} test(s) failed")
        print("="*60 + "\n")
    
    def test_stats_file(self):
        """Test the stats file if exists"""
        stats_file = self.index_dir / "backward_indexing_stats.json"
        
        print("\n" + "="*60)
        print("STATS FILE TEST")
        print("="*60 + "\n")
        
        if not stats_file.exists():
            self.test("Stats File Exists", False, "Not found")
            return
        
        self.test("Stats File Exists", True)
        
        try:
            with open(stats_file, 'r') as f:
                stats = json.load(f)
            
            self.test("Load Stats JSON", True)
            
            # Show all available fields
            print("\nStats fields found:")
            for key, value in stats.items():
                print(f"  {key}: {value}")
            
            # Check if it has data
            has_data = len(stats) > 0
            self.test("Has Stats Data", has_data, f"{len(stats)} fields")
        
        except Exception as e:
            self.test("Load Stats JSON", False, str(e))
        
        print("\n" + "="*60 + "\n")


def main():
    """Run backward index tests"""
    BACKWARD_INDEX_DIR = r"D:\Coding\DSA2\res\backward_indexing"
    
    tester = BackwardIndexTester(BACKWARD_INDEX_DIR)
    
    # Test random inverted index files
    tester.test_random_files(sample_size=10)
    
    # Test stats file
    tester.test_stats_file()
    
    # Final summary
    print("="*60)
    print(f"TOTAL: {tester.passed}/{tester.passed + tester.failed} tests passed")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()