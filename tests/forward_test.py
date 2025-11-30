import json
import random
from pathlib import Path
from typing import List

class ForwardIndexTester:
    """Quick tester for forward index JSON files"""
    
    def __init__(self, forward_index_dir: str):
        self.index_dir = Path(forward_index_dir)
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
    
    def _get_json_files(self) -> List[Path]:
        """Get all JSON files in forward index directory"""
        return list(self.index_dir.glob("*.json"))
    
    def _load_json(self, filepath: Path) -> dict:
        """Safely load a JSON file"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            raise Exception(f"Failed to load {filepath.name}: {e}")
    
    def test_random_files(self, sample_size: int = 10):
        """Test random JSON files from forward index"""
        print("\n" + "="*60)
        print("FORWARD INDEX JSON TESTS")
        print("="*60 + "\n")
        
        # Get all JSON files
        json_files = self._get_json_files()
        
        if not json_files:
            self.test("Files Found", False, "No JSON files in directory")
            return
        
        self.test("Files Found", True, f"{len(json_files):,} files")
        
        # Sample random files
        sample_count = min(sample_size, len(json_files))
        samples = random.sample(json_files, sample_count)
        
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
            
            # Test 4: Keys are document IDs (strings)
            if data:
                sample_keys = list(data.keys())[:3]
                valid_keys = all(isinstance(k, str) and len(k) > 0 for k in sample_keys)
                self.test(f"  Valid Keys (doc IDs)", valid_keys, f"e.g. {sample_keys[0]}")
                
                # Test 5: Values are lists of word IDs
                sample_vals = [data[k] for k in sample_keys]
                valid_vals = all(isinstance(v, list) for v in sample_vals)
                self.test(f"  Valid Values (lists)", valid_vals, 
                         f"e.g. {len(sample_vals[0])} word IDs" if sample_vals[0] else "empty")
            
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
    
    def test_specific_file(self, filename: str):
        """Test a specific JSON file by name"""
        print("\n" + "="*60)
        print(f"TESTING SPECIFIC FILE: {filename}")
        print("="*60 + "\n")
        
        filepath = self.index_dir / filename
        
        # Check if exists
        if not filepath.exists():
            self.test("File Exists", False, f"{filename} not found")
            return
        
        self.test("File Exists", True)
        
        # Load and test
        try:
            data = self._load_json(filepath)
            self.test("Load JSON", True)
            
            self.test("Structure (dict)", isinstance(data, dict))
            self.test("Not Empty", len(data) > 0, f"{len(data):,} entries")
            
            # Show sample entries
            if data:
                print("\nSample entries:")
                for word_id, doc_ids in list(data.items())[:5]:
                    print(f"  Word ID {word_id}: {len(doc_ids)} documents")
        
        except Exception as e:
            self.test("Load JSON", False, str(e))
        
        print("\n" + "="*60)
        print(f"Results: {self.passed}/{self.passed + self.failed} passed")
        print("="*60 + "\n")


def main():
    """Run forward index tests"""
    FORWARD_INDEX_DIR = r"D:\Coding\DSA2\res\forward_indexing"
    
    tester = ForwardIndexTester(FORWARD_INDEX_DIR)
    
    # Test random files
    tester.test_random_files(sample_size=10)
    
    # Optionally test a specific file
    # tester.test_specific_file("forward_index_0.json")


if __name__ == "__main__":
    main()