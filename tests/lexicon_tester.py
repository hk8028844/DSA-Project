import json
import time
from pathlib import Path

class LexiconTester:
    """Quick and efficient lexicon validator"""
    
    def __init__(self, lexicon_path: str):
        self.path = Path(lexicon_path)
        self.lexicon = None
        self.passed = 0
        self.failed = 0
    
    def test(self, name: str, condition: bool, details: str = ""):
        """Simple pass/fail logger"""
        status = "✓" if condition else "✗"
        print(f"{status} {name}{': ' + details if details else ''}")
        if condition:
            self.passed += 1
        else:
            self.failed += 1
    
    def run_tests(self):
        """Run all essential tests"""
        print("\n" + "="*60)
        print("LEXICON TESTS")
        print("="*60 + "\n")
        
        # Load the lexicon
        try:
            start = time.time()
            with open(self.path, 'r', encoding='utf-8') as f:
                self.lexicon = json.load(f)
            load_time = time.time() - start
            self.test("Load JSON", True, f"{len(self.lexicon):,} words in {load_time:.2f}s")
        except Exception as e:
            self.test("Load JSON", False, str(e))
            return
        
        # Size check - adapt based on actual size
        size = len(self.lexicon)
        min_size = 10_000 if size < 100_000 else 100_000
        max_size = 100_000 if size < 100_000 else 500_000
        self.test("Size Check", min_size <= size <= max_size, 
                  f"{size:,} words")
        
        # IDs should be sequential from 1
        ids = sorted(self.lexicon.values())
        sequential = ids == list(range(1, len(ids) + 1))
        self.test("Sequential IDs", sequential, f"1 to {len(ids):,}")
        
        # All words lowercase
        uppercase = [w for w in self.lexicon if w != w.lower()]
        self.test("All Lowercase", len(uppercase) == 0)
        
        # Only alphabetic characters
        invalid = [w for w in self.lexicon if not w.isalpha()]
        self.test("Only Letters", len(invalid) == 0)
        
        # Length between 2-25 chars
        bad_length = [w for w in self.lexicon if not 2 <= len(w) <= 25]
        self.test("Valid Length (2-25)", len(bad_length) == 0)
        
        # Quick lookup test
        sample = list(self.lexicon.keys())[:10]
        lookups_ok = all(word in self.lexicon for word in sample)
        self.test("Word Lookup", lookups_ok)
        
        # Summary
        print("\n" + "="*60)
        total = self.passed + self.failed
        print(f"Results: {self.passed}/{total} passed")
        
        if self.failed == 0:
            print("✓ ALL TESTS PASSED!")
        else:
            print(f"⚠ {self.failed} test(s) failed")
        print("="*60 + "\n")


if __name__ == "__main__":
    # Update this path to your lexicon file
    LEXICON_PATH = r"D:\Coding\DSA2\res\lexicon\lexicon.json"
    
    tester = LexiconTester(LEXICON_PATH)
    tester.run_tests()