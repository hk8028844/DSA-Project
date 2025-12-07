"""
File 5: 5_backward_indexer.py
CORD-19 Backward Indexing System
Creates inverted index mapping terms to documents
Organizes by 3-letter prefixes (aaa-zzz) for efficient retrieval
"""

import json
import os
import re
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class BackwardIndexBuilder:
    """Build backward (inverted) index for CORD-19 papers."""
    
    def __init__(self, base_path: str, lexicon_path: str, forward_index_dir: str, output_dir: str):
        self.base_path = Path(base_path)
        self.forward_index_dir = Path(forward_index_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load lexicon
        self.lexicon = self.load_lexicon(lexicon_path)
        self.term_to_id = {term: info['term_id'] for term, info in self.lexicon.items()}
        
        # Initialize inverted index structure: term -> {doc_id: {freq, positions, sections}}
        self.inverted_index = defaultdict(lambda: defaultdict(dict))
        
        logger.info(f"✓ Loaded lexicon with {len(self.lexicon):,} terms")
        
    def load_lexicon(self, lexicon_path: str):
        """Load the lexicon dictionary."""
        try:
            with open(lexicon_path, 'r', encoding='utf-8') as f:
                lexicon = json.load(f)
            return lexicon
        except Exception as e:
            logger.error(f"✗ Error loading lexicon: {e}")
            raise
    
    def get_prefix_bucket(self, term: str) -> str:
        """
        Get the 3-letter prefix bucket for a term.
        Examples: 
          'covid' -> 'cov'
          'ab' -> 'ab_'
          'a' -> 'a__'
        """
        term_lower = term.lower()
        
        # Handle special characters at start
        if not term_lower or not term_lower[0].isalpha():
            return '000'  # Special bucket for non-alphabetic starts
        
        # Get first 3 characters, pad with underscores if needed
        prefix = term_lower[:3].ljust(3, '_')
        
        return prefix
    
    def process_forward_index(self, forward_index_path: Path):
        """
        Process a single forward index file and add its data to inverted index.
        """
        try:
            with open(forward_index_path, 'r', encoding='utf-8') as f:
                forward_data = json.load(f)
        except Exception as e:
            logger.warning(f"⚠ Could not read {forward_index_path.name}: {e}")
            return False
        
        doc_id = forward_data.get('document_id', forward_index_path.stem)
        source_type = forward_data.get('source_type', 'unknown')
        term_frequencies = forward_data.get('term_frequencies', {})
        sections = forward_data.get('sections', {})
        
        # Process each term in this document
        for term, term_data in term_frequencies.items():
            freq = term_data.get('frequency', 0)
            positions = term_data.get('positions', [])
            term_id = term_data.get('term_id', -1)
            
            # Store document information for this term
            self.inverted_index[term][doc_id] = {
                'frequency': freq,
                'positions': positions[:100],  # Limit positions
                'source_type': source_type,
                'term_id': term_id
            }
        
        return True
    
    def process_all_forward_indexes(self):
        """Process all forward index files from pdf_json and pmc_json."""
        logger.info(f"\n{'='*80}")
        logger.info("Processing Forward Indexes")
        logger.info(f"{'='*80}\n")
        
        # Find all forward index files
        pdf_indexes = list((self.forward_index_dir / 'pdf_json').rglob('*.json'))
        pmc_indexes = list((self.forward_index_dir / 'pmc_json').rglob('*.json'))
        
        all_indexes = pdf_indexes + pmc_indexes
        total_files = len(all_indexes)
        
        logger.info(f"Found {len(pdf_indexes):,} PDF forward indexes")
        logger.info(f"Found {len(pmc_indexes):,} PMC forward indexes")
        logger.info(f"Total: {total_files:,} forward index files\n")
        
        processed = 0
        failed = 0
        
        for idx, index_path in enumerate(all_indexes, 1):
            # Progress update
            if idx % 100 == 0:
                logger.info(f"Progress: {idx:,}/{total_files:,} files | "
                          f"Processed: {processed:,} | Failed: {failed:,} | "
                          f"Unique terms: {len(self.inverted_index):,}")
            
            if self.process_forward_index(index_path):
                processed += 1
            else:
                failed += 1
        
        logger.info(f"\n{'='*80}")
        logger.info("Forward Index Processing Complete")
        logger.info(f"{'='*80}")
        logger.info(f"✓ Processed: {processed:,}/{total_files:,}")
        logger.info(f"✗ Failed: {failed:,}")
        logger.info(f"✓ Unique terms in inverted index: {len(self.inverted_index):,}")
        logger.info(f"{'='*80}\n")
        
        return processed, failed
    
    def save_by_prefix_buckets(self):
        """
        Save inverted index organized by 3-letter prefix buckets.
        Each bucket file contains all terms starting with that prefix.
        """
        logger.info(f"\n{'='*80}")
        logger.info("Saving Backward Index by Prefix Buckets")
        logger.info(f"{'='*80}\n")
        
        # Group terms by prefix
        prefix_groups = defaultdict(dict)
        
        for term, doc_dict in self.inverted_index.items():
            prefix = self.get_prefix_bucket(term)
            prefix_groups[prefix][term] = dict(doc_dict)
        
        logger.info(f"Terms grouped into {len(prefix_groups):,} prefix buckets")
        
        # Save each prefix bucket
        saved_count = 0
        total_buckets = len(prefix_groups)
        
        for idx, (prefix, terms_dict) in enumerate(sorted(prefix_groups.items()), 1):
            # Create bucket file
            bucket_data = {
                'prefix': prefix,
                'term_count': len(terms_dict),
                'total_documents': sum(len(docs) for docs in terms_dict.values()),
                'terms': terms_dict,
                'created_at': datetime.now().isoformat()
            }
            
            # Save to file
            bucket_file = self.output_dir / f"{prefix}.json"
            
            try:
                with open(bucket_file, 'w', encoding='utf-8') as f:
                    json.dump(bucket_data, f, indent=2, ensure_ascii=False)
                saved_count += 1
                
                if idx % 50 == 0:
                    logger.info(f"Progress: {idx:,}/{total_buckets:,} buckets saved")
                    
            except Exception as e:
                logger.error(f"✗ Error saving bucket {prefix}: {e}")
        
        logger.info(f"\n{'='*80}")
        logger.info("Bucket Saving Complete")
        logger.info(f"{'='*80}")
        logger.info(f"✓ Saved {saved_count:,} prefix bucket files")
        logger.info(f"✓ Output directory: {self.output_dir}")
        logger.info(f"{'='*80}\n")
        
        return saved_count
    
    def create_index_manifest(self, stats: dict):
        """Create manifest file with index statistics and structure."""
        manifest = {
            'creation_date': datetime.now().isoformat(),
            'dataset': 'CORD-19 2020-05-01',
            'index_type': 'backward_inverted',
            'organization': '3-letter prefix buckets (aaa-zzz)',
            'total_unique_terms': len(self.inverted_index),
            'total_prefix_buckets': stats.get('buckets_saved', 0),
            'statistics': stats,
            'output_directory': str(self.output_dir),
            'bucket_naming': {
                'pattern': '[prefix].json',
                'examples': ['aaa.json', 'cov.json', 'zzz.json'],
                'special': '000.json for non-alphabetic terms'
            },
            'usage': {
                'search_term': 'Find the 3-letter prefix, load corresponding bucket file',
                'example': 'To find "covid", load "cov.json" bucket'
            }
        }
        
        manifest_path = self.output_dir / "backward_index_manifest.json"
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2)
        
        logger.info(f"✓ Manifest saved to: {manifest_path}")
        
        return manifest_path
    
    def display_sample_buckets(self, n=10):
        """Display sample bucket information."""
        bucket_files = sorted(self.output_dir.glob('*.json'))
        
        if not bucket_files:
            return
        
        # Exclude manifest
        bucket_files = [f for f in bucket_files if 'manifest' not in f.name.lower()]
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Sample Buckets (showing first {n}):")
        logger.info(f"{'='*80}")
        
        for bucket_file in bucket_files[:n]:
            try:
                with open(bucket_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                prefix = data.get('prefix', 'unknown')
                term_count = data.get('term_count', 0)
                doc_count = data.get('total_documents', 0)
                
                # Get sample terms
                terms = list(data.get('terms', {}).keys())[:5]
                sample_terms = ', '.join(terms[:3])
                if len(terms) > 3:
                    sample_terms += '...'
                
                logger.info(f"  {prefix}.json: {term_count:,} terms, {doc_count:,} doc mappings")
                logger.info(f"    Sample terms: {sample_terms}")
                
            except Exception as e:
                logger.warning(f"  Could not read {bucket_file.name}: {e}")
        
        logger.info(f"{'='*80}\n")


def main():
    """Main execution function."""
    BASE_PATH = r"D:\Hamza\cord-19_2020-05-01"
    LEXICON_PATH = r"D:\Hamza\cord-19_2020-05-01\res\lexicon\cord19_lexicon.json"
    FORWARD_INDEX_DIR = r"D:\Hamza\cord-19_2020-05-01\res\forward_indexing"
    OUTPUT_DIR = r"D:\Hamza\cord-19_2020-05-01\res\backward_indexing"
    
    print("\n" + "="*80)
    print("CORD-19 BACKWARD (INVERTED) INDEXING SYSTEM")
    print("Creating term-to-document mappings in prefix buckets (aaa-zzz)")
    print("="*80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80 + "\n")
    
    try:
        # Initialize builder
        builder = BackwardIndexBuilder(
            BASE_PATH, 
            LEXICON_PATH, 
            FORWARD_INDEX_DIR, 
            OUTPUT_DIR
        )
        
        # Process all forward indexes
        processed, failed = builder.process_all_forward_indexes()
        
        # Save by prefix buckets
        buckets_saved = builder.save_by_prefix_buckets()
        
        # Create manifest
        stats = {
            'forward_indexes_processed': processed,
            'forward_indexes_failed': failed,
            'total_unique_terms': len(builder.inverted_index),
            'buckets_saved': buckets_saved,
            'total_term_document_mappings': sum(
                len(docs) for docs in builder.inverted_index.values()
            )
        }
        
        manifest_path = builder.create_index_manifest(stats)
        
        # Display samples
        builder.display_sample_buckets(10)
        
        # Final statistics
        print("\n" + "="*80)
        print("BACKWARD INDEXING COMPLETE")
        print("="*80)
        print(f"\n📊 Overall Statistics:")
        print(f"   • Forward indexes processed: {processed:,}")
        print(f"   • Forward indexes failed: {failed:,}")
        print(f"   • Total unique terms: {len(builder.inverted_index):,}")
        print(f"   • Prefix buckets created: {buckets_saved:,}")
        print(f"   • Total term-document mappings: {stats['total_term_document_mappings']:,}")
        
        print(f"\n📂 Output Location:")
        print(f"   • {OUTPUT_DIR}")
        print(f"   • Bucket files: [prefix].json (e.g., cov.json, vir.json)")
        print(f"   • Manifest: backward_index_manifest.json")
        
        print(f"\n💡 Usage:")
        print(f"   • To search for 'covid': load cov.json")
        print(f"   • To search for 'virus': load vir.json")
        print(f"   • Each bucket contains all terms with that 3-letter prefix")
        
        print("\n" + "="*80)
        print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80 + "\n")
        
    except Exception as e:
        logger.error(f"✗ Error in backward indexing: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()