"""
File 6: 6_barrel_builder.py
CORD-19 Barrel Building System
Creates barrels of 25,000 terms each with backward indexing structure
Organizes terms into manageable chunks for efficient retrieval
"""

import json
import os
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class BarrelBuilder:
    """Build barrels containing 25,000 terms each with backward index structure."""
    
    BARREL_SIZE = 25000
    
    def __init__(self, base_path: str, lexicon_path: str, backward_index_dir: str, output_dir: str):
        self.base_path = Path(base_path)
        self.backward_index_dir = Path(backward_index_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load lexicon
        self.lexicon = self.load_lexicon(lexicon_path)
        logger.info(f"✓ Loaded lexicon with {len(self.lexicon):,} terms")
        
        # Sort terms by frequency (most frequent first)
        self.sorted_terms = sorted(
            self.lexicon.items(),
            key=lambda x: x[1]['frequency'],
            reverse=True
        )
        
        # Pre-load all backward index data into memory for speed
        self.bucket_cache = {}
        self.load_all_buckets_into_memory()
        
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
        """Get the 3-letter prefix bucket for a term."""
        term_lower = term.lower()
        
        if not term_lower or not term_lower[0].isalpha():
            return '000'
        
        prefix = term_lower[:3].ljust(3, '_')
        return prefix
    
    def load_all_buckets_into_memory(self):
        """Load all backward index buckets into memory at once for fast access."""
        logger.info("Loading all backward index buckets into memory...")
        self.bucket_cache = {}
        
        bucket_files = list(self.backward_index_dir.glob('*.json'))
        bucket_files = [f for f in bucket_files if 'manifest' not in f.name.lower()]
        
        logger.info(f"Found {len(bucket_files)} bucket files to load")
        
        for idx, bucket_file in enumerate(bucket_files, 1):
            if idx % 100 == 0:
                logger.info(f"Loading buckets: {idx}/{len(bucket_files)}")
            
            try:
                with open(bucket_file, 'r', encoding='utf-8') as f:
                    bucket_data = json.load(f)
                
                prefix = bucket_data.get('prefix', bucket_file.stem)
                terms_dict = bucket_data.get('terms', {})
                
                # Store all terms from this bucket
                for term, term_data in terms_dict.items():
                    self.bucket_cache[term] = term_data
                    
            except Exception as e:
                logger.error(f"✗ Error loading bucket {bucket_file.name}: {e}")
        
        logger.info(f"✓ Loaded {len(self.bucket_cache):,} terms into memory\n")
    
    def load_term_from_backward_index(self, term: str):
        """Load term data from pre-cached backward index."""
        return self.bucket_cache.get(term, None)
    
    def create_barrel(self, barrel_num: int, start_idx: int, end_idx: int):
        """Create a single barrel containing terms from start_idx to end_idx."""
        logger.info(f"\n{'='*80}")
        logger.info(f"Building Barrel {barrel_num}")
        logger.info(f"Term range: {start_idx+1:,} to {min(end_idx, len(self.sorted_terms)):,}")
        logger.info(f"{'='*80}\n")
        
        # Get terms for this barrel
        barrel_terms = self.sorted_terms[start_idx:end_idx]
        
        barrel_data = {
            'barrel_number': barrel_num,
            'barrel_name': f"{start_idx+1}-{min(end_idx, len(self.sorted_terms))}",
            'term_range': {
                'start': start_idx + 1,
                'end': min(end_idx, len(self.sorted_terms)),
                'count': len(barrel_terms)
            },
            'created_at': datetime.now().isoformat(),
            'terms': {}
        }
        
        terms_loaded = 0
        terms_failed = 0
        total_doc_mappings = 0
        
        for idx, (term, term_info) in enumerate(barrel_terms, 1):
            if idx % 5000 == 0:
                logger.info(f"Progress: {idx:,}/{len(barrel_terms):,} terms | "
                          f"Loaded: {terms_loaded:,} | Failed: {terms_failed:,}")
            
            # Load term data from backward index (now instant from cache)
            term_data = self.load_term_from_backward_index(term)
            
            if term_data:
                barrel_data['terms'][term] = {
                    'term_id': term_info['term_id'],
                    'frequency': term_info['frequency'],
                    'document_count': len(term_data),
                    'documents': term_data
                }
                terms_loaded += 1
                total_doc_mappings += len(term_data)
            else:
                # If no backward index data, still include basic info
                barrel_data['terms'][term] = {
                    'term_id': term_info['term_id'],
                    'frequency': term_info['frequency'],
                    'document_count': 0,
                    'documents': {}
                }
                terms_failed += 1
        
        # Add statistics
        barrel_data['statistics'] = {
            'total_terms': len(barrel_terms),
            'terms_with_documents': terms_loaded,
            'terms_without_documents': terms_failed,
            'total_document_mappings': total_doc_mappings,
            'avg_documents_per_term': total_doc_mappings / terms_loaded if terms_loaded > 0 else 0
        }
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Barrel {barrel_num} Statistics")
        logger.info(f"{'='*80}")
        logger.info(f"✓ Total terms: {len(barrel_terms):,}")
        logger.info(f"✓ Terms with documents: {terms_loaded:,}")
        logger.info(f"✗ Terms without documents: {terms_failed:,}")
        logger.info(f"✓ Total document mappings: {total_doc_mappings:,}")
        logger.info(f"{'='*80}\n")
        
        return barrel_data
    
    def save_barrel(self, barrel_data: dict):
        """Save barrel to JSON file."""
        barrel_name = barrel_data['barrel_name']
        barrel_file = self.output_dir / f"{barrel_name}.json"
        
        try:
            with open(barrel_file, 'w', encoding='utf-8') as f:
                json.dump(barrel_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✓ Saved barrel to: {barrel_file}")
            return str(barrel_file)
            
        except Exception as e:
            logger.error(f"✗ Error saving barrel {barrel_name}: {e}")
            raise
    
    def build_all_barrels(self):
        """Build all barrels from the lexicon."""
        logger.info(f"\n{'='*80}")
        logger.info("BARREL BUILDING SYSTEM")
        logger.info(f"{'='*80}")
        logger.info(f"Total terms in lexicon: {len(self.sorted_terms):,}")
        logger.info(f"Barrel size: {self.BARREL_SIZE:,} terms")
        logger.info(f"Expected barrels: {(len(self.sorted_terms) + self.BARREL_SIZE - 1) // self.BARREL_SIZE}")
        logger.info(f"{'='*80}\n")
        
        barrels_created = []
        barrel_num = 1
        
        for start_idx in range(0, len(self.sorted_terms), self.BARREL_SIZE):
            end_idx = min(start_idx + self.BARREL_SIZE, len(self.sorted_terms))
            
            # Create barrel
            barrel_data = self.create_barrel(barrel_num, start_idx, end_idx)
            
            # Save barrel
            barrel_file = self.save_barrel(barrel_data)
            
            barrels_created.append({
                'barrel_number': barrel_num,
                'barrel_name': barrel_data['barrel_name'],
                'file_path': barrel_file,
                'term_count': barrel_data['statistics']['total_terms'],
                'document_mappings': barrel_data['statistics']['total_document_mappings']
            })
            
            barrel_num += 1
        
        return barrels_created
    
    def create_barrel_manifest(self, barrels_info: list):
        """Create manifest file with barrel information."""
        total_terms = sum(b['term_count'] for b in barrels_info)
        total_mappings = sum(b['document_mappings'] for b in barrels_info)
        
        manifest = {
            'creation_date': datetime.now().isoformat(),
            'dataset': 'CORD-19 2020-05-01',
            'barrel_system': {
                'barrel_size': self.BARREL_SIZE,
                'total_barrels': len(barrels_info),
                'total_terms': total_terms,
                'total_document_mappings': total_mappings
            },
            'organization': {
                'structure': 'Terms sorted by frequency (descending)',
                'barrel_naming': 'start_term_id-end_term_id.json',
                'example': '1-25000.json, 25001-50000.json, etc.'
            },
            'barrels': barrels_info,
            'output_directory': str(self.output_dir),
            'usage': {
                'search_strategy': 'Search barrels sequentially or use term_id to locate barrel',
                'term_lookup': 'Each barrel contains complete backward index for its terms',
                'note': 'Most frequent terms are in early barrels (1-25000, 25001-50000, etc.)'
            }
        }
        
        manifest_path = self.output_dir / "barrel_manifest.json"
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2)
        
        logger.info(f"\n✓ Barrel manifest saved to: {manifest_path}")
        return manifest_path
    
    def display_barrel_summary(self, barrels_info: list):
        """Display summary of created barrels."""
        logger.info(f"\n{'='*80}")
        logger.info("BARREL SUMMARY")
        logger.info(f"{'='*80}\n")
        
        for barrel in barrels_info:
            logger.info(f"Barrel {barrel['barrel_number']:2d}: {barrel['barrel_name']:20s} | "
                       f"Terms: {barrel['term_count']:>6,} | "
                       f"Doc Mappings: {barrel['document_mappings']:>10,}")
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Total Barrels: {len(barrels_info)}")
        logger.info(f"Total Terms: {sum(b['term_count'] for b in barrels_info):,}")
        logger.info(f"Total Document Mappings: {sum(b['document_mappings'] for b in barrels_info):,}")
        logger.info(f"{'='*80}\n")


def main():
    """Main execution function."""
    BASE_PATH = r"D:\Hamza\cord-19_2020-05-01"
    LEXICON_PATH = r"D:\Hamza\cord-19_2020-05-01\res\lexicon\cord19_lexicon.json"
    BACKWARD_INDEX_DIR = r"D:\Hamza\cord-19_2020-05-01\res\backward_indexing"
    OUTPUT_DIR = r"D:\Hamza\cord-19_2020-05-01\res\barrels"
    
    print("\n" + "="*80)
    print("CORD-19 BARREL BUILDING SYSTEM")
    print("Creating barrels of 25,000 terms with backward index structure")
    print("="*80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80 + "\n")
    
    try:
        # Initialize builder
        builder = BarrelBuilder(
            BASE_PATH,
            LEXICON_PATH,
            BACKWARD_INDEX_DIR,
            OUTPUT_DIR
        )
        
        # Build all barrels
        barrels_info = builder.build_all_barrels()
        
        # Create manifest
        manifest_path = builder.create_barrel_manifest(barrels_info)
        
        # Display summary
        builder.display_barrel_summary(barrels_info)
        
        # Final output
        print("\n" + "="*80)
        print("BARREL BUILDING COMPLETE")
        print("="*80)
        print(f"\n📊 Overall Statistics:")
        print(f"   • Total barrels created: {len(barrels_info)}")
        print(f"   • Barrel size: 25,000 terms each")
        print(f"   • Total terms indexed: {sum(b['term_count'] for b in barrels_info):,}")
        print(f"   • Total document mappings: {sum(b['document_mappings'] for b in barrels_info):,}")
        
        print(f"\n📂 Output Location:")
        print(f"   • {OUTPUT_DIR}")
        print(f"   • Barrel files: 1-25000.json, 25001-50000.json, etc.")
        print(f"   • Manifest: barrel_manifest.json")
        
        print(f"\n💡 Usage:")
        print(f"   • Terms are sorted by frequency (most frequent in first barrels)")
        print(f"   • Each barrel contains complete backward index for its terms")
        print(f"   • Use term_id or sequential search to locate terms")
        
        print("\n" + "="*80)
        print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80 + "\n")
        
    except Exception as e:
        logger.error(f"✗ Error in barrel building: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()