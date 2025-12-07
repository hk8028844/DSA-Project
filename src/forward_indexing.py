"""
File 4: 4_forward_indexer.py
CORD-19 Forward Indexing System
Creates individual forward index files for each paper in pdf_json and pmc_json
Maps terms to their positions and frequencies within each document
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


class ForwardIndexBuilder:
    """Build forward index for CORD-19 papers."""
    
    def __init__(self, base_path: str, lexicon_path: str, output_dir: str):
        self.base_path = Path(base_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Directories
        self.pdf_json_dir = self.base_path / "2020-05-01" / "pdf_json"
        self.pmc_json_dir = self.base_path / "2020-05-01" / "pmc_json"
        
        # Load lexicon
        self.lexicon = self.load_lexicon(lexicon_path)
        self.term_to_id = {term: info['term_id'] for term, info in self.lexicon.items()}
        
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
    
    def extract_text_with_positions(self, data, section_name=""):
        """
        Extract text from JSON structure with section information.
        Returns list of (text, section) tuples.
        """
        text_sections = []
        
        if isinstance(data, dict):
            # Handle metadata
            if section_name == "" and 'metadata' in data:
                if 'title' in data['metadata']:
                    text_sections.append((str(data['metadata']['title']), 'title'))
            
            # Handle abstract
            if section_name == "" and 'abstract' in data:
                if isinstance(data['abstract'], list):
                    for item in data['abstract']:
                        if isinstance(item, dict) and 'text' in item:
                            text_sections.append((str(item['text']), 'abstract'))
                elif isinstance(data['abstract'], str):
                    text_sections.append((data['abstract'], 'abstract'))
            
            # Handle body text
            if section_name == "" and 'body_text' in data:
                if isinstance(data['body_text'], list):
                    for idx, para in enumerate(data['body_text']):
                        if isinstance(para, dict):
                            if 'text' in para:
                                section = para.get('section', f'body_{idx}')
                                text_sections.append((str(para['text']), str(section)))
            
            # Handle back matter
            if section_name == "" and 'back_matter' in data:
                if isinstance(data['back_matter'], list):
                    for item in data['back_matter']:
                        if isinstance(item, dict) and 'text' in item:
                            text_sections.append((str(item['text']), 'back_matter'))
            
            # Recursive extraction for nested structures
            for key, value in data.items():
                if key not in ['metadata', 'abstract', 'body_text', 'back_matter', 
                              'ref_spans', 'cite_spans', 'eq_spans']:
                    self.extract_text_with_positions(value, section_name or key)
                    
        elif isinstance(data, list):
            for item in data:
                text_sections.extend(self.extract_text_with_positions(item, section_name))
                
        elif isinstance(data, str) and len(data.strip()) > 0:
            if section_name:
                text_sections.append((data, section_name))
        
        return text_sections
    
    def tokenize_and_normalize(self, text):
        """
        Tokenize text and normalize terms.
        Returns list of (term, position) tuples.
        """
        # Extract words including hyphens, numbers, and preserve case
        words = re.findall(r'\b[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9]\b|\b[A-Za-z]\b', text)
        
        tokens = []
        for pos, word in enumerate(words):
            # Normalize: lowercase for lookup
            normalized = word.lower()
            
            # Check if term is in lexicon
            if normalized in self.term_to_id:
                tokens.append((normalized, pos, word))  # (normalized, position, original)
            # Also check original case (for abbreviations)
            elif word in self.term_to_id:
                tokens.append((word, pos, word))
        
        return tokens
    
    def build_forward_index(self, file_path: Path, source_type: str):
        """
        Build forward index for a single document.
        
        Returns:
            Dictionary with term frequencies, positions, and metadata
        """
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                data = json.load(f)
        except Exception as e:
            logger.warning(f"⚠ Could not read {file_path.name}: {e}")
            return None
        
        # Extract text sections
        text_sections = self.extract_text_with_positions(data, "")
        
        if not text_sections:
            logger.warning(f"⚠ No text extracted from {file_path.name}")
            return None
        
        # Build index
        forward_index = {
            'document_id': file_path.stem,
            'source_type': source_type,
            'file_name': file_path.name,
            'total_terms': 0,
            'unique_terms': 0,
            'sections': {},
            'term_frequencies': {},
            'term_positions': {},
            'metadata': {
                'indexed_at': datetime.now().isoformat(),
                'sections_count': len(text_sections)
            }
        }
        
        all_tokens = []
        section_data = defaultdict(list)
        
        # Process each section
        for text, section in text_sections:
            tokens = self.tokenize_and_normalize(text)
            all_tokens.extend(tokens)
            
            # Track section information
            if section not in section_data:
                section_data[section] = {
                    'term_count': 0,
                    'char_length': 0,
                    'terms': defaultdict(int)
                }
            
            section_data[section]['term_count'] += len(tokens)
            section_data[section]['char_length'] += len(text)
            
            for term, pos, original in tokens:
                section_data[section]['terms'][term] += 1
        
        # Calculate statistics
        term_freq = defaultdict(int)
        term_positions = defaultdict(list)
        
        for term, pos, original in all_tokens:
            term_freq[term] += 1
            term_positions[term].append(pos)
        
        forward_index['total_terms'] = len(all_tokens)
        forward_index['unique_terms'] = len(term_freq)
        
        # Store term frequencies with lexicon IDs
        for term, freq in term_freq.items():
            term_id = self.term_to_id.get(term, -1)
            forward_index['term_frequencies'][term] = {
                'term_id': term_id,
                'frequency': freq,
                'positions': term_positions[term][:100]  # Limit to first 100 positions
            }
        
        # Store section information
        for section, info in section_data.items():
            forward_index['sections'][section] = {
                'term_count': info['term_count'],
                'char_length': info['char_length'],
                'top_terms': dict(sorted(info['terms'].items(), 
                                       key=lambda x: x[1], 
                                       reverse=True)[:20])
            }
        
        return forward_index
    
    def process_directory(self, directory: Path, source_type: str):
        """Process all JSON files in a directory."""
        if not directory.exists():
            logger.warning(f"⚠ Directory not found: {directory}")
            return 0, 0
        
        # Create output subdirectory
        output_subdir = self.output_dir / source_type
        output_subdir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Processing {source_type.upper()} files from: {directory}")
        logger.info(f"Output directory: {output_subdir}")
        logger.info(f"{'='*80}\n")
        
        # Find all JSON files
        json_files = list(directory.rglob("*.json"))
        total_files = len(json_files)
        
        logger.info(f"Found {total_files:,} JSON files")
        
        processed = 0
        failed = 0
        
        for idx, json_file in enumerate(json_files, 1):
            # Progress update
            if idx % 100 == 0:
                logger.info(f"Progress: {idx:,}/{total_files:,} files | "
                          f"Processed: {processed:,} | Failed: {failed:,}")
            
            # Build forward index
            forward_index = self.build_forward_index(json_file, source_type)
            
            if forward_index:
                # Save with same name as source file
                output_file = output_subdir / f"{json_file.stem}.json"
                
                try:
                    with open(output_file, 'w', encoding='utf-8') as f:
                        json.dump(forward_index, f, indent=2, ensure_ascii=False)
                    processed += 1
                except Exception as e:
                    logger.error(f"✗ Error saving {output_file.name}: {e}")
                    failed += 1
            else:
                failed += 1
        
        logger.info(f"\n{'='*80}")
        logger.info(f"{source_type.upper()} Processing Complete")
        logger.info(f"{'='*80}")
        logger.info(f"✓ Successfully processed: {processed:,}/{total_files:,}")
        logger.info(f"✗ Failed: {failed:,}")
        logger.info(f"{'='*80}\n")
        
        return processed, failed
    
    def create_index_summary(self, stats: dict):
        """Create summary of indexing process."""
        summary = {
            'creation_date': datetime.now().isoformat(),
            'dataset': 'CORD-19 2020-05-01',
            'lexicon_size': len(self.lexicon),
            'statistics': stats,
            'output_directory': str(self.output_dir)
        }
        
        summary_path = self.output_dir / "indexing_summary.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"✓ Summary saved to: {summary_path}")
        
        return summary_path


def main():
    """Main execution function."""
    BASE_PATH = r"D:\Hamza\cord-19_2020-05-01"
    LEXICON_PATH = r"D:\Hamza\cord-19_2020-05-01\res\lexicon\cord19_lexicon.json"
    OUTPUT_DIR = r"D:\Hamza\cord-19_2020-05-01\res\forward_indexing"
    
    print("\n" + "="*80)
    print("CORD-19 FORWARD INDEXING SYSTEM")
    print("Creating individual forward index files for each paper")
    print("="*80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80 + "\n")
    
    try:
        # Initialize builder
        builder = ForwardIndexBuilder(BASE_PATH, LEXICON_PATH, OUTPUT_DIR)
        
        stats = {
            'pdf_json': {},
            'pmc_json': {}
        }
        
        # Process PDF JSON files
        pdf_processed, pdf_failed = builder.process_directory(
            builder.pdf_json_dir, 
            'pdf_json'
        )
        stats['pdf_json'] = {
            'processed': pdf_processed,
            'failed': pdf_failed,
            'total': pdf_processed + pdf_failed
        }
        
        # Process PMC JSON files
        pmc_processed, pmc_failed = builder.process_directory(
            builder.pmc_json_dir, 
            'pmc_json'
        )
        stats['pmc_json'] = {
            'processed': pmc_processed,
            'failed': pmc_failed,
            'total': pmc_processed + pmc_failed
        }
        
        # Create summary
        summary_path = builder.create_index_summary(stats)
        
        # Final statistics
        total_processed = pdf_processed + pmc_processed
        total_failed = pdf_failed + pmc_failed
        total_files = total_processed + total_failed
        
        print("\n" + "="*80)
        print("FORWARD INDEXING COMPLETE")
        print("="*80)
        print(f"\n📊 Overall Statistics:")
        print(f"   • Total files found: {total_files:,}")
        print(f"   • Successfully indexed: {total_processed:,}")
        print(f"   • Failed: {total_failed:,}")
        print(f"   • Success rate: {(total_processed/total_files*100):.1f}%")
        
        print(f"\n📁 PDF JSON:")
        print(f"   • Processed: {pdf_processed:,}")
        print(f"   • Failed: {pdf_failed:,}")
        
        print(f"\n📁 PMC JSON:")
        print(f"   • Processed: {pmc_processed:,}")
        print(f"   • Failed: {pmc_failed:,}")
        
        print(f"\n📂 Output Location:")
        print(f"   • {OUTPUT_DIR}")
        print(f"   • PDF indexes: {OUTPUT_DIR}\\pdf_json\\")
        print(f"   • PMC indexes: {OUTPUT_DIR}\\pmc_json\\")
        
        print("\n" + "="*80)
        print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80 + "\n")
        
    except Exception as e:
        logger.error(f"✗ Error in forward indexing: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()