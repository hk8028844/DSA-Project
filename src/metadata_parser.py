"""
File 1: 1_metadata_parser.py
CORD-19 Metadata Parser - Enhanced for comprehensive lexicon building
Parses metadata CSV and catalogs all PDF and PMC JSON files
"""

import pandas as pd
import json
import logging
import os
from pathlib import Path
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CORD19MetadataParser:
    """Comprehensive parser for CORD-19 dataset metadata and files."""
    
    def __init__(self, base_path: str, output_dir: str):
        self.base_path = Path(base_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Paths
        self.metadata_path = self.base_path / "2020-05-01" / "metadata.csv"
        self.pdf_json_dir = self.base_path / "2020-05-01" / "pdf_json"
        self.pmc_json_dir = self.base_path / "2020-05-01" / "pmc_json"
        
        self.df = None
        
    def discover_all_files(self):
        """Discover all available PDF and PMC JSON files."""
        logger.info("🔍 Discovering all JSON files in directories...")
        
        pdf_files = {}
        pmc_files = {}
        
        # Discover PDF JSON files
        if self.pdf_json_dir.exists():
            for pdf_file in self.pdf_json_dir.rglob("*.json"):
                file_id = pdf_file.stem
                full_path = str(pdf_file)
                pdf_files[file_id] = full_path
            logger.info(f"✓ Found {len(pdf_files)} PDF JSON files")
        else:
            logger.warning(f"⚠ PDF JSON directory not found: {self.pdf_json_dir}")
        
        # Discover PMC JSON files
        if self.pmc_json_dir.exists():
            for pmc_file in self.pmc_json_dir.rglob("*.json"):
                file_id = pmc_file.stem
                full_path = str(pmc_file)
                pmc_files[file_id] = full_path
            logger.info(f"✓ Found {len(pmc_files)} PMC JSON files")
        else:
            logger.warning(f"⚠ PMC JSON directory not found: {self.pmc_json_dir}")
        
        return pdf_files, pmc_files
        
    def load_metadata(self) -> pd.DataFrame:
        """Load metadata CSV file into DataFrame."""
        try:
            logger.info(f"📂 Loading metadata from {self.metadata_path}")
            self.df = pd.read_csv(self.metadata_path, low_memory=False)
            logger.info(f"✓ Loaded {len(self.df)} records from metadata.csv")
            logger.info(f"✓ Available columns: {list(self.df.columns)}")
            return self.df
        except Exception as e:
            logger.error(f"✗ Error loading metadata: {e}")
            raise
    
    def get_all_papers_comprehensive(self):
        """
        Get ALL papers with comprehensive file mapping.
        Links metadata records with discovered JSON files.
        """
        if self.df is None:
            self.load_metadata()
        
        # Discover all available files
        pdf_files_map, pmc_files_map = self.discover_all_files()
        
        logger.info("\n" + "="*80)
        logger.info("📊 Building comprehensive paper catalog...")
        logger.info("="*80)
        
        papers = []
        papers_with_metadata = 0
        papers_with_pdf = 0
        papers_with_pmc = 0
        papers_with_both = 0
        papers_with_abstract = 0
        
        # Process each metadata record
        for idx, row in self.df.iterrows():
            paper_info = {
                'index': idx,
                'cord_uid': str(row['cord_uid']) if pd.notna(row['cord_uid']) else '',
                'sha': str(row['sha']) if pd.notna(row['sha']) else '',
                'title': str(row['title']) if pd.notna(row['title']) else '',
                'abstract': str(row['abstract']) if pd.notna(row['abstract']) else '',
                'publish_time': str(row['publish_time']) if pd.notna(row['publish_time']) else '',
                'authors': str(row['authors']) if pd.notna(row['authors']) else '',
                'journal': str(row['journal']) if pd.notna(row['journal']) else '',
                'doi': str(row['doi']) if pd.notna(row['doi']) else '',
                'pmcid': str(row['pmcid']) if pd.notna(row['pmcid']) else '',
                'pubmed_id': str(row['pubmed_id']) if pd.notna(row['pubmed_id']) else '',
                'has_pdf_parse': False,
                'has_pmc_xml_parse': False,
                'pdf_json_files': [],
                'pmc_json_files': []
            }
            
            # Count papers with abstracts
            if paper_info['abstract'] and paper_info['abstract'] != 'nan':
                papers_with_abstract += 1
            
            # Map PDF files using SHA hash
            if paper_info['sha'] and paper_info['sha'] != 'nan':
                sha_list = paper_info['sha'].split('; ')
                for sha in sha_list:
                    sha = sha.strip()
                    if sha in pdf_files_map:
                        paper_info['pdf_json_files'].append(pdf_files_map[sha])
                        paper_info['has_pdf_parse'] = True
            
            # Map PMC files using PMCID
            pmcid = paper_info.get('pmcid', '')
            if pmcid and pmcid != 'nan':
                pmcid_clean = pmcid.replace('PMC', '')
                if pmcid in pmc_files_map:
                    paper_info['pmc_json_files'].append(pmc_files_map[pmcid])
                    paper_info['has_pmc_xml_parse'] = True
                elif pmcid_clean in pmc_files_map:
                    paper_info['pmc_json_files'].append(pmc_files_map[pmcid_clean])
                    paper_info['has_pmc_xml_parse'] = True
            
            # Count statistics
            papers_with_metadata += 1
            if paper_info['has_pdf_parse']:
                papers_with_pdf += 1
            if paper_info['has_pmc_xml_parse']:
                papers_with_pmc += 1
            if paper_info['has_pdf_parse'] and paper_info['has_pmc_xml_parse']:
                papers_with_both += 1
            
            papers.append(paper_info)
        
        # Add all unmapped JSON files
        logger.info("\n📋 Adding unmapped JSON files to catalog...")
        
        mapped_pdfs = set()
        mapped_pmcs = set()
        
        for paper in papers:
            for pdf in paper['pdf_json_files']:
                mapped_pdfs.add(pdf)
            for pmc in paper['pmc_json_files']:
                mapped_pmcs.add(pmc)
        
        # Add unmapped PDFs
        unmapped_pdf_count = 0
        for file_id, file_path in pdf_files_map.items():
            if file_path not in mapped_pdfs:
                papers.append({
                    'index': len(papers),
                    'cord_uid': '',
                    'sha': file_id,
                    'title': '',
                    'abstract': '',
                    'publish_time': '',
                    'authors': '',
                    'journal': '',
                    'doi': '',
                    'pmcid': '',
                    'pubmed_id': '',
                    'has_pdf_parse': True,
                    'has_pmc_xml_parse': False,
                    'pdf_json_files': [file_path],
                    'pmc_json_files': []
                })
                unmapped_pdf_count += 1
        
        # Add unmapped PMCs
        unmapped_pmc_count = 0
        for file_id, file_path in pmc_files_map.items():
            if file_path not in mapped_pmcs:
                papers.append({
                    'index': len(papers),
                    'cord_uid': '',
                    'sha': '',
                    'title': '',
                    'abstract': '',
                    'publish_time': '',
                    'authors': '',
                    'journal': '',
                    'doi': '',
                    'pmcid': file_id,
                    'pubmed_id': '',
                    'has_pdf_parse': False,
                    'has_pmc_xml_parse': True,
                    'pdf_json_files': [],
                    'pmc_json_files': [file_path]
                })
                unmapped_pmc_count += 1
        
        # Log statistics
        logger.info(f"\n📊 Dataset Statistics:")
        logger.info(f"   • Total papers in metadata: {papers_with_metadata:,}")
        logger.info(f"   • Papers with abstracts: {papers_with_abstract:,}")
        logger.info(f"   • Papers with PDF full text: {papers_with_pdf:,}")
        logger.info(f"   • Papers with PMC full text: {papers_with_pmc:,}")
        logger.info(f"   • Papers with BOTH PDF & PMC: {papers_with_both:,}")
        logger.info(f"   • Unmapped PDF files added: {unmapped_pdf_count:,}")
        logger.info(f"   • Unmapped PMC files added: {unmapped_pmc_count:,}")
        logger.info(f"   • TOTAL PAPERS (including unmapped): {len(papers):,}")
        logger.info(f"   • Available PDF JSON files: {len(pdf_files_map):,}")
        logger.info(f"   • Available PMC JSON files: {len(pmc_files_map):,}")
        
        return papers
    
    def export_papers_info(self):
        """Export comprehensive paper information to JSON."""
        papers = self.get_all_papers_comprehensive()
        
        output_data = {
            'total_papers': len(papers),
            'generation_date': pd.Timestamp.now().isoformat(),
            'dataset': 'CORD-19 2020-05-01',
            'papers': papers
        }
        
        output_path = self.output_dir / "papers_info.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"\n✓ Exported {len(papers):,} papers to {output_path}")
        
        # Also save a summary
        summary = {
            'total_papers': len(papers),
            'papers_with_pdf': sum(1 for p in papers if p['has_pdf_parse']),
            'papers_with_pmc': sum(1 for p in papers if p['has_pmc_xml_parse']),
            'papers_with_abstract': sum(1 for p in papers if p['abstract'] and p['abstract'] != 'nan'),
            'generation_date': pd.Timestamp.now().isoformat()
        }
        
        summary_path = self.output_dir / "dataset_summary.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"✓ Dataset summary saved to {summary_path}")
        
        return len(papers), str(output_path)


def main():
    """Main execution function."""
    BASE_PATH = r"D:\Hamza\cord-19_2020-05-01"
    OUTPUT_DIR = r"D:\Hamza\cord-19_2020-05-01\res"
    
    print("\n" + "="*80)
    print("STEP 1: CORD-19 COMPREHENSIVE METADATA PARSER")
    print("Parsing metadata.csv + Discovering all PDF/PMC JSON files")
    print("="*80 + "\n")
    
    parser = CORD19MetadataParser(BASE_PATH, OUTPUT_DIR)
    num_papers, output_path = parser.export_papers_info()
    
    print("\n" + "="*80)
    print(f"✓ STEP 1 COMPLETE: Cataloged {num_papers:,} papers")
    print(f"✓ Output saved to: {output_path}")
    print("="*80 + "\n")
    
    return output_path


if __name__ == "__main__":
    main()