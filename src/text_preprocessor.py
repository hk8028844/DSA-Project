"""
File 2: 2_text_preprocessor.py
Enhanced Medical/Biological Lexicon Builder
Extracts medical terms, biological terminology, citations, and unique domain-specific vocabulary
Focuses on terms NOT in standard dictionaries but crucial for medical search
"""

import json
import os
import re
from collections import defaultdict
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords, words as nltk_words

# NLTK setup
try:
    word_tokenize("test")
except:
    print("Downloading NLTK punkt...")
    nltk.download('punkt', quiet=True)
    nltk.download('punkt_tab', quiet=True)

try:
    lemmatizer = WordNetLemmatizer()
    lemmatizer.lemmatize("test")
except:
    print("Downloading NLTK wordnet...")
    nltk.download('wordnet', quiet=True)
    nltk.download('omw-1.4', quiet=True)

try:
    stopwords.words('english')
except:
    print("Downloading NLTK stopwords...")
    nltk.download('stopwords', quiet=True)

try:
    nltk_words.words()
except:
    print("Downloading NLTK words corpus...")
    nltk.download('words', quiet=True)


class MedicalLexiconBuilder:
    """Extract medical/biological terms and unique vocabulary for comprehensive lexicon."""
    
    def __init__(self, base_path):
        self.base_path = base_path
        self.lemmatizer = WordNetLemmatizer()
        
        # Load English stop words (minimal set)
        self.stop_words = set(stopwords.words('english'))
        
        # Only remove the most common, non-informative stop words
        minimal_stops = {
            'the', 'a', 'an', 'and', 'or', 'but', 'if', 'of', 'at', 'by', 'for',
            'with', 'from', 'to', 'in', 'on', 'is', 'was', 'are', 'were', 'be',
            'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
            'this', 'that', 'these', 'those', 'it', 'its', 'their', 'them',
            'we', 'our', 'you', 'your'
        }
        self.stop_words = minimal_stops
        
        # Load standard English dictionary for comparison
        try:
            self.english_words = set(w.lower() for w in nltk_words.words())
            print(f"✓ Loaded {len(self.english_words):,} standard English words for comparison")
        except:
            self.english_words = set()
            print("⚠ Could not load English dictionary - will include all terms")
        
        print(f"✓ Using minimal stop words list: {len(self.stop_words)} terms")
        
    def is_medical_or_specialized_term(self, word):
        """
        Check if a word is a medical/biological/specialized term.
        Prioritizes terms NOT in standard dictionaries.
        """
        # Must be at least 2 characters
        if len(word) < 2:
            return False
        
        # Skip if it's a basic stop word
        if word.lower() in self.stop_words:
            return False
        
        # Skip pure numbers
        if word.isdigit():
            return False
        
        # KEEP scientific patterns (these are valuable!)
        # COVID-19, SARS-CoV-2, IL-6, T-cell, ACE-2, etc.
        if re.match(r'^[a-z]+[0-9]*(-[a-z0-9]+)+$', word.lower()):
            return True
        
        # KEEP protein/gene patterns: ACE2, IL6, CD4, CD8, H1N1, p53, etc.
        if re.match(r'^[a-z]+\d+[a-z]*$', word.lower()):
            return True
        
        # KEEP Greek letters and symbols: alpha, beta, gamma, delta, etc.
        greek_letters = {'alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta', 
                        'eta', 'theta', 'iota', 'kappa', 'lambda', 'mu', 'nu',
                        'xi', 'omicron', 'pi', 'rho', 'sigma', 'tau', 'upsilon',
                        'phi', 'chi', 'psi', 'omega'}
        if word.lower() in greek_letters:
            return True
        
        # KEEP medical prefixes and suffixes
        medical_affixes = {
            'cardio', 'neuro', 'hepato', 'nephro', 'gastro', 'pulmo', 'osteo',
            'hemo', 'lymph', 'cyto', 'patho', 'onco', 'dermato', 'endo', 'exo',
            'hyper', 'hypo', 'anti', 'bio', 'micro', 'macro', 'poly', 'mono',
            'itis', 'osis', 'emia', 'pathy', 'ology', 'oma', 'algia', 'ectomy',
            'plasty', 'scopy', 'gram', 'graph', 'meter', 'lysis', 'genesis'
        }
        for affix in medical_affixes:
            if affix in word.lower():
                return True
        
        # KEEP abbreviations (all caps, 2-6 letters)
        if word.isupper() and 2 <= len(word) <= 6:
            return True
        
        # KEEP mixed case terms (likely proper names or special terms)
        if word[0].isupper() and any(c.isupper() for c in word[1:]):
            return True
        
        # Must contain at least one letter
        if not re.search(r'[a-zA-Z]', word):
            return False
        
        # PRIORITIZE: Terms NOT in standard English dictionary
        # These are likely specialized medical/scientific terms
        if self.english_words and word.lower() not in self.english_words:
            # Make sure it looks like a real word (has vowels for longer words)
            if len(word) >= 4:
                if re.search(r'[aeiouy]', word.lower()):
                    return True
            else:
                return True
        
        # KEEP common medical/biological terms even if in dictionary
        medical_keywords = {
            'virus', 'viral', 'bacteria', 'bacterial', 'infection', 'disease',
            'syndrome', 'therapy', 'treatment', 'diagnosis', 'patient', 'clinical',
            'pathogen', 'immune', 'antibody', 'antigen', 'vaccine', 'protein',
            'enzyme', 'receptor', 'cell', 'tissue', 'organ', 'blood', 'serum',
            'plasma', 'genome', 'genetic', 'mutation', 'chromosome', 'dna', 'rna',
            'respiratory', 'cardiovascular', 'neurological', 'inflammatory',
            'epidemiological', 'mortality', 'morbidity', 'symptom', 'chronic',
            'acute', 'transmission', 'contagious', 'pandemic', 'epidemic'
        }
        if word.lower() in medical_keywords:
            return True
        
        # For words in dictionary, only keep if they're longer and potentially technical
        if len(word) >= 8:
            return True
        
        return False
    
    def extract_specialized_terms(self, text):
        """Extract medical, biological, and specialized terms."""
        if not text or len(text.strip()) < 1:
            return []
        
        # Preserve case for abbreviations and proper names
        # Extract words including hyphens, numbers, and mixed case
        words = re.findall(r'\b[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9]\b|\b[A-Za-z]\b', text)
        
        results = []
        seen = set()
        
        for word in words:
            # Skip if already processed (preserve first occurrence)
            if word.lower() in seen:
                continue
            
            # Check if it's a specialized term
            if not self.is_medical_or_specialized_term(word):
                continue
            
            # For pure alphabetic words, lemmatize to base form
            if re.match(r'^[a-z]+$', word.lower()):
                try:
                    # Try noun lemmatization
                    noun_form = self.lemmatizer.lemmatize(word.lower(), 'n')
                    # Try verb lemmatization
                    verb_form = self.lemmatizer.lemmatize(word.lower(), 'v')
                    # Pick the shorter form
                    lemmatized = noun_form if len(noun_form) <= len(verb_form) else verb_form
                    
                    # Re-check if lemmatized form is still specialized
                    if self.is_medical_or_specialized_term(lemmatized):
                        results.append(lemmatized)
                        seen.add(lemmatized)
                except:
                    if self.is_medical_or_specialized_term(word.lower()):
                        results.append(word.lower())
                        seen.add(word.lower())
            else:
                # Keep compound terms, abbreviations, and mixed case as-is
                results.append(word)
                seen.add(word.lower())
        
        return results
    
    def extract_text_from_json(self, data, text_parts):
        """Recursively extract all text from JSON structure."""
        if isinstance(data, dict):
            for key, value in data.items():
                # Include more fields for comprehensive extraction
                self.extract_text_from_json(value, text_parts)
                
        elif isinstance(data, list):
            for item in data:
                self.extract_text_from_json(item, text_parts)
                
        elif isinstance(data, str) and len(data.strip()) > 0:
            text_parts.append(data)
            
        return text_parts
    
    def read_pdf_json(self, file_path):
        """Read all text from PDF JSON file."""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                data = json.load(f)
                
                text_parts = []
                
                # Extract all text comprehensively
                if 'metadata' in data:
                    self.extract_text_from_json(data['metadata'], text_parts)
                
                if 'abstract' in data:
                    self.extract_text_from_json(data['abstract'], text_parts)
                
                if 'body_text' in data:
                    self.extract_text_from_json(data['body_text'], text_parts)
                
                if 'back_matter' in data:
                    self.extract_text_from_json(data['back_matter'], text_parts)
                
                if 'bib_entries' in data:
                    self.extract_text_from_json(data['bib_entries'], text_parts)
                
                return ' '.join(text_parts)
                
        except Exception as e:
            return ""
    
    def read_pmc_json(self, file_path):
        """Read all text from PMC JSON file."""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                
                # Try JSON format
                if content.strip().startswith('{'):
                    try:
                        data = json.loads(content)
                        text_parts = []
                        self.extract_text_from_json(data, text_parts)
                        return ' '.join(text_parts)
                    except:
                        pass
                
                # Try XML format
                text = re.sub(r'<[^>]+>', ' ', content)
                text = re.sub(r'\s+', ' ', text)
                return text.strip()
                
        except Exception as e:
            return ""
    
    def read_all_paper_text(self, paper_info):
        """Read ALL text from paper: metadata, abstract, body text."""
        all_text = []
        
        # Extract metadata fields
        for field in ['title', 'abstract', 'authors', 'journal']:
            if paper_info.get(field) and str(paper_info[field]) != 'nan':
                all_text.append(str(paper_info[field]))
        
        # Read PDF JSON files
        if paper_info.get('pdf_json_files'):
            for pdf_path in paper_info['pdf_json_files']:
                if pdf_path and os.path.exists(pdf_path):
                    text = self.read_pdf_json(pdf_path)
                    if text and len(text) > 10:
                        all_text.append(text)
        
        # Read PMC JSON files
        if paper_info.get('pmc_json_files'):
            for pmc_path in paper_info['pmc_json_files']:
                if pmc_path and os.path.exists(pmc_path):
                    text = self.read_pmc_json(pmc_path)
                    if text and len(text) > 10:
                        all_text.append(text)
        
        return ' '.join(all_text)
    
    def process_papers(self, cycle_number=1):
        """Process all papers and extract specialized medical/biological terms."""
        print(f"\n{'='*80}")
        print(f"MEDICAL LEXICON EXTRACTION - CYCLE {cycle_number}")
        print(f"Extracting: Medical terms, biological vocabulary, citations")
        print(f"Priority: Terms NOT in standard dictionaries + specialized terminology")
        print(f"{'='*80}\n")
        
        # Load papers info
        papers_info_path = os.path.join(self.base_path, 'res', 'papers_info.json')
        
        if not os.path.exists(papers_info_path):
            print(f"✗ Error: {papers_info_path} not found!")
            print("  Please run 1_metadata_parser.py first")
            return []
        
        with open(papers_info_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            papers = data['papers']
        
        print(f"Loaded {len(papers):,} papers from catalog")
        print(f"Processing all text sources...\n")
        
        all_terms = []
        papers_processed = 0
        files_read = 0
        papers_with_text = 0
        
        for i, paper in enumerate(papers):
            # Progress update
            if i % 100 == 0 and i > 0:
                unique = len(set(all_terms))
                print(f"  Progress: {i:,}/{len(papers):,} papers | "
                      f"Unique terms: {unique:,} | "
                      f"Total terms: {len(all_terms):,} | "
                      f"Files read: {files_read}")
            
            # Count files
            paper_files = len(paper.get('pdf_json_files', [])) + len(paper.get('pmc_json_files', []))
            
            # Read all text
            full_text = self.read_all_paper_text(paper)
            
            if full_text and len(full_text) > 10:
                # Extract specialized terms
                terms = self.extract_specialized_terms(full_text)
                if terms:
                    all_terms.extend(terms)
                    papers_with_text += 1
                    files_read += paper_files
            
            papers_processed += 1
        
        unique_count = len(set(all_terms))
        
        print(f"\n{'='*80}")
        print(f"EXTRACTION COMPLETE - CYCLE {cycle_number}")
        print(f"{'='*80}")
        print(f"✓ Total papers in catalog: {len(papers):,}")
        print(f"✓ Papers processed: {papers_processed:,}")
        print(f"✓ Papers with extractable text: {papers_with_text:,}")
        print(f"✓ JSON files successfully read: {files_read:,}")
        print(f"✓ Total terms extracted: {len(all_terms):,}")
        print(f"✓ UNIQUE SPECIALIZED TERMS: {unique_count:,}")
        print(f"{'='*80}")
        
        # Calculate frequency
        term_freq = defaultdict(int)
        for term in all_terms:
            term_freq[term] += 1
        
        # Prepare output
        output_data = {
            'cycle': cycle_number,
            'papers_total': len(papers),
            'papers_processed': papers_processed,
            'papers_with_text': papers_with_text,
            'files_read': files_read,
            'total_words': len(all_terms),
            'unique_terms': unique_count,
            'term_frequencies': dict(term_freq)
        }
        
        # Save results
        output_path = os.path.join(self.base_path, 'res', f'processed_terms_cycle{cycle_number}.json')
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2)
        
        print(f"\n✓ Results saved to: {output_path}")
        
        # Display top terms
        print(f"\nTop 50 most frequent specialized terms:")
        print(f"{'-'*70}")
        sorted_terms = sorted(term_freq.items(), key=lambda x: x[1], reverse=True)
        for rank, (term, freq) in enumerate(sorted_terms[:50], 1):
            print(f"  {rank:2d}. {term:40s} {freq:>12,}")
        print(f"{'-'*70}\n")
        
        return all_terms


def main():
    """Main execution."""
    base_path = r"D:\Hamza\cord-19_2020-05-01"
    
    print("="*80)
    print("CORD-19 MEDICAL/BIOLOGICAL LEXICON BUILDER")
    print("Extracting specialized terminology for comprehensive medical lexicon")
    print("="*80)
    
    builder = MedicalLexiconBuilder(base_path)
    builder.process_papers(cycle_number=1)
    
    print("\n" + "="*80)
    print("✓ Medical lexicon extraction complete!")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()