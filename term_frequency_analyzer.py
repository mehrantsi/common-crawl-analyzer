#!/usr/bin/env python3
"""
URL Term Frequency Analyzer

Analyzes term frequency in URLs and associates terms with domains.
Writes results to a file showing term frequency and associated domains.
"""

import re
import os
import sys
import urllib.parse
from collections import defaultdict, Counter
import tqdm
import nltk
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
import tldextract

# Download required NLTK data
nltk.download('stopwords', quiet=True)

class TermFrequencyAnalyzer:
    def __init__(self, input_file, output_file, min_term_length=3, max_domains_per_term=1000):
        self.input_file = input_file
        self.output_file = output_file
        self.min_term_length = min_term_length
        self.max_domains_per_term = max_domains_per_term
        
        # Initialize data structures
        self.domain_lookup = {}  # Maps domain ID to domain string
        self.domain_to_id = {}   # Maps domain string to domain ID
        self.term_histogram = defaultdict(lambda: {"count": 0, "domains": set()})
        
        # Initialize stemmer and stopwords
        self.stemmer = PorterStemmer()
        self.stopwords = set(stopwords.words('english'))
        # Add common URL parts but NOT TLDs (we want to keep all TLDs)
        self.stopwords.update([
            'https', 'http', 
            # Common file extensions
            'php', 'html', 'htm', 'aspx', 'asp', 'jsp', 'cfm', 'cgi', 'shtml', 'xhtml',
            # Common URL parts
            'index', 'default', 'home', 'main', 'page', 'tag', 'tags', 'category', 'categories',
            'article', 'articles', 'post', 'posts', 'entry', 'entries', 'comment', 'comments',
            'archive', 'archives', 'search', 'feed', 'rss', 'xml', 'sitemap', 'contact',
            # Common URL parameters
            'page', 'id', 'tid', 'pid', 'uid', 'sid', 'cid', 'mid', 'nid', 'itemid', 'userid',
            'view', 'action', 'type', 'format', 'mode', 'sort', 'order', 'limit', 'start',
            'ref', 'source', 'lang', 'language', 'locale', 'version', 'theme', 'template',
            # Common sizes and dimensions
            'small', 'medium', 'large', 'thumb', 'thumbnail', 'preview', 'full', 'mini',
            # Common content types
            'image', 'img', 'photo', 'pic', 'picture', 'file', 'document', 'doc', 'pdf',
            'video', 'audio', 'mp3', 'mp4', 'webm', 'mov', 'avi',
            # Common generic terms
            'content', 'system', 'module', 'component', 'plugin', 'include', 'upload',
            'download', 'print', 'public', 'private', 'admin', 'login', 'user', 'account'
        ])
        
        # Patterns to ignore
        self.ignore_patterns = [
            r'^\d+$',                          # Pure numbers
            r'^\d+[\-\.\/]\d+(?:[\-\.\/]\d+)?$',  # Date patterns like 2024-04, 2024-04-01, 04/01/2024, etc.
            r'^ww\w*$',                        # www, ww2, wwsite, etc.
            r'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$',  # UUIDs
            r'^[0-9a-f]{32}$',                 # MD5 hashes
            r'^[0-9a-f]{40}$',                 # SHA1 hashes
            r'^[0-9a-f]{64}$',                 # SHA256 hashes
            r'^\w+\d+$',                       # Common patterns like page1, form2, id123
            r'^\d+\w+$',                       # Common patterns like 123page, 2form
            r'^v\d+$',                         # Version patterns like v1, v2, v3
            r'^[a-z]\d+$',                     # Single letter followed by numbers like p123, s456
            r'^\w{1,2}$'                       # 1-2 character terms
        ]
    
    def extract_domain(self, url):
        """Extract normalized domain from URL."""
        try:
            # Use tldextract to properly get the registered domain
            ext = tldextract.extract(url)
            
            # If no domain or TLD, return None
            if not ext.domain or not ext.suffix:
                return None
            
            # Return the registered domain (domain + TLD)
            return f"{ext.domain}.{ext.suffix}"
            
        except Exception:
            return None
    
    def get_domain_id(self, domain):
        """Get domain ID from domain string, adding to lookup if not exists."""
        if domain in self.domain_to_id:
            return self.domain_to_id[domain]
        
        domain_id = len(self.domain_lookup)
        self.domain_lookup[domain_id] = domain
        self.domain_to_id[domain] = domain_id
        return domain_id
    
    def extract_terms(self, url):
        """Extract meaningful terms from URL, normalize, and filter out common terms."""
        try:
            parsed = urllib.parse.urlparse(url)
            
            # Get the path and query parts
            path = parsed.path.lower()
            query = parsed.query.lower()
            
            # Also include the hostname parts (except TLD)
            hostname = parsed.netloc.lower()
            hostname = re.sub(r'^ww\w*\.', '', hostname)  # Remove ww*, www, ww2, etc.
            
            # Extract hostname parts (excluding TLD)
            hostname_parts = hostname.split('.')
            if len(hostname_parts) > 1:
                # Only use the domain part (not the TLD)
                domain_part = hostname_parts[-2]
            else:
                domain_part = ""
            
            # Combine hostname and path for term extraction
            combined = f"{domain_part} {path} {query}"
            
            # Replace special characters with spaces
            combined = re.sub(r'[^a-z0-9]', ' ', combined)
            
            # Split into words
            words = combined.split()
            
            # Process each word
            terms = []
            for word in words:
                # Skip short words
                if len(word) < self.min_term_length:
                    continue
                
                # Skip stopwords
                if word in self.stopwords:
                    continue
                
                # Skip patterns to ignore
                if any(re.match(pattern, word) for pattern in self.ignore_patterns):
                    continue
                
                # Stem the word (normalize plural forms)
                stemmed = self.stemmer.stem(word)
                
                # Skip stems that are too short
                if len(stemmed) < self.min_term_length:
                    continue
                
                terms.append(stemmed)
            
            return terms
        except Exception:
            return []
    
    def analyze(self):
        """Analyze URL file and build term histogram."""
        # Get file size for progress tracking
        file_size = os.path.getsize(self.input_file)
        total_lines = sum(1 for _ in open(self.input_file, 'r'))
        
        processed_urls = 0
        unique_domains = 0
        unique_terms = 0
        
        print(f"Processing {self.input_file} ({total_lines:,} URLs)")
        
        with open(self.input_file, 'r') as f:
            for url in tqdm.tqdm(f, total=total_lines, unit="URLs"):
                url = url.strip()
                if not url:
                    continue
                
                # Extract domain
                domain = self.extract_domain(url)
                if not domain:
                    continue
                
                # Get domain ID
                domain_id = self.get_domain_id(domain)
                if domain_id == unique_domains:
                    unique_domains += 1
                
                # Extract terms
                terms = self.extract_terms(url)
                
                # Update histogram
                for term in terms:
                    self.term_histogram[term]["count"] += 1
                    if len(self.term_histogram[term]["domains"]) < self.max_domains_per_term:
                        self.term_histogram[term]["domains"].add(domain_id)
                
                processed_urls += 1
                if len(self.term_histogram) > unique_terms:
                    unique_terms = len(self.term_histogram)
                
                # Print memory usage stats every 1M URLs
                if processed_urls % 1000000 == 0:
                    print(f"Processed {processed_urls:,} URLs, {unique_domains:,} domains, {unique_terms:,} terms")
        
        print(f"Finished processing. Found {processed_urls:,} URLs, {unique_domains:,} domains, {unique_terms:,} terms")
    
    def write_results(self, percentage=0.8):
        """
        Write the top terms (by frequency) that make up percentage of occurrences
        to the output file.
        """
        print(f"Sorting terms by frequency...")
        
        # Get total term count
        total_count = sum(data["count"] for data in self.term_histogram.values())
        
        # Sort terms by frequency
        sorted_terms = sorted(
            self.term_histogram.items(),
            key=lambda x: x[1]["count"],
            reverse=True
        )
        
        # Determine cutoff for top percentage
        running_sum = 0
        cutoff_index = 0
        for i, (term, data) in enumerate(sorted_terms):
            running_sum += data["count"]
            if running_sum / total_count >= percentage:
                cutoff_index = i + 1
                break
        
        top_terms = sorted_terms[:cutoff_index]
        
        print(f"Writing top {len(top_terms):,} terms (representing {percentage*100:.1f}% of occurrences) to {self.output_file}")
        
        with open(self.output_file, 'w') as f:
            f.write("term,frequency,domains\n")
            for term, data in top_terms:
                domains_str = ','.join(self.domain_lookup[domain_id] for domain_id in data["domains"])
                f.write(f"{term},{data['count']},\"{domains_str}\"\n")
        
        print(f"Results written to {self.output_file}")

def main():
    if len(sys.argv) < 3:
        print("Usage: python term_frequency_analyzer.py <input_file> <output_file> [min_term_length]")
        print("  input_file: Path to urls.txt file")
        print("  output_file: Path to output file")
        print("  min_term_length: Minimum length of terms to consider (default: 3)")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    min_term_length = int(sys.argv[3]) if len(sys.argv) > 3 else 3
    
    if not os.path.exists(input_file):
        print(f"Error: Input file {input_file} does not exist")
        sys.exit(1)
    
    analyzer = TermFrequencyAnalyzer(input_file, output_file, min_term_length)
    analyzer.analyze()
    analyzer.write_results(0.8)

if __name__ == "__main__":
    main() 