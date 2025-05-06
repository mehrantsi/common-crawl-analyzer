#!/usr/bin/env python3
"""
Parallel URL Term Frequency Analyzer

Analyzes term frequency in URLs using multiple worker processes.
Each worker reads from the same input file but processes only its assigned line range.
"""

import os
import sys
import time
import argparse
import multiprocessing
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

# Import the existing analyzer
from term_frequency_analyzer import TermFrequencyAnalyzer

def count_lines(filename):
    """Count the number of lines in a file."""
    print(f"Counting lines in {filename}...")
    with open(filename, 'r') as f:
        return sum(1 for _ in f)

def get_line_offsets(input_file, total_lines, num_chunks):
    """Calculate line ranges for each worker."""
    chunk_size = total_lines // num_chunks
    
    # Create ranges for each worker
    ranges = []
    for i in range(num_chunks):
        start_line = i * chunk_size
        end_line = (i + 1) * chunk_size if i < num_chunks - 1 else total_lines
        ranges.append((start_line, end_line))
    
    print(f"Divided {total_lines:,} lines into {num_chunks} ranges")
    return ranges

class RangeURLReader:
    """Class to read a specific range of lines from a file"""
    def __init__(self, filename, start_line, end_line):
        self.filename = filename
        self.start_line = start_line
        self.end_line = end_line
        self.total_lines = end_line - start_line
    
    def __iter__(self):
        self.current_line = 0
        self.file = open(self.filename, 'r')
        
        # Skip to start line
        for _ in range(self.start_line):
            self.file.readline()
        
        return self
    
    def __next__(self):
        if self.current_line >= self.total_lines:
            self.file.close()
            raise StopIteration
        
        line = self.file.readline()
        if not line:
            self.file.close()
            raise StopIteration
            
        self.current_line += 1
        return line
    
    def __len__(self):
        return self.total_lines

class InMemoryTermFrequencyAnalyzer(TermFrequencyAnalyzer):
    """Modified analyzer that takes an iterable instead of a filename"""
    def __init__(self, url_iterable, output_file, min_term_length=3, max_domains_per_term=1000):
        super().__init__(None, output_file, min_term_length, max_domains_per_term)
        self.url_iterable = url_iterable
        # Change domain set to Counter for frequency tracking
        self.term_domain_counts = {}
    
    def analyze(self):
        """Analyze URLs from the iterable and build term histogram."""
        processed_urls = 0
        unique_domains = 0
        unique_terms = 0
        
        print(f"Processing {len(self.url_iterable):,} URLs in range")
        
        for url in self.url_iterable:
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
                # Update term count
                self.term_histogram[term]["count"] += 1
                
                # Initialize domain counter if not exists
                if term not in self.term_domain_counts:
                    self.term_domain_counts[term] = {}
                
                # Count domain frequency for this term
                if domain_id in self.term_domain_counts[term]:
                    self.term_domain_counts[term][domain_id] += 1
                else:
                    self.term_domain_counts[term][domain_id] = 1
                    self.term_histogram[term]["domains"].add(domain_id)
            
            processed_urls += 1
            if len(self.term_histogram) > unique_terms:
                unique_terms = len(self.term_histogram)
            
            # Print memory usage stats occasionally
            if processed_urls % 1000000 == 0:
                print(f"Worker processed {processed_urls:,} URLs, {unique_domains:,} domains, {unique_terms:,} terms")
        
        print(f"Finished processing range. Found {processed_urls:,} URLs, {unique_domains:,} domains, {unique_terms:,} terms")

def process_range(input_file, start_line, end_line, min_term_length):
    """Process a specific range of lines from the input file."""
    print(f"Processing lines {start_line:,} to {end_line:,} ({end_line - start_line:,} lines)")
    
    # Create a reader for this range
    url_reader = RangeURLReader(input_file, start_line, end_line)
    
    # Create and run analyzer
    analyzer = InMemoryTermFrequencyAnalyzer(url_reader, "not_used.csv", min_term_length)
    analyzer.analyze()
    
    # Get results
    total_count = sum(data["count"] for data in analyzer.term_histogram.values())
    
    # Sort terms by frequency
    sorted_terms = sorted(
        analyzer.term_histogram.items(),
        key=lambda x: x[1]["count"],
        reverse=True
    )
    
    # Convert domain IDs to actual domains with frequencies
    result = []
    for term, data in sorted_terms:
        # Convert domain IDs to domain names and include frequency
        domain_freqs = {}
        for domain_id, freq in analyzer.term_domain_counts.get(term, {}).items():
            domain = analyzer.domain_lookup[domain_id]
            domain_freqs[domain] = freq
        
        result.append((term, data["count"], domain_freqs))
    
    return result, total_count

def merge_results(results, output_file, percentage=0.8):
    """Merge results from all chunks and write to output file."""
    print("Merging results from all workers...")
    
    # Combine term frequencies across all chunks
    merged_terms = {}
    total_count = 0
    
    for chunk_result, chunk_total in results:
        total_count += chunk_total
        
        for term, count, domain_freqs in chunk_result:
            if term in merged_terms:
                merged_terms[term]["count"] += count
                
                # Merge domain frequencies
                for domain, freq in domain_freqs.items():
                    if domain in merged_terms[term]["domains"]:
                        merged_terms[term]["domains"][domain] += freq
                    else:
                        merged_terms[term]["domains"][domain] = freq
            else:
                merged_terms[term] = {
                    "count": count, 
                    "domains": domain_freqs
                }
    
    # Sort by frequency
    sorted_terms = sorted(
        merged_terms.items(),
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
    
    print(f"Writing top {len(top_terms):,} terms (representing {percentage*100:.1f}% of occurrences) to {output_file}")
    
    with open(output_file, 'w') as f:
        f.write("term,frequency,domains\n")
        for term, data in top_terms:
            # Sort domains by frequency and take top 1000
            sorted_domains = sorted(data["domains"].items(), key=lambda x: x[1], reverse=True)[:1000]
            domains_str = ','.join(domain for domain, _ in sorted_domains)
            f.write(f"{term},{data['count']},\"{domains_str}\"\n")
    
    print(f"Results written to {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Parallel URL Term Frequency Analyzer")
    parser.add_argument("input_file", help="Path to URLs file")
    parser.add_argument("output_file", help="Path to output CSV file")
    parser.add_argument("--workers", type=int, default=multiprocessing.cpu_count(),
                      help=f"Number of worker processes (default: {multiprocessing.cpu_count()})")
    parser.add_argument("--min-term-length", type=int, default=3,
                      help="Minimum length of terms to consider (default: 3)")
    parser.add_argument("--percentage", type=float, default=0.8,
                      help="Percentage of term occurrences to include (default: 0.8)")
    parser.add_argument("--total-lines", type=int, 
                      help="Total number of lines in input file (skips counting if provided)")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input_file):
        print(f"Error: Input file {args.input_file} does not exist")
        sys.exit(1)
    
    start_time = time.time()
    
    # Count total lines only if not provided
    total_lines = args.total_lines
    if total_lines is None:
        total_lines = count_lines(args.input_file)
    
    print(f"Processing {total_lines:,} lines in {args.input_file}")
    
    # Get line ranges for workers
    ranges = get_line_offsets(args.input_file, total_lines, args.workers)
    
    # Process ranges in parallel
    results = []
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for start_line, end_line in ranges:
            future = executor.submit(
                process_range, 
                args.input_file, 
                start_line, 
                end_line, 
                args.min_term_length
            )
            futures.append(future)
        
        # Collect results as they complete
        for future in futures:
            results.append(future.result())
    
    # Merge results
    merge_results(results, args.output_file, args.percentage)
    
    elapsed_time = time.time() - start_time
    print(f"Total processing time: {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main() 