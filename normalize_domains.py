#!/usr/bin/env python3
"""
Domain Name Normalizer

Reads a list of sorted domains line by line and outputs a normalized list 
where all variations of a domain (www.example.com, blog.example.com, etc.)
are converted to the registered domain (example.com, example.co.uk, etc.)

Since the input file is sorted, this uses a streaming approach that doesn't 
require keeping all domains in memory.
"""

import sys
import tldextract
from tqdm import tqdm

def normalize_domain(domain):
    """
    Extract the registered domain (domain + TLD) from any domain or URL.
    Example:
        www.example.com -> example.com
        blog.example.co.uk -> example.co.uk
    """
    # Skip empty lines
    if not domain or domain.isspace():
        return None
    
    # Extract domain components
    ext = tldextract.extract(domain.strip())
    
    # If no domain or TLD, return None
    if not ext.domain or not ext.suffix:
        return None
    
    # Return the registered domain (domain + TLD)
    return f"{ext.domain}.{ext.suffix}"

def main():
    if len(sys.argv) < 3:
        print("Usage: python normalize_domains.py <input_file> <output_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    # Count total lines for progress tracking
    if input_file == '-':
        # Reading from stdin, can't count lines in advance
        total_lines = None
        input_stream = sys.stdin
    else:
        total_lines = sum(1 for _ in open(input_file, 'r'))
        input_stream = open(input_file, 'r')
    
    print(f"Processing domains...")
    
    # Streaming approach - write domains as we go
    current_normalized = None
    domain_count = 0
    unique_count = 0
    
    with open(output_file, 'w') as out_file:
        try:
            for line in tqdm(input_stream, total=total_lines, unit="domains"):
                original_domain = line.strip()
                if not original_domain:
                    continue
                    
                normalized = normalize_domain(original_domain)
                if not normalized:
                    continue
                
                domain_count += 1
                
                # If this is a different normalized domain from the current one,
                # write the current one and update
                if normalized != current_normalized:
                    if current_normalized:
                        out_file.write(f"{current_normalized}\n")
                        unique_count += 1
                    current_normalized = normalized
            
            # Write the last domain if there was one
            if current_normalized:
                out_file.write(f"{current_normalized}\n")
                unique_count += 1
                
        finally:
            if input_file != '-':
                input_stream.close()
    
    print(f"Processed {domain_count:,} domains")
    print(f"Found {unique_count:,} unique normalized domains")
    print(f"Results written to {output_file}")

if __name__ == "__main__":
    main() 