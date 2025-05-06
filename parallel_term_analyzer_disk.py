#!/usr/bin/env python3
"""
Memory-Efficient Parallel Term Frequency Analyzer

Analyzes term frequency in URLs using multiple worker processes.
Uses disk-based storage with checkpointing to handle datasets of any size.
Can resume processing if interrupted.
"""

import os
import sys
import time
import json
import pickle
import shutil
import argparse
import multiprocessing
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
import gc  # Import garbage collector
import concurrent.futures

# Import the existing analyzer components
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
    def __init__(self, filename, start_line, end_line, sample_rate=1):
        self.filename = filename
        self.start_line = start_line
        self.end_line = end_line
        self.sample_rate = sample_rate
        self.total_lines = (end_line - start_line + sample_rate - 1) // sample_rate
    
    def __iter__(self):
        self.current_index = 0
        self.current_line = 0
        self.file = open(self.filename, 'r')
        
        # Skip to start line
        for _ in range(self.start_line):
            self.file.readline()
        
        return self
    
    def __next__(self):
        while True:
            if self.current_line >= (self.end_line - self.start_line):
                self.file.close()
                raise StopIteration
            
            line = self.file.readline()
            if not line:
                self.file.close()
                raise StopIteration
            
            self.current_line += 1
            
            # If we're sampling, only return every Nth line
            if self.sample_rate > 1:
                if self.current_line % self.sample_rate == 0:
                    self.current_index += 1
                    return line
            else:
                self.current_index += 1
                return line
    
    def __len__(self):
        return self.total_lines

class DiskBackedTermFrequencyAnalyzer(TermFrequencyAnalyzer):
    """
    Modified analyzer that periodically flushes data to disk to save memory
    and handles data in smaller chunks
    """
    def __init__(self, url_iterable, output_dir, worker_id, min_term_length=3, 
                 max_domains_per_term=1000, flush_threshold=5000000,
                 checkpoint_file=None):
        super().__init__(None, None, min_term_length, max_domains_per_term)
        self.url_iterable = url_iterable
        self.output_dir = Path(output_dir)
        self.worker_id = worker_id
        self.flush_threshold = flush_threshold  # Flush after this many terms
        self.flush_count = 0
        self.chunk_count = 0
        self.checkpoint_file = checkpoint_file
        
        # Change domain set to Counter for frequency tracking
        self.term_domain_counts = {}
        
        # Create output directory
        self.worker_dir = self.output_dir / f"worker_{worker_id}"
        self.worker_dir.mkdir(exist_ok=True, parents=True)
        
        # Load checkpoint if exists
        self.processed_urls = 0
        self.total_terms_found = 0
        if checkpoint_file and os.path.exists(checkpoint_file):
            with open(checkpoint_file, 'r') as f:
                checkpoint = json.load(f)
                self.processed_urls = checkpoint.get('processed_urls', 0)
                self.total_terms_found = checkpoint.get('total_terms_found', 0)
                self.chunk_count = checkpoint.get('chunk_count', 0)
    
    def _flush_to_disk(self):
        """Write current histogram to disk and clear memory"""
        # Skip if no terms to write
        if not self.term_histogram:
            return
            
        # Convert domain IDs to domain names in the histograms
        serializable_histogram = {}
        for term, data in self.term_histogram.items():
            domain_freqs = {}
            for domain_id in data["domains"]:
                domain = self.domain_lookup[domain_id]
                freq = self.term_domain_counts[term].get(domain_id, 1)
                domain_freqs[domain] = freq
            
            serializable_histogram[term] = {
                "count": data["count"],
                "domains": domain_freqs
            }
        
        # Write to disk
        chunk_file = self.worker_dir / f"chunk_{self.chunk_count}.pkl"
        
        # Use a temporary file first to avoid corruption
        temp_file = chunk_file.with_suffix('.tmp')
        with open(temp_file, "wb") as f:
            pickle.dump(serializable_histogram, f, protocol=4)
        
        # Rename to final name (atomic operation)
        temp_file.rename(chunk_file)
        
        print(f"Worker {self.worker_id}: Flushed {len(self.term_histogram):,} terms to {chunk_file}")
        
        # Clear memory
        self.term_histogram.clear()
        self.term_domain_counts.clear()
        self.domain_lookup.clear()
        self.domain_to_id.clear()
        
        # Increment chunk count
        self.chunk_count += 1
        self.flush_count = 0
        
        # Update checkpoint if enabled
        if self.checkpoint_file:
            checkpoint = {
                'processed_urls': self.processed_urls,
                'total_terms_found': self.total_terms_found,
                'chunk_count': self.chunk_count
            }
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint, f)
    
    def analyze(self):
        """Analyze URLs from the iterable and build term histogram."""
        unique_domains = 0
        
        # Skip already processed URLs if resuming
        url_reader = iter(self.url_iterable)
        
        print(f"Worker {self.worker_id}: Processing {len(self.url_iterable):,} URLs in range (already processed: {self.processed_urls:,})")
        
        # Resume from checkpoint if needed
        start_time = time.time()
        last_report_time = start_time
        
        for url in url_reader:
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
                if term not in self.term_histogram:
                    self.term_histogram[term] = {"count": 0, "domains": set()}
                    self.term_domain_counts[term] = {}
                
                self.term_histogram[term]["count"] += 1
                self.total_terms_found += 1
                
                # Count domain frequency for this term
                if domain_id in self.term_domain_counts[term]:
                    self.term_domain_counts[term][domain_id] += 1
                else:
                    self.term_domain_counts[term][domain_id] = 1
                    self.term_histogram[term]["domains"].add(domain_id)
            
            self.processed_urls += 1
            
            # Flush to disk if we've seen too many terms
            self.flush_count += len(terms)
            if self.flush_count >= self.flush_threshold:
                self._flush_to_disk()
            
            # Print progress occasionally with rate calculation
            current_time = time.time()
            if current_time - last_report_time >= 60:  # Report every minute
                elapsed = current_time - start_time
                rate = self.processed_urls / elapsed if elapsed > 0 else 0
                eta = (len(self.url_iterable) - self.processed_urls) / rate if rate > 0 else 0
                
                print(f"Worker {self.worker_id}: Processed {self.processed_urls:,} URLs "
                      f"({self.processed_urls/len(self.url_iterable)*100:.1f}%), "
                      f"Rate: {rate:.1f} URLs/sec, "
                      f"ETA: {eta/3600:.1f} hours")
                last_report_time = current_time
        
        # Final flush if anything remains
        if self.term_histogram:
            self._flush_to_disk()
        
        # Write worker stats
        stats = {
            "processed_urls": self.processed_urls,
            "unique_domains": unique_domains,
            "total_terms": self.total_terms_found,
            "chunks": self.chunk_count
        }
        
        stats_file = self.worker_dir / "stats.json"
        with open(stats_file, "w") as f:
            json.dump(stats, f)
        
        print(f"Worker {self.worker_id}: Finished processing {self.processed_urls:,} URLs, "
              f"{unique_domains:,} domains, {self.total_terms_found:,} term occurrences")
        
        return self.worker_dir, stats

def merge_single_worker_chunks(worker_dir):
    """
    Merges all chunk_*.pkl files within a single worker directory into one consolidated file.
    Returns the path to the merged file.
    """
    worker_dir = Path(worker_dir)
    worker_id = worker_dir.name  # Assumes directory name is like "worker_X"
    merged_file = worker_dir / "merged_worker_data.pkl"
    temp_merged_file = merged_file.with_suffix('.tmp')
    
    # Check if already merged
    if merged_file.exists():
        print(f"{worker_id}: Already merged. Skipping.")
        return str(merged_file)
        
    print(f"{worker_id}: Merging chunk files...")
    merged_data = {}
    total_chunks = 0
    
    chunk_files = list(worker_dir.glob("chunk_*.pkl"))
    if not chunk_files:
        print(f"{worker_id}: No chunk files found. Skipping merge.")
        # Create an empty merge file to indicate completion
        with open(temp_merged_file, "wb") as f:
            pickle.dump({}, f, protocol=4)
        temp_merged_file.rename(merged_file)
        return str(merged_file)
        
    for i, chunk_file in enumerate(chunk_files):
        total_chunks += 1
        if i % 50 == 0:
             print(f"{worker_id}: Processing chunk {i+1}/{len(chunk_files)}...")
        try:
            with open(chunk_file, "rb") as f:
                chunk_data = pickle.load(f)
                for term, data in chunk_data.items():
                    if term not in merged_data:
                        merged_data[term] = {"count": 0, "domains": {}}
                    
                    merged_data[term]["count"] += data["count"]
                    
                    # Merge domains efficiently
                    for domain, freq in data["domains"].items():
                        if domain in merged_data[term]["domains"]:
                            merged_data[term]["domains"][domain] += freq
                        else:
                            merged_data[term]["domains"][domain] = freq
            # Optional: Delete chunk file after merging if memory is extremely tight
            # os.remove(chunk_file)
        except Exception as e:
            print(f"Warning: Error processing {chunk_file} in {worker_id}: {e}. Skipping this chunk.")
            continue
            
    # Write merged data to temporary file
    print(f"{worker_id}: Writing merged data ({len(merged_data):,} terms) to {merged_file}")
    try:
        with open(temp_merged_file, "wb") as f:
            pickle.dump(merged_data, f, protocol=4)
        # Atomic rename
        temp_merged_file.rename(merged_file)
        print(f"{worker_id}: Merge complete.")
        return str(merged_file)
    except Exception as e:
        print(f"Error writing merged file for {worker_id}: {e}")
        # Clean up temp file if write failed
        if temp_merged_file.exists():
            os.remove(temp_merged_file)
        return None

def process_range(input_file, start_line, end_line, output_dir, worker_id, min_term_length, 
                 flush_threshold, sample_rate=1):
    """Process a specific range of lines from the input file with disk-based storage."""
    print(f"Worker {worker_id}: Processing lines {start_line:,} to {end_line:,} ({end_line - start_line:,} lines)")
    
    # Set up checkpoint file
    checkpoint_file = Path(output_dir) / f"worker_{worker_id}" / "checkpoint.json"
    
    # Create a reader for this range
    url_reader = RangeURLReader(input_file, start_line, end_line, sample_rate)
    
    # Create and run analyzer
    analyzer = DiskBackedTermFrequencyAnalyzer(
        url_reader, 
        output_dir,
        worker_id,
        min_term_length, 
        flush_threshold=flush_threshold,
        checkpoint_file=str(checkpoint_file)
    )
    
    # Run analysis and return the directory with results
    return analyzer.analyze()

def merge_final_worker_outputs(merged_worker_files, output_file, tmp_dir, percentage=0.8, max_domains_per_term=1000,
                        batch_size=250000, resume=False):
    """
    STAGE 2 Merge: Merges results from consolidated worker files.
    Processes in batches to limit memory usage.
    Uses a disk-based approach to handle datasets of any size.
    Includes pre-computation of term-to-worker mapping for efficiency.
    """
    print("Stage 2: Merging consolidated worker results...")
    import gc  # Import garbage collector
    merged_dir = Path(tmp_dir) / "final_merged_chunks" # Use a different subdir for stage 2
    merged_dir.mkdir(exist_ok=True, parents=True)
    
    # Checkpoint file for final merge process
    checkpoint_file = merged_dir / "merge_checkpoint.json"
    processed_batches = set()
    total_term_occurrences = 0
    running_total = 0
    cutoff_reached = False
    
    # --- Checkpoint Loading --- 
    if resume and checkpoint_file.exists():
        print("Found Stage 2 merge checkpoint, attempting to resume...")
        try:
            with open(checkpoint_file, "r") as f:
                checkpoint = json.load(f)
                processed_batches = set(checkpoint.get("processed_batches", []))
                total_term_occurrences = checkpoint.get("total_term_occurrences", 0)
                running_total = checkpoint.get("running_total", 0)
                cutoff_reached = checkpoint.get("cutoff_reached", False)
                print(f"Resuming Stage 2 merge: {len(processed_batches)} batches already processed")
        except Exception as e:
            print(f"Error loading Stage 2 checkpoint: {e}. Checkpoint data might be incomplete.")
            # Decide if we should reset or try to continue? For safety, let's reset affected parts.
            total_term_occurrences = 0 # Force recalculation
            running_total = 0
            # Keep processed_batches if loaded, otherwise it's empty
            processed_batches = processed_batches if 'processed_batches' in locals() else set()

    # --- Calculate Total Term Occurrences (if needed) --- 
    if total_term_occurrences == 0:
        print("Stage 2: Calculating total term occurrences from merged worker files...")
        occurrence_sum = 0 # Use a local variable to avoid modifying checkpoint value yet
        for worker_file_path in merged_worker_files:
            try:
                with open(worker_file_path, "rb") as f:
                    worker_data = pickle.load(f)
                    for term_data in worker_data.values():
                        occurrence_sum += term_data.get("count", 0)
                    del worker_data
                    gc.collect()
            except Exception as e:
                print(f"Warning: Error reading {worker_file_path} for counts: {e}")
                continue
        total_term_occurrences = occurrence_sum # Update the official value
        # Also update checkpoint immediately if calculated
        if checkpoint_file.exists():
            try: # Update existing checkpoint
                with open(checkpoint_file, "r") as f: checkpoint_data = json.load(f)
                checkpoint_data["total_term_occurrences"] = total_term_occurrences
                with open(checkpoint_file, "w") as f: json.dump(checkpoint_data, f)
            except Exception as e:
                 print(f"Warning: Could not update total_term_occurrences in checkpoint: {e}")
        
    
    print(f"Stage 2: Found {total_term_occurrences:,} total term occurrences across all workers")
    threshold_count = total_term_occurrences * percentage
    print(f"Stage 2: Including terms until we reach {threshold_count:,} occurrences ({percentage*100:.1f}%)")
    
    # --- Build Term Index (from merged worker files) --- 
    term_index_file = merged_dir / "term_index.txt"
    if not term_index_file.exists():
        print("Stage 2: Building term index (scanning merged worker files)...")
        all_terms = set()
        for worker_file_path in merged_worker_files:
            print(f"  Scanning {worker_file_path} for terms...")
            try:
                with open(worker_file_path, "rb") as f:
                    worker_data = pickle.load(f)
                    all_terms.update(worker_data.keys())
                    del worker_data
                    gc.collect()
            except Exception as e:
                print(f"Warning: Error processing {worker_file_path} for index: {e}")
                continue
        
        print(f"Stage 2: Writing {len(all_terms):,} unique terms to index file...")
        with open(term_index_file, "w") as f:
            for term in sorted(all_terms):
                f.write(f"{term}\n")
        del all_terms
        gc.collect()
    
    # --- Build or Load Term-to-WorkerFile Map --- 
    term_worker_map_file = merged_dir / "term_worker_map.pkl"
    term_worker_map = {}
    if term_worker_map_file.exists():
        print("Stage 2: Loading existing term-to-worker map...")
        try:
            with open(term_worker_map_file, "rb") as f:
                term_worker_map = pickle.load(f)
            print(f"Loaded map with {len(term_worker_map):,} terms.")
        except Exception as e:
            print(f"Error loading term map: {e}. Rebuilding map.")
            term_worker_map = {}
            term_worker_map_file.unlink(missing_ok=True) # Remove potentially corrupt file
            
    if not term_worker_map:
        print("Stage 2: Building term-to-worker map (scanning merged worker files)...")
        map_build_start = time.time()
        processed_files_for_map = 0
        for worker_file_path in merged_worker_files:
            processed_files_for_map += 1
            print(f"  Scanning {worker_file_path} for map ({processed_files_for_map}/{len(merged_worker_files)})..." )
            try:
                with open(worker_file_path, "rb") as f:
                    worker_data = pickle.load(f)
                    for term in worker_data.keys():
                        if term not in term_worker_map:
                            term_worker_map[term] = []
                        # Store index instead of path for potentially smaller map size?
                        # For simplicity, let's stick with paths for now.
                        term_worker_map[term].append(str(worker_file_path))
                    del worker_data
                    gc.collect()
            except Exception as e:
                print(f"Warning: Error processing {worker_file_path} for map: {e}")
                continue
        
        print(f"Stage 2: Saving term-to-worker map ({len(term_worker_map):,} terms) to {term_worker_map_file}")
        temp_map_file = term_worker_map_file.with_suffix('.tmp')
        try:
            with open(temp_map_file, "wb") as f:
                pickle.dump(term_worker_map, f, protocol=4)
            temp_map_file.rename(term_worker_map_file)
            print(f"Map built in {time.time() - map_build_start:.1f} seconds.")
        except Exception as e:
            print(f"Error saving term map: {e}")
            # Don't delete term_worker_map here, try to proceed without saving

    # --- Count terms and calculate batches --- 
    term_count = 0
    with open(term_index_file, "r") as f:
        for _ in f:
            term_count += 1
    total_batches = (term_count + batch_size - 1) // batch_size
    print(f"Stage 2: Processing {term_count:,} terms in {total_batches} batches of {batch_size} terms each")
    
    # --- Initialize Top Terms File --- 
    top_terms_file = merged_dir / "top_terms.csv"
    # Only recreate header if file is missing or checkpoint is corrupt/missing
    if not top_terms_file.exists() or (resume and not processed_batches and not checkpoint_file.exists()):
        with open(top_terms_file, "w") as f:
            f.write("term,frequency\n")
    
    # --- Process Batches --- 
    # Use a separate start time specifically for batch processing duration
    batch_processing_start_time = time.time() 
    total_time_on_batches_so_far = 0 # Track time spent just on batches before resuming
    
    if processed_batches:
         print(f"Resuming batch processing from batch #{len(processed_batches)}")
         
    batch_idx = 0
    with open(term_index_file, "r") as term_file:
        while True:
            current_batch_start_line = batch_idx * batch_size
            # Calculate line number for seeking if needed (though we read sequentially)
            
            # Check if this batch index was already processed
            if str(batch_idx) in processed_batches:
                batch_idx += 1
                continue # Skip to next batch index check
            
            # Read terms for the *actual* next batch to process
            # Need to correctly skip lines in term_file if resuming
            if batch_idx > 0 and len(processed_batches) == batch_idx: # Resuming and just skipped processed batches
                 print(f"Seeking to start of batch {batch_idx+1} in term index file...")
                 term_file.seek(0) # Reset to beginning
                 for _ in range(current_batch_start_line):
                      term_file.readline() # Read and discard lines to reach start
            
            # Read the terms for this batch
            batch_terms = []
            lines_to_read = min(batch_size, term_count - current_batch_start_line)
            for _ in range(lines_to_read):
                line = term_file.readline()
                if not line: break # End of file
                term = line.strip()
                if term: batch_terms.append(term)
            
            if not batch_terms:
                print("No more terms found in index file.")
                break # No more terms
            
            # --- Start Processing Current Batch --- 
            batch_start_time = time.time()
            print(f"Stage 2: Processing batch {batch_idx+1}/{total_batches} ({len(batch_terms)} terms) [Index: {batch_idx}]")
            
            # --- Identify Relevant Worker Files --- 
            relevant_worker_files = set()
            missing_terms_in_map = 0
            for term in batch_terms:
                if term in term_worker_map:
                    relevant_worker_files.update(term_worker_map[term])
                else:
                    # This shouldn't happen if map was built correctly
                    # print(f"Warning: Term '{term}' not found in term-worker map.") 
                    missing_terms_in_map += 1
            if missing_terms_in_map > 0:
                 print(f"Warning: {missing_terms_in_map} terms in this batch not found in the map.")
            print(f"  Reading {len(relevant_worker_files)} relevant worker files for this batch.")

            # --- Process Batch (reading only relevant worker files) --- 
            batch_results = {term: {"count": 0, "domains": {}} for term in batch_terms}
            files_read_this_batch = 0
            for worker_file_path in relevant_worker_files:
                files_read_this_batch += 1
                # if files_read_this_batch % 10 == 0: # Less verbose now
                #     print(f"  Reading relevant worker file {files_read_this_batch}/{len(relevant_worker_files)}...")
                try:
                    with open(worker_file_path, "rb") as f:
                        worker_data = pickle.load(f)
                        # Only process terms relevant to this batch
                        for term in batch_terms:
                            if term in worker_data:
                                term_data = worker_data[term]
                                batch_results[term]["count"] += term_data["count"]
                                # Merge domains
                                for domain, freq in term_data["domains"].items():
                                    if domain in batch_results[term]["domains"]:
                                        batch_results[term]["domains"][domain] += freq
                                    else:
                                        batch_results[term]["domains"][domain] = freq
                        del worker_data
                except Exception as e:
                    print(f"Warning: Error reading worker data from {worker_file_path}: {e}")
                    continue
                finally:
                    gc.collect() # Clean up memory after each file load
            
            # --- Save Batch Results & Update Top Terms --- 
            batch_file = merged_dir / f"batch_{batch_idx}.pkl"
            temp_batch_file = batch_file.with_suffix('.tmp')
            with open(temp_batch_file, "wb") as f:
                pickle.dump(batch_results, f, protocol=4)
            temp_batch_file.rename(batch_file)
            
            sorted_batch_terms = sorted(
                [(term, data["count"]) for term, data in batch_results.items() if data["count"] > 0],
                key=lambda x: x[1],
                reverse=True
            )
            
            with open(top_terms_file, "a") as f:
                for term, count in sorted_batch_terms:
                    f.write(f"{term},{count}\n")
            
            batch_count_sum = sum(count for _, count in sorted_batch_terms)
            running_total += batch_count_sum
            
            if not cutoff_reached and running_total >= threshold_count:
                cutoff_reached = True
                print(f"Stage 2: Reached {percentage*100:.1f}% threshold at batch {batch_idx+1}")
            
            # --- Update Checkpoint & Cleanup --- 
            processed_batches.add(str(batch_idx))
            with open(checkpoint_file, "w") as f:
                json.dump({
                    "processed_batches": list(processed_batches),
                    "total_term_occurrences": total_term_occurrences,
                    "running_total": running_total,
                    "cutoff_reached": cutoff_reached
                }, f)
            
            del batch_results
            del sorted_batch_terms
            gc.collect()
            
            # --- Report Timing (Revised Calculation) --- 
            batch_time = time.time() - batch_start_time
            total_batches_processed = len(processed_batches)
            # Calculate total time spent *only* on batch processing *in this run*
            total_time_batches_this_run = time.time() - batch_processing_start_time 
            
            # Estimate avg batch time based on this run's batches
            batches_processed_this_run = total_batches_processed - (initial_processed_count if 'initial_processed_count' in locals() else 0) # Need initial count
            if batches_processed_this_run <= 0 and total_batches_processed > 0 : # Handle resuming exactly where we left off
                 batches_processed_this_run = 1 # Avoid division by zero, use current batch time
                 avg_batch_time = batch_time
            elif batches_processed_this_run > 0:
                 avg_batch_time = total_time_batches_this_run / batches_processed_this_run
            else: # First batch ever
                 avg_batch_time = batch_time

            remaining_batches = total_batches - total_batches_processed
            estimated_remaining = remaining_batches * avg_batch_time
            print(f"  Batch completed in {batch_time:.1f}s. Avg batch time (this run): {avg_batch_time:.1f}s. Estimated time remaining: {estimated_remaining/3600:.1f} hours")
            print(f"  Running total: {running_total:,}/{total_term_occurrences:,} term occurrences ({running_total/total_term_occurrences*100:.1f}%)")
            
            batch_idx += 1
            # Need to capture initial count for correct averaging after resume
            if 'initial_processed_count' not in locals(): initial_processed_count = len(processed_batches) -1 

    # --- Free the large map if loaded --- 
    if term_worker_map: del term_worker_map; gc.collect()
    
    # --- Final Output Generation --- 
    print("Stage 2: Creating final sorted output...")
    # Read the top terms file
    term_counts = []
    with open(top_terms_file, "r") as f:
        next(f) # Skip header
        for line in f:
            if line.strip():
                parts = line.strip().split(",", 1)
                if len(parts) == 2:
                    try:
                         term_counts.append((parts[0], int(parts[1])))
                    except ValueError:
                         print(f"Warning: Skipping malformed line in {top_terms_file}: {line.strip()}")
    
    # Sort by frequency
    sorted_terms = sorted(term_counts, key=lambda x: x[1], reverse=True)
    
    # Determine cutoff for top percentage
    print(f"Stage 2: Determining exact cutoff for top {percentage*100:.1f}% of occurrences...")
    running_sum = 0
    cutoff_index = 0
    for i, (_, count) in enumerate(sorted_terms):
        running_sum += count
        if running_sum >= threshold_count:
            cutoff_index = i + 1
            break
    
    top_terms = sorted_terms[:cutoff_index]
    print(f"Stage 2: Selected top {len(top_terms):,} terms (representing {running_sum/total_term_occurrences*100:.1f}% of occurrences)")

    # --- Pre-computation: Map Top Terms to Batch File Locations --- 
    print("Stage 2: Building map of top term locations in batch files...")
    term_location_map = {}
    map_build_start_time = time.time()
    processed_batches_for_location = 0
    top_terms_set = set(term for term, _ in top_terms) # Faster lookups

    for batch_idx in range(total_batches):
        batch_file = merged_dir / f"batch_{batch_idx}.pkl"
        processed_batches_for_location += 1
        if processed_batches_for_location % 50 == 0:
             print(f"  Scanning batch file {processed_batches_for_location}/{total_batches} for term locations...")
             
        if not batch_file.exists(): continue
        try:
            with open(batch_file, "rb") as bf:
                batch_data = pickle.load(bf)
                # Find which of our top terms are in this batch
                for term in batch_data:
                    if term in top_terms_set:
                        if term not in term_location_map:
                             term_location_map[term] = batch_idx # Store batch index
                        # else: # Term appeared in multiple batches? Shouldn't happen with current logic
                        #     print(f"Warning: Term {term} found again in batch {batch_idx}, already mapped to {term_location_map[term]}")
                del batch_data # Free memory
                gc.collect()
        except Exception as e:
            print(f"Warning: Error reading batch {batch_file} while building location map: {e}")
            continue
            
    print(f"Term location map built in {time.time() - map_build_start_time:.1f} seconds. Found locations for {len(term_location_map)}/{len(top_terms)} terms.")
    if len(term_location_map) != len(top_terms):
        print("Warning: Some top terms were not found in any batch file. Output might be incomplete.")
        # Identify missing terms (optional, could be slow)
        # missing = [t for t, _ in top_terms if t not in term_location_map]
        # print(f"Missing terms: {missing[:10]}...")

    # --- Write final results (Optimized Loop) --- 
    print(f"Stage 2: Writing results to {output_file}")
    temp_output = f"{output_file}.tmp"
    with open(temp_output, 'w') as f:
        f.write("term,frequency,domains\n")
        
        batch_cache = {}
        current_batch_idx = -1
        terms_written = 0
        
        for i, (term, term_freq) in enumerate(top_terms):
            if i % 1000 == 0: # Print progress every 1000 terms
                if i > 0:
                     print(f"Writing term {i}/{len(top_terms)}...")
                gc.collect()
            
            found = False
            data = None
            
            # Use the precomputed map to find the batch index
            target_batch_idx = term_location_map.get(term)
            
            if target_batch_idx is not None:
                # Check cache first
                if target_batch_idx == current_batch_idx and term in batch_cache:
                    data = batch_cache[term]
                    found = True
                else:
                    # Load the correct batch file directly
                    batch_file = merged_dir / f"batch_{target_batch_idx}.pkl"
                    if batch_file.exists():
                        try:
                            # print(f"Loading batch {target_batch_idx} for term '{term}'...") # Verbose logging
                            with open(batch_file, "rb") as bf:
                                batch_cache = pickle.load(bf)
                            current_batch_idx = target_batch_idx
                            if term in batch_cache:
                                data = batch_cache[term]
                                found = True
                            else:
                                 print(f"Warning: Term '{term}' expected but not found in its mapped batch file {batch_file}")
                        except Exception as e:
                            print(f"Warning: Error reading mapped batch {batch_file} for term '{term}': {e}")
                            batch_cache = {} # Clear potentially corrupted cache
                            current_batch_idx = -1
                    else:
                         print(f"Warning: Mapped batch file {batch_file} for term '{term}' does not exist.")
            else:
                 print(f"Warning: Term '{term}' not found in precomputed location map.")

            # Write if data was found
            if found and data is not None:
                sorted_domains = sorted(
                    data.get("domains", {}).items(), # Use .get for safety
                    key=lambda x: x[1], 
                    reverse=True
                )[:max_domains_per_term]
                domains_str = ','.join(domain for domain, _ in sorted_domains)
                # Use term_freq from top_terms list, as batch count might be partial theoretically
                f.write(f"{term},{term_freq},\"{domains_str}\"\n") 
                terms_written += 1
            # else: (Already printed warnings above) 
            #    print(f"Warning: Could not find data for term '{term}'. Skipping.")

        print(f"Finished writing. Total terms written: {terms_written}/{len(top_terms)}")
    
    # Rename temp file to final output
    os.rename(temp_output, output_file)
    print(f"Stage 2: Results written to {output_file}")
    
    # Cleanup
    del term_counts, sorted_terms, top_terms, batch_cache, term_location_map
    gc.collect()

def main():
    parser = argparse.ArgumentParser(description="`Memory-Efficient Parallel Term Frequency Analyzer - Two Stage Merge")
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
    parser.add_argument("--flush-threshold", type=int, default=5000000,
                      help="Number of terms to process before flushing to disk (default: 5,000,000)")
    parser.add_argument("--sample-rate", type=int, default=1,
                      help="Process only 1/N of the input lines (default: 1 = process all)")
    parser.add_argument("--max-domains", type=int, default=1000,
                      help="Maximum number of domains to store per term (default: 1000)")
    parser.add_argument("--batch-size", type=int, default=250000,
                      help="Number of terms to process in each batch during merging (default: 250,000)")
    parser.add_argument("--resume", action="store_true",
                      help="Resume from previous run if checkpoint exists")
    parser.add_argument("--keep-temp", action="store_true",
                      help="Keep temporary files after processing")
    parser.add_argument("--tmp-dir", type=str, default="term_analyzer_tmp",
                      help="Directory for temporary files (default: term_analyzer_tmp)")
    parser.add_argument("--stage1-workers", type=int, default=multiprocessing.cpu_count(),
                        help=f"Number of processes for Stage 1 worker chunk merging (default: CPU count)")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input_file):
        print(f"Error: Input file {args.input_file} does not exist")
        sys.exit(1)
    
    start_time = time.time()
    tmp_dir = Path(args.tmp_dir)
    tmp_dir.mkdir(exist_ok=True, parents=True)
    
    # Prevent auto-deletion if resuming
    should_cleanup = not args.keep_temp and not args.resume
    
    with open(tmp_dir / "start_time.txt", "w") as f:
        f.write(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        # --- Initial Worker Processing (Stage 0) --- 
        worker_dirs = [tmp_dir / f"worker_{i}" for i in range(args.workers)]
        skip_worker_processing = False
        
        if args.resume:
            # Check if all worker directories exist and have stats.json
            all_workers_seem_complete = True
            for worker_dir in worker_dirs:
                stats_file = worker_dir / "stats.json"
                if not (worker_dir.exists() and stats_file.exists()):
                    all_workers_seem_complete = False
                    print(f"Worker directory {worker_dir} or its stats file missing. Cannot skip worker processing.")
                    break
            if all_workers_seem_complete:
                print("All worker stats files found. Assuming worker processing is complete.")
                skip_worker_processing = True
        
        if not skip_worker_processing:
            print("--- Starting Worker Processing (Stage 0) ---")
            # Count lines only if needed for worker processing
            total_lines = args.total_lines
            if total_lines is None:
                total_lines = count_lines(args.input_file)
            print(f"Processing {total_lines:,} lines in {args.input_file}")
            ranges = get_line_offsets(args.input_file, total_lines, args.workers)
            
            worker_results = []
            completed_workers = set()
            
            if args.resume:
                # Check actual completed workers if resuming partially
                for worker_id in range(args.workers):
                   worker_dir = tmp_dir / f"worker_{worker_id}"
                   stats_file = worker_dir / "stats.json"
                   if worker_dir.exists() and stats_file.exists():
                       try:
                           with open(stats_file, "r") as f: json.load(f)
                           completed_workers.add(worker_id)
                           print(f"Worker {worker_id} already completed")
                       except Exception as e:
                           print(f"Stats file for worker {worker_id} invalid: {e}")

            workers_to_run = args.workers - len(completed_workers)
            if workers_to_run > 0:
                print(f"Running {workers_to_run} workers...")
                with ProcessPoolExecutor(max_workers=args.workers) as executor:
                    futures = []
                    for worker_id, (start_line, end_line) in enumerate(ranges):
                        if worker_id not in completed_workers:
                            future = executor.submit(
                                process_range, args.input_file, start_line, end_line, tmp_dir,
                                worker_id, args.min_term_length, args.flush_threshold, args.sample_rate
                            )
                            futures.append(future)
                    
                    for future in futures:
                        worker_results.append(future.result())
            else:
                 print("All workers were already complete.")
        else:
            print("--- Skipping Worker Processing (Stage 0) --- ")

        # --- Stage 1: Parallel Worker Chunk Merging --- 
        print("--- Starting Stage 1: Parallel Worker Chunk Merging --- ")
        merged_worker_files = []
        stage1_workers_to_run = []
        
        for worker_dir in worker_dirs:
            merged_file = worker_dir / "merged_worker_data.pkl"
            if not merged_file.exists():
                 stage1_workers_to_run.append(worker_dir)
            else:
                 print(f"Found existing merged file: {merged_file}")
                 merged_worker_files.append(str(merged_file)) # Add existing file
        
        if stage1_workers_to_run:
            print(f"Merging chunks for {len(stage1_workers_to_run)} workers using {args.stage1_workers} processes...")
            with ProcessPoolExecutor(max_workers=args.stage1_workers) as executor:
                # Submit tasks
                future_to_dir = {executor.submit(merge_single_worker_chunks, worker_dir): worker_dir 
                                 for worker_dir in stage1_workers_to_run}
                
                # Collect results
                for future in concurrent.futures.as_completed(future_to_dir):
                    worker_dir = future_to_dir[future]
                    try:
                        result_path = future.result()
                        if result_path:
                            merged_worker_files.append(result_path)
                        else:
                            print(f"Error: Stage 1 merge failed for {worker_dir}")
                            # Decide how to handle failure - exit or continue?
                            # For now, let's try to continue without it
                    except Exception as e:
                        print(f"Error during Stage 1 merge for {worker_dir}: {e}")
        else:
             print("All worker chunks seem to be already merged.")
             
        if len(merged_worker_files) != args.workers:
             print(f"Warning: Expected {args.workers} merged worker files, but found {len(merged_worker_files)}. Final results might be incomplete.")
        
        # --- Stage 2: Final Batched Merge --- 
        print("--- Starting Stage 2: Final Batched Merge --- ")
        merge_final_worker_outputs(
            merged_worker_files, 
            args.output_file, 
            tmp_dir,
            args.percentage,
            args.max_domains,
            args.batch_size,
            args.resume # Pass resume flag to stage 2
        )
        
    finally:
        end_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        duration_hours = (time.time() - start_time) / 3600
        print(f"Script finished at {end_time_str} (Duration: {duration_hours:.2f} hours)")
        with open(tmp_dir / "end_time.txt", "w") as f:
            f.write(f"Ended at: {end_time_str}\n")
            f.write(f"Total duration: {duration_hours:.2f} hours\n")
        
        if should_cleanup:
            print(f"Cleaning up temporary files in {tmp_dir}")
            # Be careful here - maybe list contents before deleting?
            # print(f"Contents of {tmp_dir}: {list(tmp_dir.glob('*'))}")
            try:
                 shutil.rmtree(tmp_dir)
                 print("Temporary directory removed.")
            except Exception as e:
                 print(f"Error removing temporary directory {tmp_dir}: {e}")
        else:
            print(f"Keeping temporary files in {tmp_dir}")
    
    elapsed_time = time.time() - start_time
    print(f"Total processing time: {elapsed_time:.2f} seconds ({elapsed_time/3600:.2f} hours)")

if __name__ == "__main__":
    main() 