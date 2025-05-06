# Common Crawl Data Extractor

Tools to extract and analyze domains and URLs from Common Crawl data files.

## Requirements

- Python 3.7+
- Required packages (install with `pip install -r requirements.txt`):
  - pyarrow
  - pandas
  - requests
  - tqdm
  - nltk
  - tldextract

## Features

- Extracts domains or URLs from Common Crawl index files
- Normalizes domains to their registered form
- Analyzes term frequency in URLs
- Processes data efficiently with parallel workers
- Memory-efficient processing (doesn't keep all data in memory)
- Optimized for processing billions of URLs/domains

## Usage

### Extract Domains

To extract all domains from Common Crawl data:

```bash
python extract_domains.py [options]
```

Options:
- `--index-url`: URL to the index paths file (default: CC-MAIN-2025-13 columnar index)
- `--output`: Output file for domains (default: domains.txt)
- `--limit-files`: Limit the number of files to process (for testing)
- `--limit-rows`: Limit the number of rows per file (for testing)
- `--base-url`: Base URL for Common Crawl data (default: https://data.commoncrawl.org/)
- `--workers`: Number of worker threads for parallel processing (default: 4)

### Extract URLs

To extract all URLs from Common Crawl data:

```bash
python extract_urls.py [options]
```

Options are the same as for `extract_domains.py` but the default output file is `urls.txt`.

### Normalize Domains

To normalize domain variations (e.g., www.example.com, blog.example.com) to their registered domain (example.com):

```bash
python normalize_domains.py domains.txt normalized-domains.txt
```

The normalization script:
- Processes domains in a streaming fashion, requiring minimal memory
- Works efficiently with sorted domain files
- Uses tldextract to properly handle all TLDs

### Analyze Term Frequency

To analyze term frequency in URLs and associate terms with domains, choose from three options based on dataset size:

#### 1. Standard Analyzer (for small datasets)

```bash
python term_frequency_analyzer.py urls.txt term-frequency.csv
```

#### 2. Parallel In-Memory Analyzer (for medium datasets)

```bash
python parallel_term_analyzer.py urls.txt term-frequency.csv --workers 12 --total-lines 3500000000
```

#### 3. Disk-Based Analyzer (for very large datasets)

This analyzer, `parallel_term_analyzer_disk.py`, is engineered for robustly processing extremely large datasets (e.g., billions of URLs) even with limited memory. It employs a multi-stage, disk-based architecture:

**Overall Process:**

1.  **Stage 0: Parallel URL Processing & Initial Term Aggregation**
    *   The input URL file is divided among a number of primary worker processes (configurable with `--workers`).
    *   Each primary worker independently reads its assigned URL chunks, extracts terms, performs stemming, and filters out stopwords.
    *   Term counts are accumulated in memory by each primary worker.
    *   Periodically (based on `--flush-threshold`, which defines the number of unique terms a worker processes before writing to disk), or when a primary worker completes its input, it flushes its aggregated term counts to one or more temporary chunk files within its dedicated worker directory (e.g., `tmp_dir/worker_N/chunk_X.pkl`). This manages individual worker memory.

2.  **Stage 1: Parallel Merging of Individual Worker's Chunks**
    *   After Stage 0 workers complete, a set of Stage 1 workers (configurable with `--stage1-workers`) processes the chunk files produced by *each* Stage 0 worker.
    *   Each Stage 1 worker is responsible for merging all chunk files *within a single Stage 0 worker's directory* into one consolidated temporary file (e.g., `tmp_dir/worker_N/merged_worker_data.pkl`). This step runs in parallel for different Stage 0 worker directories.

3.  **Stage 2: Final Batched Merge & Output Generation**
    *   This stage takes all the `merged_worker_data.pkl` files (one from each Stage 0 worker, consolidated in Stage 1).
    *   It merges these files in a memory-efficient, batched manner (controlled by `--batch-size`) to produce the final term frequency data.
    *   This involves creating intermediate batch files (e.g., `tmp_dir/final_merged_chunks/batch_Y.pkl`) before producing the final CSV.
    *   The top N domains (as per `--max-domains`) are selected for each term during this finalization.

Example usage:
```bash
python parallel_term_analyzer_disk.py urls.txt term-frequency.csv --workers 8 --stage1-workers 4 --total-lines 3500000000 --flush-threshold 1000000 --batch-size 20000 --tmp-dir ./my_temp_data
```

Options for the disk-based analyzer:
- `--workers`: Number of primary worker processes for Stage 0 (initial URL processing and term extraction) (default: number of CPU cores).
- `--stage1-workers`: Number of worker processes for Stage 1 (merging chunk files within each Stage 0 worker's directory) (default: number of CPU cores).
- `--min-term-length`: Minimum length of terms to consider (default: 3).
- `--percentage`: Percentage of total term occurrences to include in the final output (default: 0.8, applied during Stage 2).
- `--total-lines`: Estimated total number of lines in the input file. Providing this skips an initial line count, saving time.
- `--flush-threshold`: Number of unique terms a Stage 0 worker processes before flushing its in-memory counts to a temporary disk chunk file (default: 5,000,000). Lowering this reduces Stage 0 worker memory but increases I/O and the number of chunk files.
- `--sample-rate`: Process only 1/N of the input lines during Stage 0 (default: 1 = process all lines). Useful for quick testing or generating sample outputs.
- `--max-domains`: Maximum number of unique domains to store and list per term in the final output (default: 1000, applied during Stage 2).
- `--batch-size`: Number of unique terms to load into memory and process in each batch during the final merging phase (Stage 2) (default: 250,000). Adjust based on available memory during this stage.
- `--resume`: If set, the script attempts to resume an interrupted run. It checks for existing temporary files from Stage 0 workers, Stage 1 merged files, and Stage 2 batch files to continue processing from the last known good state.
- `--keep-temp`: If set, temporary files created during Stage 0 (worker chunk files), Stage 1 (merged worker data), and Stage 2 (final merged batch files) will not be deleted upon script completion or interruption. Useful for debugging.
- `--tmp-dir`: Directory to use for storing all temporary files (default: `term_analyzer_tmp`).

All term frequency analyzers:
- Extract meaningful terms from URLs
- Stem terms to normalize related words (e.g., "running" → "run")
- Filter out common stopwords and patterns
- Associate terms with their top 1000 domains by frequency
- Output a CSV with terms, frequencies, and associated domains

## Testing with a Small Subset

To test with a small subset of the data before running on the full dataset:

```bash
# Test with 2 files, 100 rows each
python extract_domains.py --limit-files 2 --limit-rows 100

# Test with 2 files, 100 rows each
python extract_urls.py --limit-files 2 --limit-rows 100
```

## Running on the Full Dataset

To run on the full dataset (approximately 3.5 billion URLs):

```bash
# Extract all domains
python extract_domains.py --workers 8

# Extract all URLs (will create a very large file)
python extract_urls.py --workers 8

# Normalize all domains
python normalize_domains.py domains.txt normalized-domains.txt

# Analyze term frequency (disk-based with 12 workers)
python parallel_term_analyzer_disk.py urls.txt term-frequency.csv --workers 12 --stage1-workers 6 --total-lines 3500000000 --flush-threshold 1000000 --batch-size 20000
```

Note: Processing the full dataset will require significant time.

## Memory Management for Large Datasets

For the most efficient processing of very large datasets using `parallel_term_analyzer_disk.py`:

1.  **Understand the Multi-Stage Process & Worker Types**:
    *   **Stage 0 (URL Processing)**: Uses `--workers`. Memory per worker is influenced by `--flush-threshold`.
    *   **Stage 1 (Worker Chunk Merging)**: Uses `--stage1-workers`. Each of these merges chunks from *one* Stage 0 worker. Memory here depends on the size of a single Stage 0 worker's total output before it's consolidated.
    *   **Stage 2 (Final Merge)**: Primarily single-threaded but uses `--batch-size` to manage memory while merging all consolidated worker outputs.
2.  **Tune Stage 0 Worker Memory (`--flush-threshold`)**: If Stage 0 workers consume too much memory, lower the `--flush-threshold`. This results in more frequent disk writes by each Stage 0 worker and more (smaller) chunk files for Stage 1 to process.
3.  **Tune Stage 1 Worker Configuration (`--stage1-workers`)**: The number of `--stage1-workers` can be adjusted. Since each Stage 1 worker handles one Stage 0 worker's output, ensure you have enough memory for `stage1-workers` number of concurrent merge operations on that scale of data.
4.  **Tune Stage 2 Merge Memory (`--batch-size`)**: If the final merging process (Stage 2) runs out of memory, reduce the `--batch-size`. This processes smaller sets of unique terms at a time during the final consolidation.
5.  **Utilize Resumability (`--resume`)**: For very long runs, use the `--resume` flag. It can save significant reprocessing by picking up from completed parts of Stage 0, Stage 1, or Stage 2.
6.  **Leverage Sampling (`--sample-rate`)**: For initial testing or quick insights, use `--sample-rate` to process a fraction of the input in Stage 0.
7.  **Keep Temporary Files for Debugging (`--keep-temp`)**: If issues arise, `--keep-temp` allows inspection of intermediate files from all stages (`worker_N/chunk_X.pkl`, `worker_N/merged_worker_data.pkl`, `final_merged_chunks/batch_Y.pkl`).

## Data Directory

The script creates a `data` directory to store downloaded files temporarily. All downloaded files are automatically deleted after they are processed to save disk space.

## Note on Deduplication

The extraction scripts do not perform deduplication to ensure they can handle billions of URLs efficiently. If you need unique URLs, you can use external tools like GNU sort to deduplicate after extraction:

```bash
# Deduplicate URLs
sort -u urls.txt -o urls_unique.txt
```

Note that the domain normalization script automatically deduplicates domains as part of its process. 