## MATCH_RECOGNIZE rewrite (prefilter)

This project implements a logical rewrite inspired by “High-Performance Row Pattern Recognition Using Joins” (Technical Report) by Erkang Zhu, Silu Huang, and Surajit Chaudhuri.

It extracts constraints from a MATCH_RECOGNIZE query and constructs a prefilter (ranges + prefilter CTEs) on a chosen subset of pattern symbols. The final query runs MATCH_RECOGNIZE only on the prefiltered dataset.

Original paper: https://www.microsoft.com/en-us/research/wp-content/uploads/2022/05/match_recognize.pdf

### Repository layout
- `rewrite.py` — main script: builds ranges/prefilter and assembles a rewritten query.
- `helper/python_trino_parser.py` — utilities to parse MATCH_RECOGNIZE clauses.
- `results/` — generated queries are written here.

### Prerequisites
- Python 3.10+.
- The helper imports `trino_query_parser.parse_statement`. Ensure that module is available in your environment or replace it with your own parser.

### Usage

Run the script with a mode, an input SQL file, and a symbol subsequence:

```
python3 rewrite.py <mode> <input_sql_file> <SYMBOL1> [SYMBOL2 ...]
```

Examples:

- Basic prefilter
	- `python3 rewrite.py basic input/CRIME.sql R B`

- Bucketized prefilter
	- `python3 rewrite.py bucket input/CRIME.sql R B Z M`

Arguments:
- `mode`: `basic` or `bucket`.
- `input_sql_file`: a file containing a single MATCH_RECOGNIZE query.
- `SYMBOL...`: ordered subsequence of pattern symbols used for the prefilter.

Output:
- Rewritten SQL is written to `results/<dataset>_<mode>_<symbols>.sql`.

### How it works (high level)
1. Parse the MATCH_RECOGNIZE block (PATTERN, DEFINE, ORDER BY).
2. Extract symbol conditions and detect a single window condition (if present).
3. Compute a time range `[t_s, t_e]` based on the chosen symbol subsequence and the window.
4. Build a `ranges` CTE and a `prefilter` CTE using the derived WHERE clauses.
5. Attach the original MATCH_RECOGNIZE to `prefilter`.

### Modes
- Basic: builds ranges from ORDER BY and (optional) window; works when the pattern is concatenation-only (e.g., `(A B C)`).
- Bucket: bucketizes input by the window size, then expands candidate buckets; requires a detectable window condition in DEFINE (e.g., `LAST.order_by - FIRST.order_by <= INTERVAL '30' MINUTE` or a plain numeric), otherwise it fails.

### Customize
- Provide your own input file; pass the desired symbol subsequence on the command line.

### Limitations
- Input must contain one MATCH_RECOGNIZE query.
- Currently targets concatenation-only patterns; general decomposition (`decompose_pattern`) is not implemented yet.
- Detects at most one window condition; INTERVAL form or plain numeric are supported.
- Bucket mode requires a window condition; without it, no bucket rewrite is produced.

### Notes
This repository focuses on query construction. It does not execute SQL against a database; generated queries are written to `results/` for inspection or downstream execution.