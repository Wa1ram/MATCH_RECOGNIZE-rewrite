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

### Quick start
`rewrite.py` contains an inline example query. Running the script generates a rewritten SQL file under `results/`, named like `<dataset>_<pattern>.`

Example pattern in the script:

```
SELECT * FROM Crimes
MATCH_RECOGNIZE (
	ORDER BY datetime
	MEASURES
		R.id AS RID,
		B.id AS BID,
		M.id AS MID,
		COUNT(Z.id) AS GAP
	ONE ROW PER MATCH
	AFTER MATCH SKIP TO NEXT ROW
	PATTERN (R Z B Z M)
	DEFINE
		R AS R.primary_type = 'ROBBERY',
		B AS B.primary_type = 'BATTERY' AND ...,
		M AS M.primary_type = 'MOTOR VEHICLE THEFT' AND ...
)
```

The script chooses a symbol subsequence (e.g., `['R','B']`) and produces:

```
WITH ranges AS (...),
prefilter AS (...)
SELECT * FROM prefilter MATCH_RECOGNIZE (...)
```

### How it works (high level)
1. Parse the MATCH_RECOGNIZE block (PATTERN, DEFINE, ORDER BY).
2. Extract symbol conditions and detect a single window condition (if present).
3. Compute a time range `[t_s, t_e]` based on the chosen symbol subsequence and the window.
4. Build a `ranges` CTE and a `prefilter` CTE using the derived WHERE clauses.
5. Attach the original MATCH_RECOGNIZE to `prefilter`.

### Customize
- Change `symbol_seq` in `rewrite.py` to the symbol subsequence you want to prefilter on.
- Replace the inline `query` with your own MATCH_RECOGNIZE query.

### Limitations
- Currently targets concatenation-only patterns (e.g., `(A B C)`); general decomposition (`decompose_pattern`) is not implemented yet.
- Detects at most one window condition; INTERVAL form or plain numeric are supported.

### Notes
This repository focuses on query construction. It does not execute SQL against a database; generated queries are written to `results/` for inspection or downstream execution.