This repository implements the logical plan rewrite rule as discussed in
High-Performance Row Pattern Recognition Using Joins (Technical Report) by Erkang Zhu, Silu Huang and Surajit Chaudhuri.

For further information please refer to the original work: https://www.microsoft.com/en-us/research/wp-content/uploads/2022/05/match_recognize.pdf.

rewrite.py takes
input: SQL-Query Q using MATCH-RECOGNIZE in the following format + ordered subset of pattern symbols

1 SELECT * FROM Crimes MATCH_RECOGNIZE (
2 ORDER BY datetime
3 MEASURES R.id AS RID, B.id AS BID,M.id AS MID,count(Z.id) AS GAP
4 ONE ROW PER MATCH
5 AFTER MATCH SKIP TO NEXT ROW
6 PATTERN (R Z* B Z* M)
7 DEFINE R AS R.primary_type = 'ROBBERY',
8 B AS B.primary_type = 'BATTERY'
9 AND B.lon BETWEEN R.lon - 0.05 AND R.lon + 0.05
10 AND B.lat BETWEEN R.lat - 0.02 AND R.lat + 0.02,
11 M AS M.primary_type = 'MOTOR VEHICLE THEFT'
12 AND M.lon BETWEEN R.lon - 0.05 AND R.lon + 0.05
13 AND M.lat BETWEEN R.lat - 0.02 AND R.lat + 0.02
14 AND M.datetime - R.datetime <= INTERVAL '30' MINUTE)

output: restructured SQL-Query using a prefilter with the given symbol subset before MATCH-RECOGNIZE

rewrite.py follows the steps:

1. parse PATTERN and DEFINE clauses from Q into a Dict
2. get independent and dependent conditions for given (all) subset(s)
3. check validity of Q (dependent conditions must be self-contained)
4. 