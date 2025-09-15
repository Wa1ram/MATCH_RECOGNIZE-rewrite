from trino_query_parser import parse_statement
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, TypedDict


class MatchRecognizeClauses(TypedDict, total=False):
    partition: List[str]
    order_by: List[str]
    measures: List[Tuple[str, str]]  # (expression, alias)
    rows_per_match: str
    after_match_skip_to: str
    pattern: Any  # nested list/AST as returned by parser
    define: List[Tuple[str, List[str]]]  # (variable, list of AND-split conditions)


def flatten(nested: Sequence[Any]) -> Iterator[Any]:
    """Yield a flat sequence of elements from a nested list-like structure."""
    for elem in nested:
        if isinstance(elem, list):
            yield from flatten(elem)
        else:
            yield elem

def flatten_def_conds(nested: Any) -> Iterator[str]:
    """Split a DEFINE expression tree on AND and yield formatted condition strings."""
    if isinstance(nested, list) and len(nested) > 1 and nested[1] == 'AND':
        yield from flatten_def_conds(nested[0])
        yield from flatten_def_conds(nested[2])
    else:
        yield format_expr(nested)


def format_expr(expr: Any) -> str:
    """Convert expression structures into readable SQL (e.g. A.price > 100)"""
    tokens = list(flatten([expr]))
    s = " ".join(tokens)
    s = s.replace(" . ", ".").replace("( ", "(").replace(" )", ")")
    s = s.replace(" ,", ",").replace(", ", ", ")
    return s

def format_def_conds(expr: Any) -> str:
    tokens = list(flatten([expr]))
    s = " ".join(tokens)
    s = s.replace(" . ", ".").replace("( ", "(").replace(" )", ")")
    s = s.replace(" ,", ",").replace(", ", ", ")
    return s


def format_pattern(pattern: Sequence[Any]) -> str:
    """Convert a row pattern list (e.g. [['A', ['B', '+']], 'C']) into regex-like syntax"""
    parts = []
    for elem in pattern:
        if isinstance(elem, str):
            parts.append(elem)
        elif isinstance(elem, list):
            # Handle simple quantified variables like ['B', '+']
            if len(elem) == 2 and all(isinstance(x, str) for x in elem):
                parts.append(elem[0] + elem[1])
            else:
                parts.append("(" + format_pattern(elem) + ")")
        else:
            parts.append(str(elem))
    return " ".join(parts)


def find_match_recognize(node: Any) -> Optional[List[Any]]:
    """Depth-first search for the list that contains the literal 'MATCH_RECOGNIZE'."""
    if isinstance(node, list):
        if 'MATCH_RECOGNIZE' in node:
            return node
        for elem in node:
            result = find_match_recognize(elem)
            if result:
                return result
    return None

# Given the tokens after MATCH_RECOGNIZE, extract each clause
def extract_match_recognize(mr_tokens: List[Any]) -> MatchRecognizeClauses:
    """Extract clauses from a MATCH_RECOGNIZE token list.

    Assumes ``mr_tokens`` is the (sub)list that includes the literal
    'MATCH_RECOGNIZE' followed by '(' and the clause content as produced by
    ``parse_statement`` from ``trino_query_parser``.

    Parsed clauses (if present):
      partition : list[str]
      order_by : list[str]
      measures : list[tuple[str, str]]   # (expression, alias)
      rows_per_match : str               # e.g. 'ONE ROW PER MATCH'
      after_match_skip_to : str          # target after AFTER MATCH SKIP TO ...
      pattern : any                      # nested list as produced by ``trino_query_parser``
      define : list[tuple[str, str]]     # (variable, condition expression)

    Parameters
    ----------
    mr_tokens : list
        Token list containing the MATCH_RECOGNIZE construct.

    Returns
    -------
    dict
        Mapping of clause names to their parsed representation.
    """
    idx = mr_tokens.index('MATCH_RECOGNIZE')
    i = idx + 2  # skip 'MATCH_RECOGNIZE' and '('
    clauses: MatchRecognizeClauses = {}
    while i < len(mr_tokens):
        tok = mr_tokens[i]
        # PARTITION BY
        if tok == 'PARTITION':
            i += 2  # skip 'PARTITION', 'BY'
            parts = []
            while i < len(mr_tokens) and mr_tokens[i] not in ('ORDER','MEASURES','ONE','ALL','AFTER','PATTERN','DEFINE',')'):
                part = mr_tokens[i]
                parts.append(format_expr(part) if isinstance(part, dict) else str(part))
                i += 1
            clauses['partition'] = parts
            continue
        # ORDER BY
        if tok == 'ORDER':
            i += 2  # skip 'ORDER', 'BY'
            items = []
            while i < len(mr_tokens) and mr_tokens[i] not in ('MEASURES','ONE','ALL','AFTER','PATTERN','DEFINE',')'):
                item = mr_tokens[i]
                items.append(format_expr(item) if isinstance(item, dict) else str(item))
                i += 1
            clauses['order_by'] = items
            continue
        # MEASURES
        if tok == 'MEASURES':
            i += 1
            measures = []
            while i < len(mr_tokens) and mr_tokens[i] not in ('ONE','ALL','AFTER','PATTERN','DEFINE',')'):
                defn = mr_tokens[i]
                if isinstance(defn, list) and 'AS' in defn:
                    as_idx = defn.index('AS')
                    expr_tokens = defn[:as_idx]
                    alias = defn[as_idx + 1]
                    measures.append((format_expr(expr_tokens), alias))
                i += 1
            clauses['measures'] = measures
            continue
        # ROWS PER MATCH
        if tok in ('ONE','ALL'):
            clauses['rows_per_match'] = ' '.join(mr_tokens[i:i+4])
            i += 4
            # skip any optional empty-match handling keywords
            while i < len(mr_tokens) and mr_tokens[i] not in ('AFTER','PATTERN','DEFINE',')') and not isinstance(mr_tokens[i], list):
                i += 1
            continue
        # AFTER MATCH SKIP TO
        if tok == 'AFTER':
            i += 2  # skip 'AFTER', 'MATCH'
            skip = mr_tokens[i]
            if isinstance(skip, list):
                clauses['after_match_skip_to'] = ' '.join(str(x) if not isinstance(x, dict) else format_expr(x) for x in skip)
            else:
                clauses['after_match_skip_to'] = skip
            i += 1
            continue
        # PATTERN
        if tok == 'PATTERN':
            i += 2  # skip 'PATTERN', '('
            pattern_tokens = mr_tokens[i]
            clauses['pattern'] = pattern_tokens
            i += 2  # skip pattern and ')'
            continue
        # DEFINE
        if tok == 'DEFINE':
            i += 1
            defs = []
            while i < len(mr_tokens) and mr_tokens[i] != ')':
                defn = mr_tokens[i]
                if isinstance(defn, list) and len(defn) >= 3 and defn[1] == 'AS':
                    var = defn[0]
                    expr_tokens = defn[2]
                    defs.append((var, list(flatten_def_conds(expr_tokens))))
                i += 1
            clauses['define'] = defs
            i += 1  # skip ')'
            continue
        i += 1
    return clauses

def sql_to_clauses(query: str) -> Tuple[str, MatchRecognizeClauses]:
    """Parse SQL and return (dataset, MATCH_RECOGNIZE clauses).

    Parameters
    ----------
    query : str
        SQL text containing a MATCH_RECOGNIZE clause.

    Returns
    -------
    tuple[str, dict]
        Dataset name and clause dictionary as produced by ``extract_match_recognize``.
        Raises ValueError if no MATCH_RECOGNIZE is present.
    """
    tokens = parse_statement(query)
    mr_tokens = find_match_recognize(tokens)
    if not mr_tokens:
        raise ValueError("No MATCH_RECOGNIZE clause found in SQL query")
    dataset = mr_tokens[0]
    return dataset, extract_match_recognize(mr_tokens)


if __name__ == "__main__":
    import sys

    queries = [
        """
        WITH ranges AS (
            SELECT A.TS as t_s, A.TS + INTERVAL '7' DAY as t_e
            FROM PRICES AS A, PRICES AS B
            WHERE A.TS <= B.TS
            AND B.CLOSE > A.CLOSE
            AND B.TS - A.TS <= INTERVAL '7' DAY
            AND B.TS - A.TS <= INTERVAL '7' DAY
        ),
        prefilter AS (
            SELECT DISTINCT PRICES.* FROM PRICES, ranges AS r
            WHERE TS BETWEEN r.t_s AND r.t_e
        )
        SELECT * FROM prefilter MATCH_RECOGNIZE (
        PARTITION BY symbol
        ORDER BY ts
        MEASURES
            FIRST(UP.ts) AS start_ts,
            LAST(UP.ts)  AS end_ts
        ONE ROW PER MATCH
        AFTER MATCH SKIP PAST LAST ROW
        PATTERN (((a+ b?) | (C{2,3})) D* PERMUTE(E, F{2}))
        DEFINE
            B AS B.close > A.close
            AND B.ts - A.ts <= INTERVAL '7' DAY,   -- Window condition
            C AS C.close > B.close
            AND C.ts - A.ts <= INTERVAL '7' DAY    -- Window condition vs. A
        )""",
        """
        SELECT * FROM Crimes
        MATCH_RECOGNIZE (
            ORDER BY datetime
            MEASURES
                R.id        AS RID,
                B.id        AS BID,
                M.id        AS MID,
                COUNT(Z.id) AS GAP
            ONE ROW PER MATCH
            AFTER MATCH SKIP TO NEXT ROW
            PATTERN (R{1,5} PERMUTE(Z*, M) (B | Z)? M+)
            DEFINE
                R AS R.primary_type = 'ROBBERY',
                B AS  B.primary_type = 'BATTERY'
                     AND B.lon BETWEEN R.lon - 0.05 AND R.lon + 0.05
                     AND B.lat BETWEEN R.lat - 0.02 AND R.lat + 0.02,
                M AS  M.primary_type = 'MOTOR VEHICLE THEFT'
                     AND M.lon BETWEEN R.lon - 0.05 AND R.lon + 0.05
                     AND M.lat BETWEEN R.lat - 0.02 AND R.lat + 0.02
                     AND M.datetime - R.datetime <= INTERVAL '30' MINUTE
        ) AS mr
        """,

        """
        SELECT *
        FROM sensor_readings
        MATCH_RECOGNIZE (
            PARTITION BY device_id
            ORDER BY event_time
            MEASURES
                FIRST(B.event_time) AS burst_start,
                LAST(B.event_time)  AS burst_end,
                COUNT(B.*)          AS burst_points,
                FIRST(L.event_time) AS lull_start,
                LAST(L.event_time)  AS lull_end
            ALL ROWS PER MATCH SHOW EMPTY MATCHES
            AFTER MATCH SKIP PAST LAST ROW
            PATTERN (B{2,} L{3,})
            SUBSET
                BURSTS = (B),
                LULLS  = (L)
            DEFINE
                B AS power > 200,
                L AS power < 50
        ) AS bursts_lulls
        """,


        """
        SELECT *
        FROM stock_ticks
        MATCH_RECOGNIZE (
            PARTITION BY symbol
            ORDER BY ts
            MEASURES
                FIRST(A.price) AS left_peak,
                LAST(C.price)  AS right_peak,
                AVG(B.price)   AS bottom_avg
            ALL ROWS PER MATCH WITH UNMATCHED ROWS
            AFTER MATCH SKIP TO FIRST C
            PATTERN (A+ B{1,5} C+)
            DEFINE
                A AS price > PREV(price),
                B AS price < PREV(price),
                C AS price > PREV(price)
        ) AS v_shapes
        """,


        """
        SELECT *
        FROM credit_card_tx
        MATCH_RECOGNIZE (
            PARTITION BY cardholder_id
            ORDER BY tx_time
            MEASURES
                FIRST(L.tx_time) AS first_low_value,
                LAST(H.tx_time)  AS high_value_time,
                MAX(H.amount)    AS max_amount,
                COUNT(*)         AS tx_count
            AFTER MATCH SKIP TO NEXT ROW
            PATTERN (L{3,5} H{1,2})
            DEFINE
                L AS amount < 100,
                H AS amount > 1000
        ) AS fraud_ring_candidates
        """,


        """
        SELECT *
        FROM application_logs
        MATCH_RECOGNIZE (
            ORDER BY log_time
            MEASURES
                COUNT(S.error_code) AS spike_errors,
                MAX(C.log_time)     AS cooldown_end
            ONE ROW PER MATCH
            AFTER MATCH SKIP PAST LAST ROW
            PATTERN (S{5,} PERMUTE(C?))
            SUBSET
                ALL_SPIKE = (S, C)
            DEFINE
                S AS log_level = 'ERROR',
                C AS log_level <> 'ERROR'
                     AND log_time - LAST(S.log_time) >= INTERVAL '1' HOUR
        ) AS error_spikes
        """,


        """
        SELECT *
        FROM machine_metrics
        MATCH_RECOGNIZE (
            PARTITION BY factory_id, machine_id
            ORDER BY sample_time
            MEASURES
                FIRST(A.sample_time) AS start_time,
                LAST(X.sample_time)  AS failure_time,
                AVG(B.temperature)   AS avg_overheat,
                COUNT(C.*)           AS vibration_spikes
            ONE ROW PER MATCH
            AFTER MATCH SKIP TO LAST X
            PATTERN (A B C X)
            DEFINE
                A AS status = 'RUNNING',
                B AS temperature > 90,
                C AS vibration   > 1.0,
                X AS status = 'FAILED'
        ) AS failure_precursors
        """
    ]


    sql = queries[0]
    tokens = parse_statement(sql)

    # Find the MATCH_RECOGNIZE keyword in the returned token structure
    mr_tokens = find_match_recognize(tokens)
    if not mr_tokens:
        print("No MATCH_RECOGNIZE clause found.")
        sys.exit(0)

    clauses = extract_match_recognize(mr_tokens)

    print(f"Query:\n{sql}\n")
    print("MATCH_RECOGNIZE clauses:")
    if 'partition' in clauses:
        print("PARTITION BY: " + ", ".join(clauses['partition']))
    if 'order_by' in clauses:
        print("ORDER BY: " + ", ".join(clauses['order_by']))
    if 'measures' in clauses:
        print("MEASURES:")
        for expr, alias in clauses['measures']:
            print(f"  {expr} AS {alias}")
    if 'rows_per_match' in clauses:
        print("ROWS PER MATCH: " + clauses['rows_per_match'])
    if 'after_match_skip_to' in clauses:
        print("AFTER MATCH SKIP TO: " + clauses['after_match_skip_to'])
    if 'pattern' in clauses:
        print("PATTERN: " + str(clauses['pattern']))
    if 'define' in clauses:
        print("DEFINE:")
        for var, expr in clauses['define']:
            print(f"  {var} AS {expr}")
            print()
            print()
[[['(', [['(', [['A', '*'], ['B', '+']], ')'], '|', ['D', ['{', ',', '4', '}']]], ')'], '?'], ['C', ['{', '3', ',', '5', '}']]]
[[[['(', [['A', ['B', '+']], '|', ['C', ['{', '2', ',', '4', '}']]], ')'], '?'], ['D', '*']], ['PERMUTE', '(', 'E', ',', ['F', ['G', ['{', '3', '}']]], ')']]
[['A', 'B'], 'C']
[['A', [['(', ['B', '+'], ')'], '?']], 'C']