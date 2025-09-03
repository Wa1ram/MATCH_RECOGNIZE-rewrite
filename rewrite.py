import os
from helper.python_trino_parser import sql_to_clauses
import re
import sys
import textwrap
from typing import Any, Iterable, Iterator, Sequence, List, Tuple, Set, Optional, Pattern, Union, TypedDict

NestedStrList = Sequence[Union[str, 'NestedStrList']]

class MatchRecognizeClauses(TypedDict, total=False):
    partition: List[str]
    order_by: List[str]
    measures: List[Tuple[str, str]]  # (expression, alias)
    rows_per_match: str
    after_match_skip_to: str
    pattern: Any  # nested list/AST as returned by parser
    define: List[Tuple[str, List[str]]]  # (variable, list of AND-split conditions)


def flatten(lst: Iterable[Any]) -> Iterator[Any]:
    # Recursively flatten a nested list
    for el in lst:
        if isinstance(el, list):
            yield from flatten(el)
        else:
            yield el


def extract_pattern_symbols(
    pattern: NestedStrList,
    define: Sequence[Tuple[str, Sequence[str]]],
) -> Tuple[Set[str], Set[str]]:
    all_sym = set(el for el in flatten(pattern) if el.isalpha() and el != 'PERMUTE')
    def_sym = set(clause[0] for clause in define)
    return all_sym, def_sym

    
def decompose_pattern(pattern: NestedStrList) -> List[Sequence[str]]:
    pass    
    
    
def build_window_condition_regex(first_sym: str, last_sym: str, order_by: str) -> Pattern[str]:
    """
    Compile a regex matching '<last>.<order_by> - <first>.<order_by> <= <window>'.
    Supports INTERVAL 'n' UNIT or plain 'n UNIT'; captures the window as group 'window'.
    """
    pattern_str = rf"""
        ^\s*
        ({last_sym})\.{re.escape(order_by)}
        \s*-\s*
        ({first_sym})\.{re.escape(order_by)}
        \s*<=\s*
        (?P<window>
            (?:INTERVAL\s+['"]?-?\d+(?:\.\d+)?['"]?\s+\w+)
            |
            (?:-?\d+(?:\.\d+)?\s*\w*)
        )
        \s*$
    """
    return re.compile(pattern_str, re.VERBOSE | re.IGNORECASE)


def add_symbol_prefix(cond: str, symbol: str) -> str:
    """
    Add a symbol prefix (e.g., A.) to all column names in a MATCH_RECOGNIZE DEFINE condition.
    Does not add prefix to SQL keywords, numeric literals, or function names.
    
    Parameters:
        cond (str): The condition string from DEFINE.
        symbol (str): The symbol to prefix column names with (e.g., 'A').
    
    Returns:
        str: The condition with prefixed column names.
    """
    # SQL keywords to ignore
    sql_keywords = {
        "AND", "OR", "NOT", "NULL", "IS", "IN", "LIKE", "BETWEEN",
        "CASE", "WHEN", "THEN", "ELSE", "END", "TRUE", "FALSE"
    }

    # Only add prefix if symbol is not already present
    if not re.search(rf"\b{re.escape(symbol)}\.", cond):
        def add_prefix(match):
            word = match.group(0)
            # Skip SQL keywords
            if word.upper() in sql_keywords:
                return word
            # Skip numeric literals
            if re.fullmatch(r"\d+(\.\d+)?", word):
                return word
            # Skip function names (directly followed by '(')
            start = match.end()
            if start < len(cond) and cond[start] == '(':
                return word
            # Otherwise, add symbol prefix
            return f"{symbol}.{word}"

        cond = re.sub(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", add_prefix, cond)

    return cond


def map_symbols_to_conds(
    pattern: Sequence[str],
    define: Sequence[Tuple[str, Sequence[str]]],
    order_by: str,
    symbol_seq: Sequence[str],
    rewrite: str = "basic"
) -> Tuple[List[str], Optional[str]]:
    """
    Build WHERE conditions for a chosen symbol sequence and extract the (single) window condition.
    """
    conds = []
    
    # TODO rename duplicate events
    # symbol_seq must be ordered upfront if pattern contains duplicates
    if len(pattern) == len(set(pattern)): # contains no duplicates
        symbol_seq = sorted(symbol_seq, key=pattern.index)
    
    if rewrite == "basic":
        # sequential conditions
        for sym1, sym2 in zip(symbol_seq[:-1], symbol_seq[1:]):
            conds.append(f"{sym1}.{order_by} <= {sym2}.{order_by}")
        
    # creates dict from list of (sym, def_conds) tuples
    define_dict = dict(define)
    
    window_pattern = build_window_condition_regex(pattern[0], pattern[-1], order_by)
    window_cond = None
    window: Optional[str] = None
    non_seq_syms = set(pattern).difference(symbol_seq)
    
    # you can express window funcs through adjusting the pattern
    # eg. change    PATTERN (A)     DEFINE A AS price > PREV(price)
    # to            PATTERN (X A)   DEFINE A AS A.price > X.price
    win_func_pattern = re.compile(r'\b(PREV|NEXT|FIRST|LAST|LAG|LEAD|FIRST_VALUE|LAST_VALUE)\b')
    
    # filter symbol conditions
    for sym, cond_list in define_dict.items():
        new_cond_list = []
        for cond in cond_list:
            # get pattern window condition (can only be one; TODO more window conditions)
            m = window_pattern.match(cond)
            if m:
                window_cond = cond
                window = m.group("window")
                continue
            
            # remove all conds that include symbols which are not in symbolseq
            if any(f"{non_seq_sym}." in cond for non_seq_sym in non_seq_syms):
                continue
            
            # window functions are not allowed due to difficult translation (TODO)
            # also ensures that the conditions are self contained
            if win_func_pattern.search(cond):
                continue
            
            # TODO according to trino docs: a non-prefixed column name refers to all rows of the current match.
            # converts A AS price > cost to A AS A.price > A.cost
            cond = add_symbol_prefix(cond, sym)
            
            new_cond_list.append(cond)
        if sym in symbol_seq:
            conds += new_cond_list
    
    if window_cond:
        # window condition can be “propagated backward” through sequential conditions (Proposition 3.1)
        window_cond = window_cond.replace(f"{pattern[0]}.", f"{symbol_seq[0]}.").replace(f"{pattern[-1]}.", f"{symbol_seq[-1]}.")
        conds.append(window_cond)
    return conds, window


def extract_full_mr(query: str) -> str:
    """Extract the content inside MATCH_RECOGNIZE (...)."""
    pattern = r"""
    MATCH_RECOGNIZE       # match the keyword MATCH_RECOGNIZE
    \s*                   # match optional whitespace
    \(                    # match opening parenthesis
    (.*?)                 # capture group 1: lazy match everything inside parentheses
    \)                    # match closing parenthesis
    (?:                   # non-capturing group for optional alias
        \s+AS\s+          # match ' AS ' with whitespace around
        (\w+)             # capture group 2: alias name (alphanumeric + underscore)
    )?                    # make the alias part optional
    \s*                   # match optional whitespace
    (?:;|$)               # match semicolon or end of string
    """
    regex = re.compile(pattern, re.IGNORECASE | re.DOTALL | re.VERBOSE)
    match = regex.search(query)

    if match:
        return match.group(1).strip()
    print("No MATCH_RECOGNIZE clause found.")
    sys.exit(0)


#####################################################
# ************************************************* #
#                                                   #
#                 BASIC PREFILTER                   #
#                                                   #
# ************************************************* #
#####################################################

def get_basic_time_range(
    symbol_seq: Sequence[str],
    pattern: Sequence[str],
    order_by: str,
    window: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    # function from DEFINTION 3.7 (TODO might be incorrect for duplicates)
    t_s = t_e = None
    if symbol_seq[0] == pattern[0] and symbol_seq[-1] == pattern[-1]:
        t_s = f"{symbol_seq[0]}.{order_by}"
        t_e = f"{symbol_seq[-1]}.{order_by}"
    elif window and symbol_seq[-1] == pattern[-1]:
        t_s = f"{symbol_seq[-1]}.{order_by} - {window}"
        t_e = f"{symbol_seq[-1]}.{order_by}"
    elif window and symbol_seq[0] == pattern[0]:
        t_s = f"{symbol_seq[0]}.{order_by}"
        t_e = f"{symbol_seq[0]}.{order_by} + {window}"
    elif window:
        t_s = f"{symbol_seq[-1]}.{order_by} - {window}"
        t_e = f"{symbol_seq[0]}.{order_by} + {window}"
    return t_s, t_e


def build_basic_ranges(
    pattern: Sequence[str],
    symbol_seq: Sequence[str],
    order_by: str,
    dataset_name: str,
    conds: Sequence[str],
    window: Optional[str],
) -> str:
    """Create the ranges CTE based on a chosen symbol sequence and window."""
    t_s, t_e = get_basic_time_range(symbol_seq, pattern, order_by, window)
    if t_s is None or t_e is None:
        raise ValueError("Cannot compute time range: insufficient endpoints or window")
    
    sym_join = ", ".join(f"{dataset_name} AS {sym}" for sym in symbol_seq)
    cond_str = "\n\t\tAND ".join(conds)
    
    ranges = "\n".join([
            "WITH ranges AS (",
            f"\tSELECT {t_s} as t_s, {t_e} as t_e",
            f"\tFROM {sym_join}",
            f"\tWHERE {cond_str}"
            "\n),"])
            
    return ranges


def build_basic_prefilter(dataset_name: str, order_by: str) -> str:
    prefilter = textwrap.dedent(f"""
        prefilter AS (
            SELECT DISTINCT {dataset_name}.* FROM {dataset_name}, ranges AS r
            WHERE {order_by} BETWEEN r.t_s AND r.t_e
        )
        """)
    return prefilter


def build_basic_query(ranges: str, prefilter: str, full_mr: str) -> str:
    final = f"SELECT * FROM prefilter MATCH_RECOGNIZE (\n\t{full_mr}\n)"
    query = "".join([ranges, prefilter, final])
    return query


def rewrite_basic(
    clauses: MatchRecognizeClauses,
    symbol_seq: List[str],
    dataset_name: str,
    full_mr: str
    ) -> str:
    
    pattern_literals = list(flatten(clauses['pattern']))
    pattern = "".join(pattern_literals)
    
    # special case: pattern only has Concatenation operator, e.g., (A B C D E)
    if pattern.isalpha():
        conds, window = map_symbols_to_conds(
            pattern_literals,
            clauses['define'],
            clauses['order_by'][0],
            symbol_seq,
        )
        
        ranges: str = build_basic_ranges(pattern_literals, symbol_seq, clauses['order_by'][0], dataset_name, conds, window)
        prefilter: str = build_basic_prefilter(dataset_name, clauses['order_by'][0])
        new_query: str = build_basic_query(ranges, prefilter, full_mr)
        
        
    else:   
        # decompose general pattern to special cases
        special_patterns = decompose_pattern(clauses['pattern'])
        for pattern in special_patterns:
            map_symbols_to_conds(pattern, clauses['define'], clauses['order_by'], symbol_seq)
            
    return new_query


#####################################################
# ************************************************* #
#                                                   #
#              BUCKETIZED PREFILTER                 #
#                                                   #
# ************************************************* #
#####################################################

def build_input_bucketized(
    dataset_name: str,
    order_by: str,
    window: str
    ) -> str:
    
    bucketized_input = "\n".join([
            "WITH input_bucketized AS (",
            f"\tSELECT *, cast({order_by} / {window} AS bigint) AS bk",
            f"\tFROM {dataset_name}",
            "),\n"])
    
    return bucketized_input

def get_bucket_time_range(
    symbol_seq: Sequence[str],
    pattern: Sequence[str]
) -> Tuple[str, str]:
    # function from DEFINTION 3.10
    bk_s = bk_e = None
    if symbol_seq[0] == pattern[0] and symbol_seq[-1] == pattern[-1]:
        bk_s = f"{symbol_seq[0]}.bk"
        bk_e = f"{symbol_seq[-1]}.bk"
    elif symbol_seq[-1] == pattern[-1]:
        bk_s = f"{symbol_seq[-1]}.bk - 1"
        bk_e = f"{symbol_seq[-1]}.bk"
    elif symbol_seq[0] == pattern[0]:
        bk_s = f"{symbol_seq[0]}.bk"
        bk_e = f"{symbol_seq[0]}.bk + 1"
    else:
        bk_s = f"{symbol_seq[-1]}.bk - 1"
        bk_e = f"{symbol_seq[0]}.bk + 1"
    return bk_s, bk_e


def build_bucketized_ranges(pattern: Sequence[str], symbol_seq: List[str], conds: List[str]) -> str:
    
    bk_s, bk_e = get_bucket_time_range(symbol_seq, pattern)
    
    sym_join = ", ".join(f"input_bucketized AS {sym}" for sym in symbol_seq)
    
    # all events in the same bucket
    cond_same_bucket = [f"{symbol_seq[0]}.bk = {sym}.bk" for sym in symbol_seq[1:]]
    bucketized_range1 = "\n".join([
            "ranges AS (",
            f"\tSELECT {bk_s} as bk_s, {bk_e} as bk_e",
            f"\tFROM {sym_join}",
            f"\tWHERE {"\n\t\tAND ".join(cond_same_bucket + conds)}"])
    
    # no union necessary for single event
    if len(symbol_seq) == 1:
        return bucketized_range1 + "\n),"
    
    # events can be spread between two consecutive buckets
    cond_two_buckets = [f"{symbol_seq[0]}.bk + 1 = {symbol_seq[-1]}.bk"]
    if len(symbol_seq) > 2:
        for sym1, sym2 in zip(symbol_seq[:-1], symbol_seq[1:]):
            cond_two_buckets.append(f"{sym1}.bk <= {sym2}.bk")
    
    bucketized_range2 = "\n".join([
            f"\tSELECT {bk_s} as bk_s, {bk_e} as bk_e",
            f"\tFROM {sym_join}",
            f"\tWHERE {"\n\t\tAND ".join(cond_two_buckets + conds)}",
            "),"])
    
    return "\n\tUNION\n".join([bucketized_range1, bucketized_range2])


def build_buckets() -> str:
    buckets = textwrap.dedent("""
        buckets AS (
        \tSELECT DISTINCT bk FROM ranges
        \tCROSS JOIN UNNEST(sequence(ranges.bk_s, ranges.bk_e)) AS t(bk)
        ), """)
    return buckets


def build_bucketized_prefilter() -> str:
    prefilter = textwrap.dedent("""
        prefilter AS (
        \tSELECT i.* FROM input_bucketized AS i, buckets AS b
        \tWHERE i.bk = b.bk
        )\n""")
    return prefilter


def build_bucketized_query(input_bucketized: str, ranges: str, buckets: str, prefilter: str, full_mr: str) -> str:
    final = f"SELECT * FROM prefilter MATCH_RECOGNIZE (\n\t{full_mr}\n)"
    query = "".join([input_bucketized, ranges, buckets, prefilter, final])
    return query


def rewrite_bucketized(
    clauses: MatchRecognizeClauses,
    symbol_seq: List[str],
    dataset_name: str,
    full_mr: str
    ) -> str:
    
    pattern_literals = list(flatten(clauses['pattern']))
    pattern = "".join(pattern_literals)
    
    # special case: pattern only has Concatenation operator, e.g., (A B C D E)
    if pattern.isalpha():
        conds, window = map_symbols_to_conds(
            pattern_literals,
            clauses['define'],
            clauses['order_by'][0],
            symbol_seq,
            "bucket"
        )
        
        if not window:
            raise ValueError("Bucketized Prefilter relies on the (missing) Pattern Window Condition")
        
        input_bucketized: str = build_input_bucketized(dataset_name, clauses['order_by'][0], window)
        ranges: str = build_bucketized_ranges(pattern_literals, symbol_seq, conds)
        buckets: str = build_buckets()
        prefilter: str = build_bucketized_prefilter()
        
        new_query = build_bucketized_query(input_bucketized, ranges, buckets, prefilter, full_mr)
    
    return new_query


def main(
    rewrite: str = "basic",
    query_path: str = "input/query.sql",
    symbol_seq: List[str] = ['A', 'B']
    ) -> None:
    """rewrite basic/bucket"""
    
    with open(query_path) as f:
        query = f.read()
    full_mr = extract_full_mr(query)
    dataset_name, mr_clauses = sql_to_clauses(query)
    
    #TODO split to general and special patterns here; same procedure regardless of rewrite
    
    if rewrite == "basic":   
        new_query = rewrite_basic(mr_clauses, symbol_seq, dataset_name, full_mr) 
    elif rewrite == "bucket":
        new_query = rewrite_bucketized(mr_clauses, symbol_seq, dataset_name, full_mr)
        
    os.makedirs("results", exist_ok = True)
    with open(f"results/{dataset_name}_{rewrite}_{"_".join(symbol_seq)}.sql", "w") as out:
        out.write(new_query)

    

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3:])