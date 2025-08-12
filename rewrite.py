import os
from helper.python_trino_parser import sql_to_clauses
import re
import sys
import textwrap


def flatten(lst):
    # Recursively flatten a nested list
    for el in lst:
        if isinstance(el, list):
            yield from flatten(el)
        else:
            yield el


def extract_pattern_symbols(pattern, define):
    all_sym = set(el for el in flatten(pattern) if el.isalpha() and el != 'PERMUTE')
    def_sym = set(clause[0] for clause in define)
    return all_sym, def_sym

    
    
def decompose_pattern(pattern):
    pass    
    
    
def build_window_condition_regex(first_sym, last_sym, order_by):
    
    pattern_str = rf"""
        ^\s*
        ({last_sym})\.{re.escape(order_by)}
        \s*-\s*
        ({first_sym})\.{re.escape(order_by)}
        \s*<=\s*
        (?P<window>
            (?:INTERVAL\s+['"]?\d+(?:\.\d+)?['"]?\s+\w+)
            |
            (?:\d+(?:\.\d+)?\s*\w+)
        )
        \s*$
    """
    return re.compile(pattern_str, re.VERBOSE | re.IGNORECASE)



def map_symbols_to_conds(pattern, define, order_by, symbol_seq):
    conds = []
    
    # symbol_seq must be ordered upfront if pattern contains duplicates
    if len(pattern) == len(set(pattern)): # contains no duplicates
        symbol_seq = sorted(symbol_seq, key=pattern.index)
    
    # sequential conditions
    for sym1, sym2 in zip(symbol_seq[:-1], symbol_seq[1:]):
        conds.append(f"{sym1}.{order_by} <= {sym2}.{order_by}")
        
    # creates dict from list of (sym, def_conds) tuples
    define_dict = dict(define) 
    
    window_pattern = build_window_condition_regex(pattern[0], pattern[-1], order_by)
    window_cond = ""
    window = ""
    non_seq_syms = set(pattern).difference(symbol_seq)
    
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
            
            new_cond_list.append(cond)
        if sym in symbol_seq:
            conds += new_cond_list
    
    if window_cond:
        # window condition can be “propagated backward” through sequential conditions (Proposition 3.1)
        window_cond = window_cond.replace(f"{pattern[0]}.", f"{symbol_seq[0]}.").replace(f"{pattern[-1]}.", f"{symbol_seq[-1]}.")
        conds.append(window_cond)
    return conds, window


def extract_full_mr(query):
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
        mr_content = match.group(1).strip()
        return mr_content
    else:
        print("No MATCH_RECOGNIZE clause found.")
        sys.exit(0)



def get_time_range(symbol_seq, pattern, order_by, window):
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


def build_ranges(pattern, symbol_seq, order_by, dataset_name, conds, window):
    t_s, t_e = get_time_range(symbol_seq, pattern, order_by, window)
    
    sym_join = ", ".join(f"{dataset_name} AS {sym}" for sym in symbol_seq)
    cond_str = "\n\tAND ".join(conds)
    
    ranges = "\n".join([
            "WITH ranges AS (",
            f"\tSELECT {t_s} as t_s, {t_e} as t_e",
            f"\tFROM {sym_join}",
            f"\tWHERE {cond_str}"
            "\n),"])
            
    return ranges


def build_prefilter(dataset_name, order_by):
    prefilter = textwrap.dedent(f"""
        prefilter AS (
            SELECT DISTINCT {dataset_name}.* FROM {dataset_name}, ranges AS r
            WHERE {order_by} BETWEEN r.t_s AND r.t_e
        )
        """)
    return prefilter


def build_query(ranges, prefilter, full_mr):
    final = f"SELECT * FROM prefilter MATCH_RECOGNIZE (\n{full_mr}\n)"
    query = "".join([ranges, prefilter, final])
    return query


def main():
    query = textwrap.dedent("""
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
            PATTERN (R Z B Z M)
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
        """)
    symbol_seq = ['R', 'B']
    full_mr = extract_full_mr(query)
    dataset_name, clauses = sql_to_clauses(query)
    pattern_literals = list(flatten(clauses['pattern']))
    
    # special case: pattern only has Concatenation operator, e.g., (A B C D E)
    if all(literal.isalpha() for literal in pattern_literals):
        conds, window = map_symbols_to_conds(pattern_literals, clauses['define'], clauses['order_by'][0], symbol_seq)
        
        ranges_str = build_ranges(pattern_literals, symbol_seq, clauses['order_by'][0], dataset_name, conds, window)
        prefilter_str = build_prefilter(dataset_name, clauses['order_by'][0])
        new_query = build_query(ranges_str, prefilter_str, full_mr)
        
        os.makedirs("results", exist_ok = True)
        with open(f"results/{dataset_name}_{pattern_literals}", "w") as out:
            out.write(new_query)
    else:   
        # decompose general pattern to special cases
        special_patterns = decompose_pattern(clauses['pattern'])
        for pattern in special_patterns:
            map_symbols_to_conds(pattern, clauses['define'], clauses['order_by'], symbol_seq)
    
    
    
    
    all_sym, def_sym = extract_pattern_symbols(clauses['pattern'], clauses['define'])
    
    if not set(symbol_seq).issubset(def_sym):
        print("All Events in the symbol_set must be defined.")
        sys.exit(0)
    
    
        
    print()
    

if __name__ == "__main__":
    main()