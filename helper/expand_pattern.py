from itertools import permutations, product
from typing import Any, Iterable, Iterator, List, Sequence, Tuple, Union

# A very simple recursive AST shape: atoms are str, composites are lists
PatternNode = Union[str, List['PatternNode']]


def _is_str(x: Any) -> bool:
    """Return True if x is a string token."""
    return isinstance(x, str)


def _strip_commas(lst: Sequence[Any]) -> List[Any]:
    """Remove ',' tokens from a token list."""
    return [t for t in lst if t != ',']


def _parse_range_token(op: Any) -> Union[Tuple[int, int], Tuple[int, None], None]:
    """Parse a quantifier token of the form {m,n}, {,n}, or {m}.

    Returns (lo, hi) where hi may be None for open upper bound, or None if not a range.
    """
    # op like ['{','3',',','5','}'] or ['{',',','4','}'] or ['{','3','}']
    inner = op[1:-1]  # drop { and }
    if ',' in inner:
        idx = inner.index(',')
        left = inner[:idx]
        right = inner[idx+1:]
        lo = int(left[0]) if left and isinstance(left[0], str) and left[0].isdigit() else 0
        hi = int(right[0]) if right and isinstance(right[0], str) and right[0].isdigit() else None
        return lo, hi
    else:
        if inner and isinstance(inner[0], str) and inner[0].isdigit():
            n = int(inner[0])
            return n, n
        return 0, 0


def has_quantifier(node: Any) -> bool:
    """Return True if node has a quantifier (*, +, ?, or {m,n})."""
    op = node[1]
    if isinstance(op, str):
        return op in {'*', '+', '?'}
    if isinstance(op, list):
        return '{' in op
    return False


def expand(node: PatternNode) -> List[List[str]]:
    """Expand a Trino row pattern AST into concrete symbol sequences.

    Returns: list of patterns, each a list of symbol strings (operators removed).
    Quantifiers policy (approximation for prefiltering):
      - {m,n}: emit exactly m repeats; if m == 0 and (hi > 0), also emit one repeat
      - *: emit 0 and 1
      - +: emit exactly 1
      - ?: emit 0 and 1
    Alternation '|' is expanded. Anchor tokens '^'/'$' and stray '|'/'?' tokens are ignored.
    """
    # base string
    if _is_str(node):
        # ignore explicit operator tokens if parser produced them as strings
        if node in {'|', '?'}:
            return [[]]
        if node in {'^', '$'}:
            return [[]]  # anchor -> no symbol contribution for prefilter
        return [[node]]

    # list node
    if isinstance(node, list):
        node = _strip_commas(node)
        
        # strip away brackets
        if node[0] == '(' and node[-1] == ')':
            node = node[1:-1]
        
        
        # symbol with quantifier: ['A','*'] or ['C', ['{','3',',','5','}']]
        if len(node) == 2 and has_quantifier(node):
            # TODO reluctant quantifiers not implemented
            patternPrimary = node[0]
            op = node[1]
            if op == '*':
                # min=0 -> produce 0 and 1
                return [[]] + expand(patternPrimary)
            if op == '+':
                # min=1 -> produce exactly 1
                return expand(patternPrimary)
            if op == '?':
                return [[]] + expand(patternPrimary)
            # range {m,n}
            rng = _parse_range_token(op) if isinstance(op, list) else None
            if rng is not None:
                lo, hi = rng
                if lo == 0 and (hi is None or hi > lo):
                    # produce 0 and 1 as requested
                    return [[]] + expand(patternPrimary)
                else:
                    # produce exactly lo repetitions (take first expansion of primary)
                    primary = expand(patternPrimary)
                    return [primary[0] * lo] if primary else [[]]
            # unknown op -> treat as single symbol
            return expand(patternPrimary)
        
        # PERMUTE(...)  tokenization may look like ['PERMUTE','(', arg1, ',', arg2, ..., ')']
        if len(node) >= 4 and _is_str(node[0]) and node[0].upper() == 'PERMUTE':
            args = node[2:-1]
            # expand each arg, can yield list-of-partial-patterns
            expanded_args = [expand(arg) for arg in args]
            out: List[List[str]] = []
            for choice in product(*expanded_args):
                # choice is tuple of sequences lists (each sequence is a list of symbols)
                for perm in permutations(range(len(choice))):
                    seq: List[str] = []
                    for idx in perm:
                        seq.extend(choice[idx])
                    out.append(seq)
            # dedupe
            uniq: List[List[str]] = []
            seen = set()
            for s in out:
                key = tuple(s)
                if key not in seen:
                    seen.add(key)
                    uniq.append(list(s))
            return uniq

        # alternation
        if len(node) == 3 and node[1] == '|':
            return expand(node[0]) + expand(node[2])
        
        # empty pattern '()' case
        if len(node) == 2 and node[0] == '(' and node[1] == ')':
            return [[]]

        # excluded pattern ['{-', inner, '-}'] -> ignore (no contribution)
        if len(node) == 3 and node[0] == '{-' and node[2] == '-}':
            return [[]]

        # generic concatenation: expand children and concat cartesian product
        parts: List[List[List[str]]] = []
        for sub in node:
            parts.append(expand(sub))

        res: List[List[str]] = [[]]
        for part in parts:
            new: List[List[str]] = []
            for prefix in res:
                for seq in part:
                    new.append(prefix + seq)
            res = new

        # dedupe keeping order
        uniq: List[List[str]] = []
        seen = set()
        for s in res:
            key = tuple(s)
            if key not in seen:
                seen.add(key)
                uniq.append(list(s))
        return uniq

    # fallback
    return [[]]


if __name__ == "__main__":
    tests = {
        # ((A* B+) | D{,4})? C{3,5}
        "node1": [[['(', [['(', [['A', '*'], ['B', '+']], ')'], '|', ['D', ['{', ',', '4', '}']]], ')'], '?'],
                  ['C', ['{', '3', ',', '5', '}']]],

        # (A B+ | C{2,4})? D* PERMUTE(E, F G{3})
        "node2": [[[['(', [['A', ['B', '+']], '|', ['C', ['{', '2', ',', '4', '}']]], ')'], '?'], ['D', '*']],
                  ['PERMUTE', '(', 'E', ',', ['F', ['G', ['{', '3', '}']]], ')']],

        # A (B+)? C
        "node3": [['A', [['(', ['B', '+'], ')'], '?']], 'C'],

        # (A|B)(C|D)
        "node4": [['(', ['A', '|', 'B'], ')'], ['(', ['C', '|', 'D'], ')']],

        # A | B(C|D)
        "node5": ['A', '|', ['B', ['(', ['C', '|', 'D'], ')']]],

        # PERMUTE(A, B+, C, D{1})
        "node6": ['PERMUTE', '(', 'A', ',', ['B', '+'], ',', 'C', ',', ['D', ['{', '1', '}']], ')'],

        # ((A+ B?) | (C{2,3})) D* PERMUTE(E, F{2})
        "node7": [[['(', [['(', [['A', '+'], ['B', '?']], ')'], '|', ['(', ['C', ['{', '2', ',', '3', '}']], ')']], ')'],
                   ['D', '*']],
                  ['PERMUTE', '(', 'E', ',', ['F', ['{', '2', '}']], ')']]
    }

    for name, node in tests.items():
        res = expand(node)
        print(f"{name}: {len(res)} patterns")
        for pat in res:
            print(" ", pat)
        print("-" * 40)
