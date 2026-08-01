#!/usr/bin/env python3
"""Differential testing of Zodd against clingo.

Generates random stratified Datalog programs in the dialect subset both
engines share, evaluates them with the zodd CLI and with clingo, and
compares every derived predicate row for row. A random bound-argument
query is also answered through zodd's demand-driven path (magic sets) and
checked against the clingo model.

The generator stays inside the common semantics on purpose: arithmetic is
limited to + and * over small non-negative integers, because zodd's
unsigned fail-on-underflow subtraction and fail-on-zero division
legitimately differ from clingo's signed integers.

Usage: difftest.py [--runs N] [--seed S] [--zodd PATH]
"""

import argparse
import random
import subprocess
import sys
import tempfile
from pathlib import Path

import clingo

CMP_OPS = ["<", "<=", ">", ">=", "!="]
AGG_FUNCS = ["count", "sum", "min", "max"]


class Pred:
    def __init__(self, name, arity, tier):
        self.name = name
        self.arity = arity
        self.tier = tier


def fresh_vars(n):
    return [f"V{i}" for i in range(n)]


class Generator:
    """Builds one random layered program: tier 0 holds facts, higher tiers
    hold rules over strictly lower tiers (negation, comparisons,
    assignments) or the same tier (positive recursion only)."""

    def __init__(self, rng):
        self.rng = rng
        self.zodd_lines = []
        self.clingo_lines = []
        self.derived = []

    def emit(self, zodd, clingo_line=None):
        self.zodd_lines.append(zodd)
        self.clingo_lines.append(clingo_line if clingo_line is not None else zodd)

    def generate(self):
        rng = self.rng
        domain = rng.randint(6, 12)

        # Tier 0: base facts.
        edb = []
        for i in range(rng.randint(2, 3)):
            pred = Pred(f"b{i}", rng.randint(1, 3), 0)
            edb.append(pred)
            for _ in range(rng.randint(3, 14)):
                row = [rng.randrange(domain) for _ in range(pred.arity)]
                self.emit(f"{pred.name}({', '.join(map(str, row))}).")

        lower = list(edb)
        for tier in range(1, rng.randint(2, 4)):
            tier_preds = []
            for i in range(rng.randint(1, 3)):
                pred = Pred(f"t{tier}p{i}", rng.randint(1, 3), tier)
                tier_preds.append(pred)
            for pred in tier_preds:
                for _ in range(rng.randint(1, 2)):
                    self.rule(pred, lower, tier_preds)
            self.derived.extend(tier_preds)
            lower.extend(tier_preds)

        # Top tier: one aggregate over a binary lower predicate, when one
        # exists.
        binary = [p for p in lower if p.arity == 2 and p.tier > 0]
        if binary and rng.random() < 0.7:
            src = rng.choice(binary)
            func = rng.choice(AGG_FUNCS)
            agg = Pred("agg0", 2, 99)
            self.derived.append(agg)
            self.emit(
                f"{agg.name}(G, {func}(M)) :- {src.name}(G, M).",
                f"{agg.name}(G, C) :- {src.name}(G, _), "
                f"C = #{func}{{ M : {src.name}(G, M) }}.",
            )
        return self.derived

    def rule(self, head, lower, tier_preds):
        rng = self.rng
        recursive = rng.random() < 0.35 and any(p is not head for p in tier_preds)

        body = []
        bound = []
        next_var = 0

        def pick_terms(arity):
            nonlocal next_var
            terms = []
            for _ in range(arity):
                if bound and rng.random() < 0.5:
                    terms.append(rng.choice(bound))
                else:
                    var = f"V{next_var}"
                    next_var += 1
                    terms.append(var)
                    bound.append(var)
            return terms

        for i in range(rng.randint(1, 3)):
            src = rng.choice(lower)
            if recursive and i == 0:
                src = rng.choice([p for p in tier_preds if p is not head] or lower)
            body.append(f"{src.name}({', '.join(pick_terms(src.arity))})")

        items = list(body)

        # Assignment (non-recursive rules only, so both engines reach the
        # same finite fixed point).
        assigned = None
        if not recursive and bound and rng.random() < 0.4:
            assigned = f"V{next_var}"
            next_var += 1
            expr = f"{rng.choice(bound)} {rng.choice(['+', '*'])} {rng.randint(1, 5)}"
            items.append((f"{assigned} is {expr}", f"{assigned} = {expr}"))
            bound.append(assigned)

        # Negated literal over a strictly lower tier, all arguments bound.
        candidates = [p for p in lower if p.tier < head.tier and p.arity <= len(bound)]
        if candidates and rng.random() < 0.5:
            src = rng.choice(candidates)
            args = rng.sample(bound, src.arity)
            items.append(f"not {src.name}({', '.join(args)})")

        # Comparison filter, sometimes with arithmetic.
        if bound and rng.random() < 0.6:
            lhs = rng.choice(bound)
            if rng.random() < 0.5:
                lhs = f"{lhs} {rng.choice(['+', '*'])} {rng.randint(1, 4)}"
            rhs = str(rng.randint(0, 16)) if rng.random() < 0.6 else rng.choice(bound)
            items.append(f"{lhs} {rng.choice(CMP_OPS)} {rhs}")

        head_args = [rng.choice(bound) for _ in range(head.arity)]
        if assigned and rng.random() < 0.7:
            head_args[rng.randrange(head.arity)] = assigned

        zodd_body = ", ".join(i if isinstance(i, str) else i[0] for i in items)
        clingo_body = ", ".join(i if isinstance(i, str) else i[1] for i in items)
        head_text = f"{head.name}({', '.join(head_args)})"
        self.emit(f"{head_text} :- {zodd_body}.", f"{head_text} :- {clingo_body}.")


def solve_clingo(program):
    """Grounds and solves; returns {pred: sorted list of int tuples}."""
    ctl = clingo.Control(["--warn", "no-atom-undefined"])
    ctl.add("base", [], program)
    ctl.ground([("base", [])])
    rows = {}
    with ctl.solve(yield_=True) as handle:
        for model in handle:
            for atom in model.symbols(atoms=True):
                rows.setdefault(atom.name, set()).add(
                    tuple(arg.number for arg in atom.arguments)
                )
            break
    return {name: sorted(tuples) for name, tuples in rows.items()}


def parse_zodd_run(output):
    """Parses `zodd run` output: `pred:` headers followed by `(a, b)` rows."""
    rows = {}
    current = None
    for line in output.splitlines():
        if line.endswith(":") and not line.startswith(" ") and not line.startswith("("):
            current = line[:-1].removeprefix("?- ")
            rows.setdefault(current, set())
        elif line.startswith("(") and current is not None:
            if line.startswith("(no rows"):
                continue
            body = line.strip("()")
            rows[current].add(tuple(int(v) for v in body.split(", ")) if body else ())
    return {name: sorted(tuples) for name, tuples in rows.items()}


def run_zodd(zodd, args):
    result = subprocess.run([zodd, *args], capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(f"zodd {' '.join(args)} failed:\n{result.stderr}")
    return result.stdout


def check_seed(zodd, seed, workdir):
    rng = random.Random(seed)
    gen = Generator(rng)
    derived = gen.generate()
    zodd_text = "\n".join(gen.zodd_lines) + "\n"
    clingo_text = "\n".join(gen.clingo_lines) + "\n"

    program = workdir / f"seed{seed}.dl"
    program.write_text(zodd_text)

    expected = solve_clingo(clingo_text)
    parallel = ["-j", "4"] if seed % 2 == 0 else []
    got = parse_zodd_run(run_zodd(zodd, ["run", str(program), *parallel]))

    failures = []
    for pred in derived:
        want = expected.get(pred.name, [])
        have = got.get(pred.name, [])
        if want != have:
            failures.append((f"run: {pred.name}", want, have))

    # Demand-driven (magic sets) spot check: bind the first argument of one
    # derived predicate to a value clingo derived for it.
    demand_targets = [p for p in derived if expected.get(p.name)]
    if demand_targets and not failures:
        target = rng.choice(demand_targets)
        rows = expected[target.name]
        bound_value = rng.choice(rows)[0]
        goal = f"{target.name}({bound_value}{', _' * (target.arity - 1)})"
        out = run_zodd(zodd, ["query", str(program), goal])
        have = sorted(
            tuple(int(v) for v in line.strip("()").split(", "))
            for line in out.splitlines()
            if line.startswith("(") and not line.startswith("(no rows")
        )
        want = sorted(r for r in rows if r[0] == bound_value)
        if want != have:
            failures.append((f"queryDemand: {goal}", want, have))

    if failures:
        print(f"\nseed {seed}: MISMATCH")
        print("--- program (zodd) ---")
        print(zodd_text)
        print("--- program (clingo) ---")
        print(clingo_text)
        for what, want, have in failures:
            print(f"[{what}]")
            print(f"  clingo: {want}")
            print(f"  zodd:   {have}")
        return False
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=200)
    parser.add_argument("--seed", type=int, default=None, help="run one specific seed")
    parser.add_argument("--zodd", default="zig-out/bin/zodd")
    args = parser.parse_args()

    zodd = Path(args.zodd)
    if not zodd.exists():
        sys.exit(f"zodd CLI not found at {zodd}; run `make cli` first")

    seeds = [args.seed] if args.seed is not None else range(args.runs)
    failures = 0
    with tempfile.TemporaryDirectory() as tmp:
        for seed in seeds:
            if not check_seed(str(zodd), seed, Path(tmp)):
                failures += 1
            else:
                print(".", end="", flush=True)
    print()
    if failures:
        sys.exit(f"{failures} mismatching seed(s)")
    print(f"all {len(list(seeds))} seeds agree")


if __name__ == "__main__":
    main()
