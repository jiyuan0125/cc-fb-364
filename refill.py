#!/usr/bin/env python3
"""Apply pending backfill (refill) requests to the daily aggregate.

Inputs (all inside the data directory):

  * ``backfill_requests.csv`` -- ``req_id,win_from,win_to,ver,state``.
    Several rows with the same ``req_id`` are versions of one backfill;
    the larger version number is the newer one.  ``VOID`` marks a version
    whose values were already merged into the daily aggregate earlier,
    ``PENDING`` marks a request that still has to be executed.
  * ``raw/<req_id>_v<ver>.csv`` -- freshly collected interaction events.
    The file may contain events outside the request window; only the
    request window (inclusive on both ends) is authoritative.
  * ``daily_agg.csv``   -- the already computed daily aggregate.
  * ``downstream.txt``  -- tasks depending on the daily aggregate.

The script executes the PENDING requests:

  1. every not-yet-rolled-back VOID version of that request is rolled back
     over *its own* application window;
  2. the PENDING version(s) are merged over *their own* windows;
  3. dates touched by any of those operations are recomputed while all
     other dates and the date set itself stay exactly as they were;
  4. every applied operation is recorded per affected date in ``ledger.txt``;
  5. downstream tasks whose dependency interval intersects an affected
     date are flagged ``REFRESH`` in ``cascade.txt``.

A (request, version, operation) already present in ``ledger.txt`` is never
applied again, so repeated executions are idempotent: the aggregate and the
ledger do not double up.
"""

import argparse
import csv
import os
import sys

ACTIONS = ("view", "like", "comment", "share")
LEDGER_HEADER = "op req_id ver date " + " ".join(ACTIONS)
CASCADE_HEADER = "task_id status dates reason"

ROLLBACK = "ROLLBACK"
REFILL = "REFILL"


# ---------------------------------------------------------------- reading --

def read_requests(path):
    """Return the request rows in file order."""
    rows = []
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for raw in reader:
            if not raw or not (raw.get("req_id") or "").strip():
                continue
            rows.append(
                {
                    "req_id": raw["req_id"].strip(),
                    "win_from": raw["win_from"].strip(),
                    "win_to": raw["win_to"].strip(),
                    "ver": int(raw["ver"].strip()),
                    "state": raw["state"].strip().upper(),
                }
            )
    return rows


def read_daily_agg(path):
    """Return ``{date: {action: int}}`` keeping the original row order."""
    rows = {}
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for raw in reader:
            if not raw or not (raw.get("date") or "").strip():
                continue
            date = raw["date"].strip()
            rows[date] = {a: int((raw.get(a) or "0").strip() or 0) for a in ACTIONS}
    return rows


def write_daily_agg(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["date"] + list(ACTIONS))
        for date, counts in rows.items():
            writer.writerow([date] + [counts[a] for a in ACTIONS])


def read_downstream(path):
    tasks = []
    if not os.path.exists(path):
        return tasks
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for raw in reader:
            if not raw or not (raw.get("task_id") or "").strip():
                continue
            tasks.append(
                {
                    "task_id": raw["task_id"].strip(),
                    "dep_from": raw["dep_from"].strip(),
                    "dep_to": raw["dep_to"].strip(),
                }
            )
    return tasks


def read_ledger(path):
    """Return the set of ``(req_id, ver, op)`` already registered."""
    applied = set()
    if not os.path.exists(path):
        return applied
    with open(path, newline="", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if parts[0].lower() == "op":  # header
                continue
            if len(parts) < 4:
                continue
            op = parts[0].upper()
            req_id = parts[1]
            try:
                ver = int(parts[2])
            except ValueError:
                continue
            if op in (ROLLBACK, REFILL):
                applied.add((req_id, ver, op))
    return applied


def read_raw_counts(raw_dir, req, agg_dates):
    """Count in-window events of one version per date and per action.

    Events outside ``[win_from, win_to]`` are ignored: the request window,
    not the raw file content, defines what may be applied.  Dates that do
    not exist in the daily aggregate are dropped as well, because the set
    of aggregate dates is fixed.
    """
    req_id = req["req_id"]
    ver = req["ver"]
    win_from = req["win_from"]
    win_to = req["win_to"]

    path = os.path.join(raw_dir, f"{req_id}_v{ver}.csv")
    counts = {}
    if not os.path.exists(path):
        print(f"refill: warning: raw file missing for {req_id} v{ver}: {path}",
              file=sys.stderr)
        return counts

    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for raw in reader:
            if not raw:
                continue
            date = (raw.get("event_date") or "").strip()
            action = (raw.get("action") or "").strip().lower()
            if action not in ACTIONS:
                continue
            if not (win_from <= date <= win_to):
                continue  # outside the authoritative application window
            if date not in agg_dates:
                print(
                    f"refill: warning: {req_id} v{ver} has events on {date} "
                    f"outside the aggregate date set; ignored",
                    file=sys.stderr,
                )
                continue
            bucket = counts.setdefault(date, {a: 0 for a in ACTIONS})
            bucket[action] += 1
    return counts


# -------------------------------------------------------------- execution --

def apply_operation(op, req, counts, agg, new_ledger_lines, op_dates):
    """Apply one operation to the aggregate and stage its ledger rows.

    Returns True when at least one date was touched.
    """
    sign = -1 if op == ROLLBACK else 1
    touched = False
    for date in sorted(counts):
        delta = counts[date]
        if not any(delta[a] for a in ACTIONS):
            continue
        for action in ACTIONS:
            agg[date][action] += sign * delta[action]
        new_ledger_lines.append(
            " ".join(
                [op, req["req_id"], str(req["ver"]), date]
                + [str(delta[a]) for a in ACTIONS]
            )
        )
        op_dates.setdefault(date, set()).add(op.lower())
        touched = True
    return touched


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Execute pending backfill requests on the daily aggregate."
    )
    parser.add_argument(
        "data_dir",
        nargs="?",
        default=None,
        help="directory holding the input files (default: current directory)",
    )
    args = parser.parse_args(argv)

    if args.data_dir:
        base = args.data_dir
    elif os.path.exists(os.path.join(os.getcwd(), "backfill_requests.csv")):
        base = os.getcwd()
    else:
        base = os.path.dirname(os.path.abspath(__file__))

    req_path = os.path.join(base, "backfill_requests.csv")
    agg_path = os.path.join(base, "daily_agg.csv")
    downstream_path = os.path.join(base, "downstream.txt")
    ledger_path = os.path.join(base, "ledger.txt")
    cascade_path = os.path.join(base, "cascade.txt")
    raw_dir = os.path.join(base, "raw")

    requests = read_requests(req_path)
    agg = read_daily_agg(agg_path)
    agg_dates = set(agg)
    tasks = read_downstream(downstream_path)
    applied = read_ledger(ledger_path)

    # Group versions by request, preserving the order requests appear in.
    order = []
    groups = {}
    for req in requests:
        rid = req["req_id"]
        if rid not in groups:
            order.append(rid)
            groups[rid] = []
        groups[rid].append(req)

    new_ledger_lines = []
    op_dates = {}  # date -> {"rollback", "refill"}
    processed = set(applied)  # guard against duplicates within a single run
    aggregate_changed = False

    for rid in order:
        group = groups[rid]
        pendings = [r for r in group if r["state"] == "PENDING"]
        if not pendings:
            continue  # only PENDING requests are executed
        # The largest version number is the newest one: older PENDING rows
        # are superseded drafts that were never merged.
        current = max(pendings, key=lambda r: r["ver"])
        voids = sorted(
            (r for r in group if r["state"] == "VOID"), key=lambda r: r["ver"]
        )

        # First revert what the voided versions had already merged...
        for req in voids:
            key = (rid, req["ver"], ROLLBACK)
            if key in processed:
                continue
            counts = read_raw_counts(raw_dir, req, agg_dates)
            if apply_operation(
                ROLLBACK, req, counts, agg, new_ledger_lines, op_dates
            ):
                aggregate_changed = True
            processed.add(key)

        # ...then merge the new (pending) version.
        req = current
        key = (rid, req["ver"], REFILL)
        if key not in processed:
            counts = read_raw_counts(raw_dir, req, agg_dates)
            if apply_operation(
                REFILL, req, counts, agg, new_ledger_lines, op_dates
            ):
                aggregate_changed = True
            processed.add(key)

    # ------------------------------------------------------------- output --
    if aggregate_changed:
        write_daily_agg(agg_path, agg)

    if not os.path.exists(ledger_path):
        with open(ledger_path, "w", encoding="utf-8") as fh:
            fh.write(LEDGER_HEADER + "\n")
    if new_ledger_lines:
        with open(ledger_path, "a", encoding="utf-8") as fh:
            for line in new_ledger_lines:
                fh.write(line + "\n")

    cascade_rows = []
    for task in tasks:
        lo, hi = task["dep_from"], task["dep_to"]
        affected = sorted(d for d in op_dates if lo <= d <= hi)
        if not affected:
            continue  # task is not impacted by this run
        ops = set()
        for date in affected:
            ops |= op_dates[date]
        if ops == {"rollback", "refill"}:
            reason = "rollback+refill"
        elif ops == {"rollback"}:
            reason = "rollback"
        else:
            reason = "refill"
        cascade_rows.append(
            " ".join([task["task_id"], "REFRESH", " ".join(affected), reason])
        )

    # Nothing was applied (e.g. a repeated run): keep the previous cascade
    # untouched instead of replacing it with an empty result.
    if new_ledger_lines or not os.path.exists(cascade_path):
        with open(cascade_path, "w", encoding="utf-8") as fh:
            fh.write(CASCADE_HEADER + "\n")
            for line in cascade_rows:
                fh.write(line + "\n")

    print(
        f"refill: {len(new_ledger_lines)} ledger row(s), "
        f"{len(cascade_rows)} cascade task(s)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
