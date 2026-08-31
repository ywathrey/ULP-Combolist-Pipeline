"""
Script to validate CAC JSON files. Built for very large packages (tens of GB).

Differences from the original that matter at scale:
  * the CSV is streamed in binary, so nothing is held in memory and invalid
    UTF-8 bytes cannot abort the run hours in
  * domain ranking is counted in a dict instead of `sort | uniq -c`, which on a
    26 GB input needs several GB of temp disk and a long external merge sort
  * progress is reported to stderr with a rate and an ETA
  * malformed rows go to a file with a count, not millions of lines to stdout
  * extra JSON shards produced by the converter are detected and reported

External dependencies (unchanged):
  * unified-csv-tojson             (on PATH)
  * python3 ~/Codes/scripts/json_validator.py
  * cat, sort, uniq, head          (only with --use-sort)

Usage:
    python3 cac_validate_large.py <parsed_csv> [options]
    python3 cac_validate_large.py huge_parsed.csv --sftp-link 'sftp://...'
"""

import argparse
import os
import subprocess as sp
import sys
import time
from pathlib import Path

GREEN = "\033[32m"
YELLOW = "\033[33m"
RESET = "\033[0m"

METRICS_FILE = "metrics.txt"
FINAL_METRICS_FILE = "final_metrics.txt"
PARSE_FILE = "parse"
BAD_ROWS_FILE = "bad_rows.txt"

READ_BUFFER = 8 * 1024 * 1024
WRITE_BUFFER = 8 * 1024 * 1024
PROGRESS_ROWS = 500_000          # how often to check the clock
BAD_ROWS_SHOWN = 20              # printed to stderr; the rest go to the file
HIGH_CARDINALITY_WARN = 5_000_000

TAIL = "Other notable data fields observed in this package include: "


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def human(num_bytes):
    value = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:,.1f} {unit}"
        value /= 1024


def elapsed(seconds):
    seconds = int(seconds)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{seconds:02d}s"
    if minutes:
        return f"{minutes}m{seconds:02d}s"
    return f"{seconds}s"


def note(message):
    print(message, file=sys.stderr, flush=True)


# --------------------------------------------------------------------------- #
# Pass 1: domain counts
# --------------------------------------------------------------------------- #
def count_domains(csv_file, write_metrics, progress_interval):
    """Stream the CSV once, counting email domains.

    Returns (counts, row_count, bad_count). Memory use is proportional to the
    number of *distinct* domains, not to the size of the file.
    """
    total_bytes = os.path.getsize(csv_file)
    counts = {}
    rows = bad = seen_bytes = 0
    start = time.monotonic()
    last_report = start

    metrics_out = None
    bad_out = None
    try:
        if write_metrics:
            metrics_out = open(METRICS_FILE, "wb", buffering=WRITE_BUFFER)

        with open(csv_file, "rb", buffering=READ_BUFFER) as fptr:
            header = fptr.readline()
            seen_bytes += len(header)

            for raw in fptr:
                seen_bytes += len(raw)
                rows += 1

                field = raw.split(b",", 1)[0]
                parts = field.split(b"@")
                if len(parts) > 1:
                    domain = parts[1].strip().lower()
                else:
                    domain = b""

                if domain:
                    counts[domain] = counts.get(domain, 0) + 1
                    if metrics_out is not None:
                        metrics_out.write(domain + b"\n")
                else:
                    bad += 1
                    if bad_out is None:
                        bad_out = open(BAD_ROWS_FILE, "wb", buffering=1024 * 1024)
                    bad_out.write(field.strip().lower() + b"\n")
                    if bad <= BAD_ROWS_SHOWN:
                        note(field.strip().lower().decode("utf-8", "replace"))

                if progress_interval and rows % PROGRESS_ROWS == 0:
                    now = time.monotonic()
                    if now - last_report >= progress_interval:
                        _report(rows, seen_bytes, total_bytes, len(counts),
                                now - start)
                        last_report = now
    finally:
        if metrics_out is not None:
            metrics_out.close()
        if bad_out is not None:
            bad_out.close()

    took = time.monotonic() - start
    note(
        f"  scanned {rows:,} rows / {human(seen_bytes)} in {elapsed(took)}"
        f"  ({len(counts):,} distinct domains, {bad:,} without one)"
    )
    if bad > BAD_ROWS_SHOWN:
        note(f"  {bad:,} malformed rows written to {BAD_ROWS_FILE}")
    if len(counts) > HIGH_CARDINALITY_WARN:
        note(
            f"{YELLOW}  warning: {len(counts):,} distinct domains is unusually "
            f"high; if memory is tight rerun with --use-sort{RESET}"
        )
    return counts, rows, bad


def _report(rows, seen, total, distinct, took):
    pct = (seen / total * 100) if total else 0.0
    rate = seen / took if took else 0
    eta = (total - seen) / rate if rate else 0
    note(
        f"  {pct:5.1f}%  {rows:>13,} rows  {human(seen):>10}/{human(total)}"
        f"  {human(rate)}/s  {distinct:>9,} domains  ETA {elapsed(eta)}"
    )


# --------------------------------------------------------------------------- #
# Ranking
# --------------------------------------------------------------------------- #
def rank_in_memory(counts, limit):
    """Same ordering and column width as `uniq -c | sort -bnr | head -N`."""
    ranked = sorted(counts.items(), key=lambda kv: (kv[1], kv[0]), reverse=True)
    return "\n".join(
        f"{count:7d} {domain.decode('utf-8', 'replace')}"
        for domain, count in ranked[:limit]
    )


def rank_with_sort(limit):
    """The original shell pipeline. Needs temp disk roughly the size of metrics.txt."""
    return sp.getoutput(
        f"cat {METRICS_FILE} | sort | uniq -c | sort -bnr | head -{limit}"
    )


def write_final_metrics(ranked, limit):
    with open(FINAL_METRICS_FILE, "w", encoding="utf-8") as fptr:
        fptr.write(f"Top {limit} Email Domains\n\n{ranked}")
    print(f"{GREEN}Metrics Collection Done!{RESET}")


# --------------------------------------------------------------------------- #
# Conversion + validation
# --------------------------------------------------------------------------- #
def save_sftp_link(csv_file, link):
    original = csv_file.replace("parsed", "original")
    if link is None:
        link = input("Enter SFTP link: ")
    with open(original, "w", encoding="utf-8") as fptr:
        fptr.write(link)


def convert_to_json(csv_file):
    stem = csv_file[:-4]
    started = time.monotonic()
    note("  running unified-csv-tojson (this is the slow part on a large file)")

    # shell=True keeps the command byte-identical to the original os.system call,
    # but unlike os.system it does not block SIGINT in this process, so Ctrl-C
    # actually stops the run instead of falling through to a false "Done!".
    try:
        completed = sp.run(["unified-csv-tojson", csv_file, ".", stem], check=False)
    except KeyboardInterrupt:
        note(f"{YELLOW}  interrupted after "
             f"{elapsed(time.monotonic() - started)} — conversion incomplete{RESET}")
        raise

    took = elapsed(time.monotonic() - started)
    code = completed.returncode
    if code < 0:
        sys.exit(f"error: unified-csv-tojson killed by signal {-code} after {took}; "
                 f"conversion is incomplete")
    if code:
        sys.exit(f"error: unified-csv-tojson exited {code} after {took}; "
                 f"conversion is incomplete")

    note(f"  converter finished in {took}")
    print(f"{GREEN}Coversion to JSON Done!{RESET}")


def find_shards(csv_file):
    """Locate the JSON shards the converter produced, in numeric order."""
    stem = Path(csv_file[:-4])
    shards = sorted(
        stem.parent.glob(f"{stem.name}*.json"),
        key=lambda p: int("".join(c for c in p.stem[len(stem.name):] if c.isdigit()) or 0),
    )
    return shards


def run_validator(json_file):
    started = time.monotonic()
    report = sp.getoutput(
        f"python3 ~/Codes/scripts/json_validator.py {json_file}"
    ).strip()
    note(f"  validated {json_file} in {elapsed(time.monotonic() - started)}")
    return report


# --------------------------------------------------------------------------- #
# Parse blurb
# --------------------------------------------------------------------------- #
def read_metrics(report):
    total_lines = password_lines = usernames = 0
    pass_type = ""

    for line in report.split("\n"):
        if "cac_email" in line:
            total_lines = int(line.split("\t")[1])
        if "Number of unique usernames" in line:
            usernames = int(line.split("\t")[1])
        if "cac_password" in line:
            password_lines = int(line.split("\t")[1])
        if "Possible password type" in line:
            pass_type = line.split("\t")[1]

    return total_lines, password_lines, usernames, pass_type


TAIL = "Other notable data fields observed in this package include:"

def build_parse_info(total_lines, password_lines, usernames, pass_type):
    lead = "From this package, ZeroFox extracted"
    subject_only = "email addresses and/or usernames" if usernames else "email addresses"

    if not password_lines:
        return (f"{lead} {total_lines:,} {subject_only}. No passwords were "
                f"found in this breach. {TAIL}")

    if total_lines != password_lines:
        return (f"{lead} {total_lines:,} {subject_only}. Of these, an assessed "
                f"{password_lines:,} records were successfully linked to "
                f"{pass_type} passwords. {TAIL}")

    subject = "email addresses, usernames and" if usernames else "email addresses and"
    return f"{lead} {total_lines:,} {subject} {pass_type} passwords. {TAIL}"


# --------------------------------------------------------------------------- #
def parse_args():
    parser = argparse.ArgumentParser(
        description="Validate CAC JSON files (large-package build).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("csv", help="parsed CSV file")
    parser.add_argument("--top", type=int, default=100,
                        help="domains to report (default: 100)")
    parser.add_argument("--sftp-link",
                        help="supply the link up front so the run is unattended")
    parser.add_argument("--shard", type=int, default=0,
                        help="which JSON shard to validate (default: 0)")
    parser.add_argument("--use-sort", action="store_true",
                        help="rank with the original cat|sort|uniq pipeline")
    parser.add_argument("--no-metrics-file", action="store_true",
                        help=f"do not write {METRICS_FILE} (saves GBs of I/O)")
    parser.add_argument("--skip-metrics", action="store_true",
                        help="reuse existing metrics; skip the CSV scan")
    parser.add_argument("--skip-convert", action="store_true",
                        help="JSON shards already exist; skip the converter")
    parser.add_argument("--progress-interval", type=float, default=15.0,
                        help="seconds between progress lines, 0 to silence")
    args = parser.parse_args()

    if args.use_sort and args.no_metrics_file:
        parser.error("--use-sort needs metrics.txt; drop --no-metrics-file")
    return args


def main():
    args = parse_args()
    csv_file = args.csv

    if not os.path.isfile(csv_file):
        sys.exit(f"error: no such file: {csv_file}")

    note(f"input: {csv_file} ({human(os.path.getsize(csv_file))})")

    if not args.skip_metrics:
        counts, _, _ = count_domains(
            csv_file,
            write_metrics=not args.no_metrics_file,
            progress_interval=args.progress_interval,
        )
        ranked = rank_with_sort(args.top) if args.use_sort \
            else rank_in_memory(counts, args.top)
        write_final_metrics(ranked, args.top)
        del counts
    else:
        note("  skipping CSV scan (--skip-metrics)")

    save_sftp_link(csv_file, args.sftp_link)

    if not args.skip_convert:
        convert_to_json(csv_file)

    shards = find_shards(csv_file)
    if len(shards) > 1:
        note(
            f"{YELLOW}  note: converter produced {len(shards)} JSON shards; "
            f"validating shard {args.shard} only, as the original script did. "
            f"Counts in the blurb will cover that shard, not the whole package. "
            f"Use --shard N to pick another.{RESET}"
        )
    json_file = f"{csv_file[:-4]}{args.shard}.json"
    if not os.path.isfile(json_file):
        found = "\n  ".join(str(p) for p in shards) if shards else "(none)"
        sys.exit(
            f"error: expected shard not found: {json_file}\n"
            f"JSON files matching the prefix:\n  {found}\n"
            f"If the converter names its output differently, pass the right index "
            f"with --shard, or check that it finished."
        )

    report = run_validator(json_file)
    parse_info = build_parse_info(*read_metrics(report))

    contents = f"{report}\n\n{parse_info}\n\nBreach Package Headers"
    with open(PARSE_FILE, "w", encoding="utf-8") as fptr:
        fptr.write(contents)
    print(contents)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
