# ULP Combolist Pipeline

A batch pipeline for turning raw **ULP (URL:Login:Password) combolist / breach dumps** into a clean, deduplicated credential dataset plus summary metrics. It is built for **large packages** — tens of gigabytes of messy text files — and streams data through each stage so memory use stays bounded.

The pipeline runs in three stages:

```
input_dir/  ──►  all_emails.txt  ──►  all_emails_parsed.csv  ──►  metrics + JSON + validation report
   (1) extract        (2) parse & dedup            (3) metrics & validate
```

Each stage is a standalone script, and a small orchestrator chains them together with a resume point so an interrupted run can pick up where it left off.

---

## Repository layout

| File | Role |
| --- | --- |
| `combolist_pipeline.py` | Orchestrator. Runs the three stages in order with resume/`--force` logic. |
| `combolist_email_extractor.py` | **Stage 1.** Recursively scans a directory of text files and pulls out every line containing an email address, in parallel. |
| `parsing_combolist.py` | **Stage 2.** Splits each line into email / password / username / domain, then deduplicates by `(email, password)` pair. |
| `get_cac_metrics.py` | **Stage 3.** Computes domain metrics, converts the CSV to JSON, and runs the validator to produce a summary report. |
| `json_validator.py` | Out-of-core (DuckDB) validator that produces per-column counts and data-quality checks over the JSON output. |

> **Note on layout:** the orchestrator expects the two stage scripts (`combolist_email_extractor.py`, `parsing_combolist.py`) to live in a `combolist/` subdirectory relative to itself, while `get_cac_metrics.py` sits alongside the orchestrator. In this repo the files are flat, so either move the stage scripts into a `combolist/` folder or adjust `INNER_SCRIPT_DIR` in `combolist_pipeline.py` before using the orchestrator. The individual scripts can also be run directly (see below).

---

## Requirements

- **Python 3** (uses `multiprocessing`, so run from a terminal, not a Jupyter cell).
- **[DuckDB](https://duckdb.org/) Python package** — required by `json_validator.py` (`pip install duckdb`).
- **`unified-csv-tojson`** — an external CLI, expected on `PATH`, that converts the parsed CSV into newline-delimited JSON. This is an internal/downstream tool rather than a public dependency; substitute your own CSV→JSON converter if you don't have it.
- **Standard shell tools** (`cat`, `sort`, `uniq`, `head`) — only needed if you use the `--use-sort` ranking mode in stage 3.

A couple of paths are **hardcoded** and worth adjusting for your environment:
- `get_cac_metrics.py` invokes the validator at `~/Codes/scripts/json_validator.py`.
- The stage-3 summary blurb is written for a specific downstream cataloging workflow (it mentions "ZeroFox" and a `cac_`-prefixed schema). Edit `build_parse_info()` and the CSV header in `parsing_combolist.py` if you want different wording or column names.

---

## Usage

### Run the whole pipeline

```bash
python combolist_pipeline.py /path/to/input_directory
```

- `input_directory` is a folder containing the raw combolist files.
- Outputs are written to the **parent** of that directory (`all_emails.txt`, `all_emails_parsed.csv`, etc.).
- The runner auto-detects a resume point: if `all_emails_parsed.csv` already exists it starts at stage 3; if only `all_emails.txt` exists it starts at stage 2; otherwise it starts at stage 1. Stage 3 always runs.
- Add `--force` to ignore existing artifacts and re-run from stage 1.

### Or run each stage on its own

**Stage 1 — extract lines containing emails**

```bash
python combolist_email_extractor.py /path/to/input_directory
```

Recursively scans files ending in `.txt`, `.csv`, `.json`, `.log`, `.md`, `.dat`, splits each file into byte-range chunks aligned to line boundaries, and scans them across up to 8 CPU cores. Every line containing an email match is written to `all_emails.txt` in the parent directory, with live progress (lines checked / lines matched) reported every few seconds.

**Stage 2 — parse and deduplicate**

```bash
python parsing_combolist.py [output_dir] [workers] [input_file]
```

Defaults: `output_dir` = directory of the input file, `workers` = CPU count − 2, `input_file` = `all_emails.txt`. Produces:
- `all_emails_parsed.csv` — columns `cac_email, cac_password, cac_username, cac_email_domain`
- `all_emails_removed.log` — oversize rows that were dropped

**Stage 3 — metrics, JSON conversion, and validation**

```bash
python3 get_cac_metrics.py all_emails_parsed.csv [options]
```

Useful options:

| Option | Effect |
| --- | --- |
| `--top N` | Number of top email domains to report (default 100). |
| `--sftp-link '<link>'` | Provide the source link up front for an unattended run (otherwise prompted). |
| `--shard N` | Which JSON shard to validate if the converter emits several (default 0). |
| `--use-sort` | Rank domains with the `cat \| sort \| uniq -c` shell pipeline instead of in memory. |
| `--no-metrics-file` | Skip writing `metrics.txt` to save I/O. |
| `--skip-metrics` | Reuse existing metrics; skip the CSV scan. |
| `--skip-convert` | JSON shards already exist; skip the converter. |
| `--progress-interval S` | Seconds between progress lines (0 to silence). |

---

## How stage 2 parsing works

Combolists are notoriously inconsistent, so the parser does more than a naive `split(":")`:

- **Field splitting** protects `://` so URLs aren't torn apart, then splits the remaining line on `:`.
- **Classification** identifies exactly one email per line (via regex), treats the field nearest the email that *isn't* URL-like as the password, and discards URL-like fields, bare hostnames, `scheme://host` fragments, and `host:port` continuations.
- **Deduplication** hashes each `(email, password)` pair with BLAKE2b and routes it into one of `NUM_BUCKETS` (default **512**) partitions. Phase 1 parses and partitions in parallel; phase 2 deduplicates each bucket in RAM and frees its inputs as it goes. Peak memory scales with `distinct_pairs / NUM_BUCKETS`, so raise `NUM_BUCKETS` if memory is tight.

At the end it prints a breakdown: clean rows written, rows with a password, email-only rows, duplicates skipped, oversize rows removed, and unparseable lines skipped.

---

## Validation checks (`json_validator.py`)

The validator uses DuckDB so it can stream files larger than RAM (spilling to `/tmp/duckdb_tmp`, memory capped at 10 GB by default). Over the `*_parsed<N>.json` files in the working directory it reports:

- Total rows and per-column non-null counts.
- Repeated emails and count of usernames that differ from the email.
- Rows where the password equals the email (flagged).
- Maximum password length, used to guess **plain-text vs. hashed** passwords.
- Invalid date-of-birth months/days and malformed phone numbers, with optional sample rows.

Its output format is kept stable because `get_cac_metrics.py` greps specific lines out of it to build the summary blurb.

---

## Performance notes

- All three heavy stages are parallel and chunked; stage 1 caps at 8 cores, stages 2–3 scale with available CPUs.
- Everything is streamed in binary where possible, so invalid UTF-8 bytes won't abort a multi-hour run and memory stays proportional to *distinct* values rather than file size.
- Temporary working files are created under the input/output directories (`_email_chunks_tmp`, a `parse_chunks_*` temp dir, DuckDB spill in `/tmp`) and cleaned up on success — make sure there's free disk space roughly on the order of the input size.

---

## Responsible use

This tooling is intended for **authorized breach-monitoring and threat-intelligence work** — cataloging leaked-credential packages so affected users, domains, and organizations can be notified and protected. The data it handles is sensitive personal information.

- Only process data you are legally permitted to handle, and store it securely (encrypted at rest, access-controlled).
- The pipeline is purely for *analysis and cataloging*: it does not test, validate, or use credentials against any live service, and it should not be adapted to do so.
- Follow the disclosure, retention, and data-handling requirements that apply to your jurisdiction and organization.
