#!/usr/bin/env python3
"""Pipeline runner with a resume point.
The steps form a chain: input_dir -> all_emails.txt -> all_emails_parsed.csv.
So we look for the *latest* artifact that already exists and start from the
step after it. Anything earlier in the chain is irrelevant by definition.
  all_emails_parsed.csv present -> start at step 3
  all_emails.txt present        -> start at step 2
  neither                       -> start at step 1
Step 3 always runs. Use --force to ignore existing artifacts entirely.
"""
import argparse
import os
import subprocess
import sys
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INNER_SCRIPT_DIR = os.path.join(SCRIPT_DIR, "combolist")  # keep your existing value
def check_exists(path):
    if not os.path.exists(path):
        print(f"Error: expected output missing: {path}")
        sys.exit(1)
def is_usable(path, min_bytes=1):
    """Present and non-empty.
    A run killed mid-write leaves a 0-byte file. Treating that as a valid
    artifact would skip the step forever and poison every run after it, so
    size is part of the test rather than bare os.path.exists().
    """
    return os.path.isfile(path) and os.path.getsize(path) >= min_bytes
def run(script_path, *args, cwd=None):
    # sys.executable, not "python": guarantees the child runs in the same
    # interpreter/venv as the parent. Bare "python" resolves via PATH and can
    # silently pick a different environment.
    subprocess.run([sys.executable, script_path, *args], check=True, cwd=cwd)
def main():
    parser = argparse.ArgumentParser(description="Run the email pipeline.")
    parser.add_argument("input_directory")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore existing artifacts and re-run from step 1.",
    )
    args = parser.parse_args()
    input_dir = os.path.abspath(os.path.normpath(args.input_directory))
    if not os.path.isdir(input_dir):
        print(f"Error: {input_dir} is not a directory")
        sys.exit(1)
    parent_dir = os.path.dirname(input_dir)
    output_txt = os.path.join(parent_dir, "all_emails.txt")
    output_csv = os.path.join(parent_dir, "all_emails_parsed.csv")
    print(f"input_dir:  {input_dir}")
    print(f"parent_dir: {parent_dir}")
    # ---------- pick the resume point ----------
    # Checked newest-artifact-first, so the csv short-circuits the txt check.
    # If the csv is already here, whether the txt exists doesn't matter.
    if args.force:
        start_step = 1
        reason = "--force"
    elif is_usable(output_csv):
        start_step = 3
        reason = f"found {os.path.basename(output_csv)}"
    elif is_usable(output_txt):
        start_step = 2
        reason = f"found {os.path.basename(output_txt)}"
    else:
        start_step = 1
        reason = "no existing artifacts"
    print(f"\nStarting at step {start_step} ({reason})")
    # ---------- Step 1 ----------
    if start_step <= 1:
        print(f"\nRunning step 1 on: {input_dir}")
        run(os.path.join(INNER_SCRIPT_DIR, "combolist_email_extractor.py"), input_dir)
        check_exists(output_txt)
    # ---------- Step 2 ----------
    # cwd=parent_dir because script2 reads "all_emails.txt" via a relative path
    if start_step <= 2:
        print("\nRunning step 2...")
        run(os.path.join(INNER_SCRIPT_DIR, "parsing_combolist.py"), parent_dir, cwd=parent_dir)
        check_exists(output_csv)
    # ---------- Step 3 ----------
    print("\nRunning step 3...")
    run(os.path.join(SCRIPT_DIR, "get_cac_metrics.py"), output_csv, cwd=parent_dir)
    print("\nPipeline finished successfully.")
if __name__ == "__main__":
    main()
