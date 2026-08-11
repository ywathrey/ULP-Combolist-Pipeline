"""
Extract all lines containing email addresses from large text files,
using all available CPU cores in parallel, with live progress updates.

Usage:
    python extract_emails_parallel.py /path/to/target/directory

All matching lines are written to a single file:
    /path/to/target/directory/all_emails.txt

Run from a terminal (NOT inside a Jupyter cell) for best reliability.
"""

import argparse
import os
import re
import sys
import time
from multiprocessing import Manager, Pool, cpu_count

EMAIL_PATTERN = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
BATCH_SIZE = 10_000          # lines buffered before writing
PROGRESS_EVERY = 500_000     # lines between progress updates
NUM_CORES = min(8, cpu_count())

OUTPUT_FILENAME = 'all_emails.txt'   # single output file, written into the target directory


def find_chunk_boundaries(filepath, num_chunks):
    """Split a file into byte ranges aligned to line boundaries."""
    size = os.path.getsize(filepath)
    if size == 0:
        return []
    num_chunks = max(1, min(num_chunks, size))
    approx = size // num_chunks
    bounds = [0]
    with open(filepath, 'rb') as f:
        for i in range(1, num_chunks):
            f.seek(approx * i)
            f.readline()          # advance to the next full line
            pos = f.tell()
            if pos >= size:
                break
            bounds.append(pos)
    bounds.append(size)
    bounds = sorted(set(bounds))
    return [(bounds[i], bounds[i + 1]) for i in range(len(bounds) - 1)]


def process_chunk(args):
    """Worker: scan one byte-range of one file, write matches to its own temp file."""
    filepath, start, end, chunk_id, tmp_dir, progress = args
    out_path = os.path.join(tmp_dir, f"chunk_{chunk_id:04d}.txt")
    checked = 0
    matched = 0
    batch = []
    since_update = 0

    with open(filepath, 'rb') as f_in, \
         open(out_path, 'w', encoding='utf-8', buffering=8 * 1024 * 1024) as f_out:
        f_in.seek(start)
        pos = start
        while pos < end:
            line = f_in.readline()
            if not line:
                break
            pos += len(line)
            checked += 1
            since_update += 1

            text = line.decode('utf-8', errors='ignore')
            if '@' in text and EMAIL_PATTERN.search(text):
                batch.append(text.strip())
                matched += 1
                if len(batch) >= BATCH_SIZE:
                    f_out.write('\n'.join(batch) + '\n')
                    batch.clear()

            if since_update >= PROGRESS_EVERY:
                progress[chunk_id] = (checked, matched)
                since_update = 0

        if batch:
            f_out.write('\n'.join(batch) + '\n')

    progress[chunk_id] = (checked, matched)
    return chunk_id, out_path, checked, matched


def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract lines containing email addresses from all text "
                    "files in a directory, in parallel, with progress updates. "
                    "Writes all matches into a single output file."
    )
    parser.add_argument(
        "directory",
        help="Path to the target directory containing the text files to scan.",
    )
    args = parser.parse_args()

    target_dir = os.path.abspath(args.directory)
    if not os.path.isdir(target_dir):
        print(f"❌ Not a valid directory: {target_dir}")
        sys.exit(1)

    return target_dir


def main():
    target_dir = parse_args()
    parent_dir = os.path.dirname(target_dir)   # sibling location of the target directory
    output_path = os.path.join(parent_dir, OUTPUT_FILENAME)

    print(f"📁 Target directory: {target_dir}")
    print(f"📄 Output file:      {output_path}")
    print(f"🧠 Using {NUM_CORES} core(s)\n")

    tmp_dir_name = '_email_chunks_tmp'
    text_files = []
    for root, dirs, files in os.walk(target_dir):
        dirs[:] = [d for d in dirs if d != tmp_dir_name]   # don't descend into our own tmp dir
        for f in files:
            if f.endswith(('.txt', '.csv', '.json', '.log', '.md', '.dat')):
                text_files.append(os.path.join(root, f))

    if not text_files:
        print("❌ No text files found!")
        return

    print(f"📄 Found {len(text_files)} text file(s) ;) ")

    tmp_dir = os.path.join(target_dir, '_email_chunks_tmp')
    os.makedirs(tmp_dir, exist_ok=True)

    sizes = {f: os.path.getsize(f) for f in text_files}
    total_size = sum(sizes.values()) or 1

    # Build chunk jobs: split each file proportionally to its size so all
    # cores across all files stay busy, instead of 1 core per file.
    jobs = []
    chunk_id = 0
    for f in text_files:
        share = sizes[f] / total_size
        n_chunks = max(1, round(NUM_CORES * share))
        for start, end in find_chunk_boundaries(f, n_chunks):
            jobs.append((f, start, end, chunk_id))
            chunk_id += 1

    total_jobs = len(jobs)
    print(f"🧩 Split into {total_jobs} chunk(s) across {NUM_CORES} core(s)\n")

    manager = Manager()
    progress = manager.dict({j[3]: (0, 0) for j in jobs})
    job_args = [(f, start, end, cid, tmp_dir, progress) for (f, start, end, cid) in jobs]

    start_time = time.time()
    with Pool(processes=NUM_CORES) as pool:
        async_result = pool.map_async(process_chunk, job_args)

        while not async_result.ready():
            time.sleep(5)
            checked_total = sum(v[0] for v in progress.values())
            matched_total = sum(v[1] for v in progress.values())
            elapsed = time.time() - start_time
            print(f"   ⏳ {elapsed:6.0f}s elapsed | lines checked: {checked_total:,} "
                  f"| lines with emails: {matched_total:,}")

        results = async_result.get()

    elapsed = time.time() - start_time
    checked_total = sum(r[2] for r in results)
    matched_total = sum(r[3] for r in results)
    print(f"\n✅ All chunks finished in {elapsed:.1f}s")
    print(f"   Total lines checked: {checked_total:,}")
    print(f"   Total lines with emails: {matched_total:,}\n")

    # Merge all chunk outputs, in original order, into the single output file.
    print(f"🔗 Merging {len(results)} chunk file(s) into '{OUTPUT_FILENAME}' ...")

    with open(output_path, 'w', encoding='utf-8', buffering=8 * 1024 * 1024) as f_out:
        for _, chunk_path, _, _ in results:   # already in job/chunk order
            with open(chunk_path, 'r', encoding='utf-8') as f_in:
                for line in f_in:
                    f_out.write(line)
            os.remove(chunk_path)
    os.rmdir(tmp_dir)

    if matched_total:
        size_mb = os.path.getsize(output_path) / (1024 ** 2)
        print(f"💾 Saved: {output_path}  ({size_mb:.2f} MB)\n")

        print("Preview of extracted lines:")
        print("-" * 60)
        with open(output_path, 'r', encoding='utf-8') as f_out:
            for i, line in enumerate(f_out, 1):
                if i > 20:
                    print("\n... and more lines (see the full output file)")
                    break
                print(f"{i:3d}. {line.strip()}")
    else:
        print("❌ No emails found in any files!")
        os.remove(output_path)


if __name__ == "__main__":
    main()
