import csv
import os
import re
import sys
import shutil
import hashlib
import tempfile
import multiprocessing as mp

csv.field_size_limit(2 ** 31 - 1)

# ---------------------------------------------------------------------------
# Scaling knobs
# ---------------------------------------------------------------------------
# Dedup is partitioned into NUM_BUCKETS by hash(email,password). Each bucket is
# deduped in RAM in phase 2, so peak dedup memory is roughly:
#     (total_distinct / NUM_BUCKETS) * ~70 bytes * (parallel phase-2 workers)
# Raise NUM_BUCKETS to shrink that. Each phase-1 worker holds NUM_BUCKETS files
# open at once, so also mind the open-file limit (raised best-effort below).
NUM_BUCKETS = 512

# Max allowed length for any single output field (matches csv default).
LIMIT = 131072
_SCAN_CAP = 8192


def _raise_fd_limit(target):
    """Best-effort raise of the soft open-file limit so a worker can hold all
    bucket files open. No-op on platforms without `resource` (e.g. Windows)."""
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft < target:
            resource.setrlimit(resource.RLIMIT_NOFILE, (min(target, hard), hard))
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Field cleaning / classification (unchanged from your version)
# ---------------------------------------------------------------------------

CONTROL_CHAR_RE = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f]')


def clean_field(value):
    return CONTROL_CHAR_RE.sub('', value)


EMAIL_RE = re.compile(
    r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*\.[a-zA-Z]{2,24}'
)
URL_LIKE_RE = re.compile(r'([a-zA-Z][a-zA-Z0-9+.-]*://|www\.)', re.IGNORECASE)
DOMAINISH_RE = re.compile(r'^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/.+$')
# Bare hostname with no scheme and no path, e.g. "service.transunion.com".
# One or more dot-separated labels ending in a 2-24 char alphabetic TLD.
BARE_DOMAIN_RE = re.compile(
    r'^(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,24}$'
)
SCHEME_HOST_ONLY_RE = re.compile(r'^[a-zA-Z][a-zA-Z0-9+.-]*://[^/]+$', re.IGNORECASE)
PORT_RE = re.compile(r'^(\d{1,5})(/.*)?$')
NOISE_RE = re.compile(r'^[\W_]+$')
PROTO_PLACEHOLDER = '\x00SCHEME\x00'


def is_url_like(field):
    field = field.strip()
    if not field:
        return False
    head = field if len(field) <= _SCAN_CAP else field[:_SCAN_CAP]
    if ('://' in head or 'www.' in head.lower()) and URL_LIKE_RE.search(head):
        return True
    if '/' in field and len(field) <= _SCAN_CAP and DOMAINISH_RE.match(field):
        return True
    # Bare host like "service.transunion.com" — no scheme, no path. These are
    # site context, not password, so treat them as URL-like and deprioritize.
    if len(field) <= _SCAN_CAP and BARE_DOMAIN_RE.match(field):
        return True
    return False


def is_port_continuation(prev_field, field):
    prev_field = prev_field.strip()
    field = field.strip()
    if not SCHEME_HOST_ONLY_RE.match(prev_field):
        return False
    m = PORT_RE.match(field)
    if not m:
        return False
    return int(m.group(1)) <= 65535


def split_fields(line):
    protected = line.replace('://', PROTO_PLACEHOLDER)
    parts = protected.split(':')
    return [p.replace(PROTO_PLACEHOLDER, '://') for p in parts]


def classify_line(line):
    fields = split_fields(line)

    meaningful = []
    for i, f in enumerate(fields):
        f = f.strip()
        if not f or NOISE_RE.match(f):
            continue
        if i > 0 and is_port_continuation(fields[i - 1], f):
            continue
        meaningful.append((i, f))

    email_candidates = []
    for i, f in meaningful:
        if '@' not in f:
            continue
        found = EMAIL_RE.findall(f if len(f) <= _SCAN_CAP else f[:_SCAN_CAP])
        if len(found) == 1:
            email_candidates.append((i, found[0]))

    if not email_candidates:
        return None

    email_idx, email = email_candidates[0]
    others = [(i, f) for i, f in meaningful if i != email_idx]
    other_candidates = [(i, f) for i, f in others if not is_url_like(f)]

    if not other_candidates:
        return (email, "", [])

    other_candidates.sort(key=lambda pair: abs(pair[0] - email_idx))
    password = other_candidates[0][1]
    extra = [f for _, f in other_candidates[1:]]
    return (email, password, extra)


def _pair_digest(email, password):
    h = hashlib.blake2b(digest_size=16)
    h.update(email.encode("utf-8"))
    h.update(b"\x00")
    h.update(password.encode("utf-8"))
    return h.digest()


def _classify_and_clean(line):
    result = classify_line(line)
    if result is None:
        return None
    email, password, _extra = result
    email = clean_field(email.lower())
    password = clean_field(password)
    if len(email) > LIMIT or len(password) > LIMIT:
        return ('over', len(email), len(password), email[:80])
    return ('ok', _pair_digest(email, password), email, password)


# ---------------------------------------------------------------------------
# Chunking (unchanged)
# ---------------------------------------------------------------------------

def _compute_chunks(path, n):
    size = os.path.getsize(path)
    if size == 0 or n <= 1:
        return [(0, size)]
    step = size // n
    bounds = [i * step for i in range(n)] + [size]
    return [(bounds[i], bounds[i + 1]) for i in range(n)]


def _aligned_start(f, start):
    if start == 0:
        return 0
    f.seek(start - 1)
    prev = f.read(1)
    if prev != b'\n':
        f.readline()
    return f.tell()


# ---------------------------------------------------------------------------
# Phase 1: parse a byte-range chunk, route kept rows into per-bucket temp files
# ---------------------------------------------------------------------------

def _worker_partition(spec):
    idx, start, end, input_file, bucket_dir, log_path = spec
    _raise_fd_limit(NUM_BUCKETS + 128)
    bad = oversize = 0
    paths = [os.path.join(bucket_dir, f"w{idx}_b{j}.csv") for j in range(NUM_BUCKETS)]
    files = [open(p, 'w', encoding='utf-8', newline='') for p in paths]
    writers = [csv.writer(f, quoting=csv.QUOTE_ALL) for f in files]
    try:
        with open(input_file, 'rb') as f, \
                open(log_path, 'w', encoding='utf-8') as lout:
            pos = _aligned_start(f, start)
            while pos < end:
                raw = f.readline()
                if not raw:
                    break
                pos = f.tell()
                line = raw.decode('utf-8', 'replace').strip()
                if not line:
                    continue
                res = _classify_and_clean(line)
                if res is None:
                    bad += 1
                elif res[0] == 'over':
                    _, elen, mlen, preview = res
                    oversize += 1
                    lout.write(
                        f"oversize: email_len={elen} password_len={mlen} "
                        f"email={preview!r}\n"
                    )
                else:
                    _, digest, email, password = res
                    j = int.from_bytes(digest[:2], 'big') % NUM_BUCKETS
                    # Store only email+password; phase 2 recomputes the digest.
                    # Saves ~32 bytes/row of temp disk at 150GB+ scale.
                    writers[j].writerow([email, password])
    finally:
        for f in files:
            f.close()
    return {'idx': idx, 'bad': bad, 'oversize': oversize, 'log': log_path}


# ---------------------------------------------------------------------------
# Phase 2: dedup one bucket in RAM (bounded), write a deduped shard
# ---------------------------------------------------------------------------

def _dedup_bucket(spec):
    j, in_paths, shard_path = spec
    seen = set()
    unique = good = email_only = total = 0
    with open(shard_path, 'w', encoding='utf-8', newline='') as sout:
        cw = csv.writer(sout, escapechar='\\')
        for p in in_paths:
            if not os.path.exists(p):
                continue
            with open(p, newline='', encoding='utf-8') as din:
                for email, password in csv.reader(din):
                    total += 1
                    d = _pair_digest(email, password)
                    if d in seen:
                        continue
                    seen.add(d)
                    domain = email.split('@')[-1] if '@' in email else ""
                    cw.writerow([email, password, email, domain])
                    unique += 1
                    if password == "":
                        email_only += 1
                    else:
                        good += 1
            os.remove(p)  # free this bucket's input as soon as it's consumed
    return {'j': j, 'unique': unique, 'good': good,
            'email_only': email_only, 'total': total}


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def process(output_dir=None, workers=None, input_file="all_emails.txt"):
    if not os.path.isfile(input_file):
        print(f"File not found: {input_file}")
        return

    file_name = os.path.basename(input_file)
    base_name, _ext = os.path.splitext(file_name)
    if output_dir is None:
        output_dir = os.path.dirname(os.path.abspath(input_file))
    os.makedirs(output_dir, exist_ok=True)
    good_path = os.path.join(output_dir, f"{base_name}_parsed.csv")
    log_path = os.path.join(output_dir, f"{base_name}_removed.log")

    if workers is None:
        workers = max(1, (os.cpu_count() or 1) - 2)
    _raise_fd_limit(NUM_BUCKETS + 128)
    print(f"Processing {file_name} with {workers} worker(s), "
          f"{NUM_BUCKETS} buckets ...")

    tmpdir = tempfile.mkdtemp(prefix="parse_chunks_", dir=output_dir)
    try:
        # ---- Phase 1: parallel parse + partition ----------------------------
        chunks = _compute_chunks(input_file, workers)
        specs = [
            (i, start, end, input_file, tmpdir,
             os.path.join(tmpdir, f"log_{i}.txt"))
            for i, (start, end) in enumerate(chunks)
        ]
        if workers == 1:
            p1 = [_worker_partition(specs[0])]
        else:
            with mp.Pool(processes=workers) as pool:
                p1 = pool.map(_worker_partition, specs)

        bad = sum(r['bad'] for r in p1)
        oversize = sum(r['oversize'] for r in p1)
        n_chunks = len(specs)

        # ---- Phase 2: parallel per-bucket dedup -----------------------------
        bucket_specs = [
            (j,
             [os.path.join(tmpdir, f"w{i}_b{j}.csv") for i in range(n_chunks)],
             os.path.join(tmpdir, f"s_{j}.csv"))
            for j in range(NUM_BUCKETS)
        ]
        if workers == 1:
            p2 = [_dedup_bucket(s) for s in bucket_specs]
        else:
            with mp.Pool(processes=workers) as pool:
                p2 = pool.map(_dedup_bucket, bucket_specs)

        good = sum(r['good'] for r in p2)
        email_only = sum(r['email_only'] for r in p2)
        unique_total = sum(r['unique'] for r in p2)
        routed_total = sum(r['total'] for r in p2)
        dup = routed_total - unique_total

        # ---- Finalize: header + concat shards, then concat logs -------------
        with open(good_path, "w", encoding="utf-8", newline="") as out:
            csv.writer(out, escapechar='\\').writerow(
                ["cac_email", "cac_password", "cac_username", "cac_email_domain"])
            for j in range(NUM_BUCKETS):
                sp = os.path.join(tmpdir, f"s_{j}.csv")
                with open(sp, "r", encoding="utf-8", newline="") as sin:
                    shutil.copyfileobj(sin, out, length=1024 * 1024)
                os.remove(sp)  # free each shard right after it's merged

        with open(log_path, "w", encoding="utf-8") as final_log:
            for r in p1:
                with open(r['log'], encoding='utf-8') as lf:
                    shutil.copyfileobj(lf, final_log)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    total = good + email_only
    print(f"Done {file_name} -> {good_path}")
    print(f"  clean rows written : {total}")
    print(f"    with a password   : {good}")
    print(f"    email only       : {email_only}")
    print(f"  duplicates skipped : {dup}")
    print(f"  oversize removed   : {oversize}  (log: {log_path})")
    print(f"  bad lines skipped  : {bad}")
    return {"good": good, "email_only": email_only, "dup": dup,
            "oversize": oversize, "bad": bad}


if __name__ == "__main__":
    output_dir = sys.argv[1] if len(sys.argv) > 1 else None
    workers = int(sys.argv[2]) if len(sys.argv) > 2 else None
    in_file = sys.argv[3] if len(sys.argv) > 3 else "all_emails.txt"
    process(output_dir, workers, in_file)
