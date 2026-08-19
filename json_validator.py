'''
Out-of-core validator for the CAC JSON files, using DuckDB.

Why DuckDB: the pandas version held a growing email set in RAM, which on a
30 GB machine blew into 40+ GB of swap and got OOM-killed near the end.
DuckDB streams the files, does the heavy aggregates (incl. exact
COUNT(DISTINCT email)) in C, and SPILLS to disk when it exceeds the memory
limit you set -- so it finishes instead of thrashing.

Output format matches the original pandas-based json_validator.py exactly,
since cac_validator.py greps specific lines (e.g. 'cac_email\\t<count>',
'cac_password\\t<count>', 'Number of unique usernames', 'Possible password
type') out of this script's stdout.
'''
import os
import re
import time
import duckdb

# ------------------------------- config ------------------------------
MEM_LIMIT = '10GB'                # keep well under physical RAM
TEMP_DIR  = '/tmp/duckdb_tmp'     # fast local disk with room to spill
SHOW_INVALID_SAMPLES = True       # print a few sample bad DOB/phone rows
SAMPLE_LIMIT = 100
PHONE_PATTERN = r'^\+?[0-9\s().-]{7,15}$'
# ---------------------------------------------------------------------

files = sorted(x for x in os.listdir('.') if re.findall(r'_parsed\d+\.json', x))
print(files)
if not files:
    raise SystemExit('No *_parsed<N>.json files found in the current directory.')

os.makedirs(TEMP_DIR, exist_ok=True)
con = duckdb.connect()
con.execute(f"SET memory_limit='{MEM_LIMIT}';")
con.execute(f"SET temp_directory='{TEMP_DIR}';")
con.execute("SET preserve_insertion_order=false;")

file_list = "[" + ",".join("'" + f.replace("'", "''") + "'" for f in files) + "]"
SRC = (f"read_json_auto({file_list}, format='newline_delimited', "
       f"union_by_name=true, maximum_object_size=104857600)")

# 1) discover columns (cheap schema-only scan)
cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {SRC}").fetchall()]

# 2) one aggregate pass: metrics + per-column non-null counts
metric_exprs = ["count(*) AS total_rows"]
if 'cac_email' in cols:
    metric_exprs.append("count(*) - count(DISTINCT cac_email) AS repeated_emails")
if 'cac_email' in cols and 'cac_username' in cols:
    metric_exprs.append("count(*) FILTER (WHERE cac_email <> cac_username) AS username_differs")
if 'cac_email' in cols and 'cac_password' in cols:
    metric_exprs.append("count(*) FILTER (WHERE cac_email = cac_password) AS pw_equals_email")
if 'cac_password' in cols:
    metric_exprs.append("max(length(CAST(cac_password AS VARCHAR))) AS max_pw_len")
if 'dob_birthday_month' in cols:
    metric_exprs.append(
        "count(*) FILTER (WHERE TRY_CAST(dob_birthday_month AS BIGINT) NOT BETWEEN 1 AND 12) AS invalid_months"
    )
if 'dob_birthday_day' in cols:
    metric_exprs.append(
        "count(*) FILTER (WHERE TRY_CAST(dob_birthday_day AS BIGINT) NOT BETWEEN 1 AND 31) AS invalid_days"
    )
if 'phone_number' in cols:
    metric_exprs.append(
        "count(*) FILTER (WHERE phone_number IS NOT NULL AND "
        f"NOT regexp_matches(CAST(phone_number AS VARCHAR), '{PHONE_PATTERN}')) AS invalid_phones"
    )

col_exprs = ", ".join(f'count("{c}") AS "cnt::{c}"' for c in cols)
query = "SELECT " + ", ".join(metric_exprs) + ", " + col_exprs + f" FROM {SRC}"

t0 = time.time()
row = con.execute(query).fetchone()
res = dict(zip([d[0] for d in con.description], row))
elapsed = time.time() - t0

DUPE_EMAILS = res.get('repeated_emails', 0) or 0
USER_COUNT = res.get('username_differs', 0) or 0
PASSW_EMAILS = res.get('pw_equals_email', 0) or 0
MAX_PW_LEN = res.get('max_pw_len') or 0
PASSW_TYPE = ('hashed' if MAX_PW_LEN > 30 else 'plain-text') if MAX_PW_LEN else ''
INVALID_MON_COUNT = res.get('invalid_months', 0) or 0
INVALID_DAY_COUNT = res.get('invalid_days', 0) or 0
INVALID_PHONE_COUNT = res.get('invalid_phones', 0) or 0

# ------------------------------ output -------------------------------
# Same section headers / tab layout as the original script, so
# cac_validator.py's parsing of this stdout keeps working unchanged.

print('\nCOLUMNS AND COUNTS')
print('-' * 18)
for c in cols:
    print(f'{c.ljust(25)}\t{res[f"cnt::{c}"]}')

print('\nADDITIONAL METRICS')
print('-' * 18)
if len(cols) > 28:
    print("\033[1;31mThe number of columns exceed 28\033[0m")
print(f"{'Repeated Emails':<25}\t{DUPE_EMAILS}")
print(f"{'Number of unique usernames':<25}\t{USER_COUNT}")
if PASSW_EMAILS:
    print(f"\033[1;31m{'Passwords with Emails':<25}\t{PASSW_EMAILS}\033[0m")
if PASSW_TYPE:
    print(f"{'Possible password type':<25}\t{PASSW_TYPE}")
if INVALID_PHONE_COUNT:
    print(f"{'Invalid phone numbers':<25}\t{INVALID_PHONE_COUNT}")

if INVALID_MON_COUNT:
    print("\nInvalid value in DOB_MONTH")
    if SHOW_INVALID_SAMPLES:
        sample = con.execute(
            f"SELECT cac_email, dob_birthday_month FROM {SRC} "
            f"WHERE TRY_CAST(dob_birthday_month AS BIGINT) NOT BETWEEN 1 AND 12 "
            f"LIMIT {SAMPLE_LIMIT}"
        ).fetchall()
        print(sample)
    else:
        print(f"({INVALID_MON_COUNT} invalid rows -- set SHOW_INVALID_SAMPLES=True to list a sample)")

if INVALID_DAY_COUNT:
    print("\nInvalid value in DOB_DAY")
    if SHOW_INVALID_SAMPLES:
        sample = con.execute(
            f"SELECT cac_email, dob_birthday_day FROM {SRC} "
            f"WHERE TRY_CAST(dob_birthday_day AS BIGINT) NOT BETWEEN 1 AND 31 "
            f"LIMIT {SAMPLE_LIMIT}"
        ).fetchall()
        print(sample)
    else:
        print(f"({INVALID_DAY_COUNT} invalid rows -- set SHOW_INVALID_SAMPLES=True to list a sample)")

# Phone-number validity report -- mirrors the pandas script's per-file
# "Invalid phone numbers:" listing / "All phone numbers are valid" message,
# but done once as a single global pass instead of once per file.
if 'phone_number' in cols:
    if INVALID_PHONE_COUNT:
        print("\nInvalid phone numbers")
        if SHOW_INVALID_SAMPLES:
            sample = con.execute(
                f"SELECT cac_email, phone_number FROM {SRC} "
                f"WHERE phone_number IS NOT NULL AND "
                f"NOT regexp_matches(CAST(phone_number AS VARCHAR), '{PHONE_PATTERN}') "
                f"LIMIT {SAMPLE_LIMIT}"
            ).fetchall()
            print(sample)
        else:
            print(f"({INVALID_PHONE_COUNT} invalid rows -- set SHOW_INVALID_SAMPLES=True to list a sample)")
    else:
        print("\nAll phone numbers are valid (excluding NaNs).")

print(f"\n(scan completed in {elapsed:,.0f}s)")
