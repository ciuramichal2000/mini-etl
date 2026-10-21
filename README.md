# mini-etl

A small, config-driven ETL pipeline: it reads a CSV file, runs a list of transform steps (rename, cast, filter, derive, lookup join against a second CSV) and loads the result into SQLite. The pipeline is described by a JSON config, a re-run of the same input never duplicates rows, and every run is recorded in a log table. It uses only the Python standard library.

## Interesting engineering

- **Reject a row, abort a run.** Bad data (a non-numeric age, an unmatched lookup, a duplicate or empty key, a wrong field count, a row the target table refuses with NOT NULL / CHECK / UNIQUE) raises `RowError`, which sets that one row aside with its line number, stage and reason. Setup problems (unknown column in a step, unreadable file, incompatible target table) raise `MiniEtlError` subclasses and stop the run. Steps are checked against the column layout before the first row is read, and expressions, date formats, delimiters and encodings are validated when the config loads, so a typo fails immediately instead of rejecting every row.
- **Idempotent keyed load.** Each row is looked up by its key and classified as inserted, updated or unchanged (the comparison is NULL-safe), so re-running identical input writes nothing and reports `unchanged`.
- **One transaction per run.** Data, run-log row and rejects are written together; if the load fails midway everything rolls back and a separate `failed` row is added to the run log.
- **Dry run that cannot write.** The database is opened read-only (or not at all if it does not exist), and the result is classified by the same code path as a real run.
- **No `eval`.** `filter` and `derive` expressions are parsed with `ast` and compiled from a whitelist (arithmetic, comparisons including `in` and `is None`, `and`/`or`/`not`, `a if c else b`, tuples, `upper`, `lower`, `strip`, `len`, `abs`, `round`, `coalesce`); attribute access, subscripts, lambdas, `**` and `is` against anything but `None` are refused when the config loads. Casts are strict as well: `"1_000"`, `"nan"` and `"12.0"`-as-int are rejected, and values SQLite cannot store faithfully (NaN, 65-bit integers, tuples) reject the row instead of corrupting the load.

## Usage

Needs Python 3.11 or newer and nothing else. Run the commands below from this folder (there is nothing to install).

`examples/config.json`:

```json
{
  "name": "customers",
  "source": {"path": "customers.csv"},
  "steps": [
    {"type": "rename", "mapping": {"Cust ID": "id", "Full Name": "name", "Age": "age", "Country": "country_code", "Joined": "joined"}},
    {"type": "cast", "columns": {"id": "int", "age": "int", "joined": {"to": "date", "format": "%d/%m/%Y"}}},
    {"type": "filter", "expr": "age >= 18"},
    {"type": "derive", "column": "name_upper", "expr": "upper(name)"},
    {"type": "lookup", "path": "countries.csv", "on": "country_code", "key": "code", "columns": {"country_name": "name"}}
  ],
  "target": {"database": "customers.db", "table": "customers", "key": "id"}
}
```

```
$ python -m minietl run examples/config.json --dry-run
pipeline customers (dry run, nothing written)
read 6: inserted 2, updated 0, unchanged 0, filtered 1, rejected 3
  rejected line 5 [step 2 (cast)]: column 'age': 'abc' is not an integer
  rejected line 6 [step 5 (lookup)]: no match for country_code='ZZ' in countries.csv
  rejected line 7 [load]: duplicate key 1; first seen on line 2

$ python -m minietl run examples/config.json       # writes examples/customers.db
$ python -m minietl run examples/config.json       # same input again: "unchanged 2"
```

Relative paths in the config (source, lookup files, database) are resolved next to the config file. The exit code is 0 when the run completed (even with rejected rows) and 1 when it was aborted; the reason goes to stderr.

From Python:

```python
from minietl import load_config_file, run_pipeline

result = run_pipeline(load_config_file("examples/config.json"), base_dir="examples")
print(result.rows_loaded, result.rows_rejected)
```

### Steps

| type | keys | effect |
| --- | --- | --- |
| `rename` | `mapping` | `{old: new}`; swaps are fine, collisions are refused |
| `cast` | `columns` | `{column: "int"/"float"/"str"/"bool"/"date"}`; dates also accept `{"to": "date", "format": "%d/%m/%Y"}` (checked when the config loads) and are stored as ISO `YYYY-MM-DD` |
| `filter` | `expr`, optional `on_false` | rows where `expr` is false are dropped (counted as `filtered`), or rejected with `"on_false": "reject"` |
| `derive` | `column`, `expr` | adds (or replaces) a column; the column has no fixed SQLite type |
| `lookup` | `path`, `on`, `key`, `columns`, optional `on_missing`, `delimiter`, `encoding` | joins `{new_column: column_in_lookup_file}`; a miss rejects the row, or yields NULLs with `"on_missing": "null"` |

`source` accepts `path`, `delimiter` (default `,`; one character, not a line break or `"`) and `encoding` (default `utf-8-sig`; must be a text encoding). `target` takes `database`, `table` and a single `key` column. Unknown config keys are errors, not ignored.

### What gets stored

- The target table is created on first use with `key` as its primary key; later runs require the same columns and the same single-column primary key.
- `etl_run_log`: one row per run with `started_at`, `finished_at`, `status` (`success` or `failed`), `rows_read`, `rows_loaded` (inserted + updated), `rows_inserted`, `rows_updated`, `rows_unchanged`, `rows_filtered`, `rows_rejected` and `error`. Every row read lands in exactly one of inserted, updated, unchanged, filtered or rejected. The counts of a failed run are NULL.
- `etl_rejects`: one row per rejected input line with `run_id`, `line_number`, `stage`, `reason` and the original cells as JSON.
- A dry run writes none of this.

## Limitations

- Load is upsert only: rows that disappear from the input stay in the table, and a table whose columns no longer match the pipeline aborts the run instead of being migrated.
- One key column; no composite keys.
- Expressions can only name columns that are valid identifiers (use `rename` first). Blank CSV cells become NULL and cells are stripped of surrounding spaces.
- Lookup files are held in memory and matched exactly (case-sensitive) on the text form of the value; they must have unique keys.
- A file-level problem aborts the whole run and rolls it back, however late it shows up: undecodable bytes or malformed CSV on line 40,000 discard the rows loaded before it.
- A dry run reads the target table but does not execute its constraints, so it can count as inserted a row that a real run would reject (NOT NULL, CHECK, UNIQUE on a table created outside this tool). It does fail when the database folder does not exist or the table does not match.
- Rows are looked up and written one at a time and nothing has been benchmarked. The input is streamed, but the keys seen so far are kept in memory to detect duplicates.
- A reject's line number is the line where the record ends, which differs from its first line for quoted multi-line cells.
- A config that cannot be parsed starts no run, so it is not logged. Problems found after the run has started, such as an unknown column in a step, are logged as failed.

## Tests

```
python -m pytest -q
```

The tests use temporary directories and need no network. The tests that check timestamps inject a fixed clock; no test asserts on the current date or time. They cover the expression language, the CSV reader, each step, the end-to-end pipeline against SQLite (idempotent re-runs, dry run, rollback of a failure midway, the run log) and the command line. One test checks that the config and the dry-run output shown above are what `examples/` and the CLI really produce.

Layout: `minietl/` (the main modules are `config.py`, `steps.py`, `expressions.py`, `csvsource.py`, `target.py`, `pipeline.py` and `__main__.py`), `tests/`, `examples/`.

Built with AI coding assistance.
