import json
import re
import shutil
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest

from minietl import ConfigError, load_config_file
from minietl.__main__ import main

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXAMPLES = PROJECT_ROOT / "examples"

# Hand-derived from examples/customers.csv: Ada and Grace load, Tim (15) is filtered,
# Linus has a non-numeric age, ZZ has no country, and the second "1" repeats a key.
EXAMPLE_ROWS = [
    (1, "Ada Lovelace", 36, "GB", "2019-12-10", "ADA LOVELACE", "United Kingdom"),
    (2, "Grace Hopper", 85, "US", "2020-02-01", "GRACE HOPPER", "United States"),
]


@pytest.fixture
def example(tmp_path):
    folder = tmp_path / "example"
    shutil.copytree(EXAMPLES, folder)
    return folder / "config.json"


def rows(db, sql):
    with closing(sqlite3.connect(db)) as conn:
        return conn.execute(sql).fetchall()


def test_run_command_loads_the_example_and_prints_a_summary(example, capsys):
    assert main(["run", str(example)]) == 0
    out = capsys.readouterr().out.splitlines()
    assert out[0] == "pipeline customers (run 1)"
    assert out[1] == "read 6: inserted 2, updated 0, unchanged 0, filtered 1, rejected 3"
    assert out[2:] == [
        "  rejected line 5 [step 2 (cast)]: column 'age': 'abc' is not an integer",
        "  rejected line 6 [step 5 (lookup)]: no match for country_code='ZZ' in countries.csv",
        "  rejected line 7 [load]: duplicate key 1; first seen on line 2",
    ]
    assert rows(example.parent / "customers.db", "SELECT * FROM customers ORDER BY id") == EXAMPLE_ROWS


def test_second_run_reports_everything_unchanged(example, capsys):
    main(["run", str(example)])
    capsys.readouterr()
    assert main(["run", str(example)]) == 0
    out = capsys.readouterr().out.splitlines()
    assert out[0] == "pipeline customers (run 2)"
    assert out[1] == "read 6: inserted 0, updated 0, unchanged 2, filtered 1, rejected 3"
    assert rows(example.parent / "customers.db", "SELECT COUNT(*) FROM customers") == [(2,)]


def test_dry_run_flag_reports_but_writes_nothing(example, capsys):
    assert main(["run", str(example), "--dry-run"]) == 0
    out = capsys.readouterr().out.splitlines()
    assert out[0] == "pipeline customers (dry run, nothing written)"
    assert out[1] == "read 6: inserted 2, updated 0, unchanged 0, filtered 1, rejected 3"
    assert not (example.parent / "customers.db").exists()


def test_only_the_first_ten_rejects_are_listed(tmp_path, capsys):
    (tmp_path / "bad.csv").write_text("id\n" + "x\n" * 12, encoding="utf-8")
    config = {
        "source": {"path": "bad.csv"},
        "steps": [{"type": "cast", "columns": {"id": "int"}}],
        "target": {"database": "bad.db", "table": "t", "key": "id"},
    }
    (tmp_path / "config.json").write_text(json.dumps(config), encoding="utf-8")
    assert main(["run", str(tmp_path / "config.json")]) == 0
    out = capsys.readouterr().out.splitlines()
    assert "rejected 12" in out[1]
    assert len([line for line in out if line.startswith("  rejected line")]) == 10
    assert out[-1] == "  ... and 2 more (see table etl_rejects)"


def test_dry_run_with_many_rejects_says_they_are_not_stored(tmp_path, capsys):
    (tmp_path / "bad.csv").write_text("id\n" + "x\n" * 12, encoding="utf-8")
    config = {
        "source": {"path": "bad.csv"},
        "steps": [{"type": "cast", "columns": {"id": "int"}}],
        "target": {"database": "bad.db", "table": "t", "key": "id"},
    }
    (tmp_path / "config.json").write_text(json.dumps(config), encoding="utf-8")
    assert main(["run", str(tmp_path / "config.json"), "--dry-run"]) == 0
    out = capsys.readouterr().out.splitlines()
    assert out[-1] == "  ... and 2 more (a dry run does not store them)"
    assert not (tmp_path / "bad.db").exists()


def test_readme_example_is_the_example_that_ships_and_its_output_is_what_the_cli_prints(example, capsys):
    readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")
    shown_config = re.search(r"```json\n(.*?)\n```", readme, re.S).group(1)
    assert json.loads(shown_config) == json.loads((EXAMPLES / "config.json").read_text(encoding="utf-8"))

    command = "$ python -m minietl run examples/config.json --dry-run\n"
    shown_output = readme.split(command, 1)[1].split("\n\n", 1)[0].splitlines()
    assert main(["run", str(example), "--dry-run"]) == 0
    assert capsys.readouterr().out.splitlines() == shown_output


def test_the_python_snippet_in_the_readme_works_as_written(example):
    from minietl import run_pipeline  # the README imports it from the package root

    result = run_pipeline(load_config_file(example), base_dir=example.parent)
    assert (result.rows_loaded, result.rows_rejected) == (2, 3)


def test_config_file_that_is_not_utf8_is_a_clean_error_not_a_traceback(tmp_path, capsys):
    path = tmp_path / "config.json"
    path.write_bytes('{"name": "zażółć"}'.encode("cp1250"))
    assert main(["run", str(path)]) == 1
    assert "not valid UTF-8" in capsys.readouterr().err
    with pytest.raises(ConfigError, match="not valid UTF-8"):
        load_config_file(path)


def test_unreadable_config_exits_with_one_and_explains_on_stderr(tmp_path, capsys):
    assert main(["run", str(tmp_path / "missing.json")]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith("error: cannot read config file")


@pytest.mark.parametrize(
    "text, message",
    [("{not json", "not valid JSON"), ('["a list"]', "must contain a JSON object")],
)
def test_malformed_config_files_exit_with_one(tmp_path, capsys, text, message):
    path = tmp_path / "config.json"
    path.write_text(text, encoding="utf-8")
    assert main(["run", str(path)]) == 1
    assert message in capsys.readouterr().err


def test_a_failed_run_exits_with_one_and_names_the_problem(example, capsys):
    (example.parent / "customers.csv").unlink()
    assert main(["run", str(example)]) == 1
    assert "cannot open CSV file" in capsys.readouterr().err


def test_missing_subcommand_is_a_usage_error():
    with pytest.raises(SystemExit) as excinfo:
        main([])
    assert excinfo.value.code == 2


def test_load_config_file_accepts_a_utf8_bom(tmp_path):
    path = tmp_path / "config.json"
    path.write_bytes(b"\xef\xbb\xbf" + json.dumps({"name": "x"}).encode("utf-8"))
    assert load_config_file(path) == {"name": "x"}


def test_load_config_file_error_is_a_config_error(tmp_path):
    with pytest.raises(ConfigError):
        load_config_file(tmp_path / "nope.json")


def test_python_dash_m_entry_point_runs_a_config_given_by_absolute_path(example):
    completed = subprocess.run(
        [sys.executable, "-m", "minietl", "run", str(example)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert completed.returncode == 0, completed.stderr
    assert "read 6: inserted 2" in completed.stdout
    assert rows(example.parent / "customers.db", "SELECT id FROM customers ORDER BY id") == [(1,), (2,)]
