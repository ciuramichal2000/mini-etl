import pytest

from minietl.errors import ConfigError, RowError, SourceError
from minietl.steps import CastSpec, build_step, cast_value


def make(spec, tmp_path):
    return build_step(spec, 1, tmp_path)


# ---- rename ---------------------------------------------------------------------------------

def test_rename_changes_row_and_schema_keeping_column_order(tmp_path):
    step = make({"type": "rename", "mapping": {"Old Name": "name"}}, tmp_path)
    schema = step.bind({"id": "TEXT", "Old Name": "TEXT"})
    assert list(schema) == ["id", "name"]
    assert step.apply({"id": "1", "Old Name": "Ada"}) == {"id": "1", "name": "Ada"}


def test_rename_of_unknown_column_fails_before_any_row(tmp_path):
    step = make({"type": "rename", "mapping": {"nope": "x"}}, tmp_path)
    with pytest.raises(ConfigError, match="unknown column"):
        step.bind({"id": "TEXT"})


def test_rename_onto_an_existing_column_is_refused(tmp_path):
    step = make({"type": "rename", "mapping": {"a": "b"}}, tmp_path)
    with pytest.raises(ConfigError, match="same name"):
        step.bind({"a": "TEXT", "b": "TEXT"})


def test_rename_can_swap_two_columns(tmp_path):
    step = make({"type": "rename", "mapping": {"a": "b", "b": "a"}}, tmp_path)
    assert list(step.bind({"a": "TEXT", "b": "INTEGER"}).items()) == [("b", "TEXT"), ("a", "INTEGER")]
    assert step.apply({"a": 1, "b": 2}) == {"b": 1, "a": 2}


# ---- cast -----------------------------------------------------------------------------------

@pytest.mark.parametrize(
    "to, value, expected",
    [
        ("int", "42", 42),
        ("int", " -7 ", -7),
        ("int", "+5", 5),
        ("int", "007", 7),
        ("int", "9223372036854775807", 2**63 - 1),
        ("int", "-9223372036854775808", -(2**63)),
        ("float", "3.5", 3.5),
        ("float", ".5", 0.5),
        ("float", "-2.", -2.0),
        ("float", "1e3", 1000.0),
        ("float", "12", 12.0),
        ("bool", "YES", True),
        ("bool", "1", True),
        ("bool", "n", False),
        ("bool", "0", False),
        ("str", 5, "5"),
        ("int", None, None),
        ("date", None, None),
    ],
)
def test_cast_accepts_exactly_the_documented_spellings(to, value, expected):
    result = cast_value(value, CastSpec(to))
    assert result == expected
    assert type(result) is type(expected)  # 1 == True in Python, so compare the type too


@pytest.mark.parametrize(
    "to, value",
    [
        ("int", "1_000"),                  # int() would accept this
        ("int", "12.0"),
        ("int", "1e3"),
        ("int", ""),
        ("int", "abc"),
        ("int", "9223372036854775808"),    # one past the 64-bit range SQLite stores
        ("float", "nan"),                  # float() would accept these
        ("float", "inf"),
        ("float", "1_0"),
        ("float", "1e999"),                # parses, but overflows to infinity
        ("float", "1.2.3"),
        ("float", "abc"),
        ("bool", "maybe"),
        ("date", "2024-02-30"),
        ("date", "2024/01/05"),
        ("date", "tomorrow"),
    ],
)
def test_cast_rejects_everything_else(to, value):
    with pytest.raises(ValueError):
        cast_value(value, CastSpec(to))


def test_date_cast_normalises_to_iso_using_the_configured_format():
    # Day first: 05/01/2024 is 5 January, not May 1st.
    assert cast_value("05/01/2024", CastSpec("date", "%d/%m/%Y")) == "2024-01-05"
    assert cast_value("2024-1-5", CastSpec("date")) == "2024-01-05"
    assert cast_value("29/02/2024", CastSpec("date", "%d/%m/%Y")) == "2024-02-29"  # leap year
    with pytest.raises(ValueError):
        cast_value("29/02/2023", CastSpec("date", "%d/%m/%Y"))


def test_a_date_format_typo_fails_when_the_config_loads_not_on_every_row(tmp_path):
    for bad in ("%Q", "%-d/%m/%Y"):
        with pytest.raises(ConfigError, match="not a usable date format"):
            make({"type": "cast", "columns": {"d": {"to": "date", "format": bad}}}, tmp_path)
    # Formats that are fine must not be refused by the check (including zone directives).
    for good in ("%d/%m/%Y", "%Y%m%d", "%b %d %Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %Z", "%Y-%m-%d%z"):
        make({"type": "cast", "columns": {"d": {"to": "date", "format": good}}}, tmp_path)


def test_cast_step_reports_the_column_and_leaves_other_columns_alone(tmp_path):
    step = make({"type": "cast", "columns": {"age": "int", "score": "float"}}, tmp_path)
    assert step.apply({"age": "7", "score": "1.5", "name": "x"}) == {"age": 7, "score": 1.5, "name": "x"}
    with pytest.raises(RowError, match="column 'score'"):
        step.apply({"age": "7", "score": "high", "name": "x"})


def test_cast_step_sets_sql_types_and_checks_columns(tmp_path):
    step = make({"type": "cast", "columns": {"n": "int", "f": "float", "b": "bool", "d": "date", "s": "str"}}, tmp_path)
    schema = {name: "TEXT" for name in "nfbds"}
    assert step.bind(schema) == {"n": "INTEGER", "f": "REAL", "b": "INTEGER", "d": "TEXT", "s": "TEXT"}
    with pytest.raises(ConfigError, match="unknown column"):
        step.bind({"n": "TEXT"})


@pytest.mark.parametrize(
    "spec, message",
    [
        ({"type": "cast", "columns": {"a": "decimal"}}, "must be one of"),
        ({"type": "cast", "columns": {"a": {"to": "int", "format": "%Y"}}}, "only applies to date"),
        ({"type": "cast", "columns": {"a": {"to": "int", "fromat": "x"}}}, "unknown key"),
        ({"type": "cast", "columns": {}}, "non-empty object"),
        ({"type": "cast"}, "missing key"),
    ],
)
def test_malformed_cast_steps_are_refused(tmp_path, spec, message):
    with pytest.raises(ConfigError, match=message):
        make(spec, tmp_path)


# ---- filter ---------------------------------------------------------------------------------

def test_filter_drops_false_rows_and_keeps_the_boundary(tmp_path):
    step = make({"type": "filter", "expr": "age >= 18"}, tmp_path)
    assert step.apply({"age": 17}) is None
    assert step.apply({"age": 18}) == {"age": 18}


def test_filter_in_reject_mode_raises_instead_of_dropping(tmp_path):
    step = make({"type": "filter", "expr": "amount >= 0", "on_false": "reject"}, tmp_path)
    assert step.apply({"amount": 0}) == {"amount": 0}
    with pytest.raises(RowError, match="condition not met: amount >= 0"):
        step.apply({"amount": -1})


def test_filter_on_a_null_value_rejects_rather_than_silently_dropping(tmp_path):
    step = make({"type": "filter", "expr": "age >= 18"}, tmp_path)
    with pytest.raises(RowError, match="cannot evaluate"):
        step.apply({"age": None})


def test_filter_naming_an_unknown_column_fails_at_bind_time(tmp_path):
    step = make({"type": "filter", "expr": "agee >= 18"}, tmp_path)
    with pytest.raises(ConfigError, match="agee"):
        step.bind({"age": "INTEGER"})


# ---- derive ---------------------------------------------------------------------------------

def test_derive_adds_a_column_without_type_affinity(tmp_path):
    step = make({"type": "derive", "column": "total", "expr": "price * qty"}, tmp_path)
    assert step.bind({"price": "REAL", "qty": "INTEGER"}) == {"price": "REAL", "qty": "INTEGER", "total": ""}
    assert step.apply({"price": 2.5, "qty": 4}) == {"price": 2.5, "qty": 4, "total": 10.0}


def test_derive_can_overwrite_a_column_and_resets_its_type(tmp_path):
    step = make({"type": "derive", "column": "name", "expr": "upper(name)"}, tmp_path)
    assert step.bind({"name": "TEXT"}) == {"name": ""}
    assert step.apply({"name": "ada"}) == {"name": "ADA"}


@pytest.mark.parametrize(
    "expr, row, message",
    [
        ("(a, a)", {"a": 1}, "type tuple"),                      # SQLite cannot bind a tuple
        ("a * 10", {"a": 1e308}, "not a finite number"),         # overflows to infinity
        ("a * 4611686018427387904", {"a": 3}, "64 bits"),        # 3 * 2**62 > 2**63 - 1
    ],
)
def test_derive_refuses_values_sqlite_cannot_store(tmp_path, expr, row, message):
    step = make({"type": "derive", "column": "out", "expr": expr}, tmp_path)
    with pytest.raises(RowError, match=message):
        step.apply(row)


def test_derive_keeps_booleans_and_nulls(tmp_path):
    step = make({"type": "derive", "column": "flag", "expr": "a > 1"}, tmp_path)
    assert step.apply({"a": 2})["flag"] is True
    step = make({"type": "derive", "column": "x", "expr": "coalesce(a, None)"}, tmp_path)
    assert step.apply({"a": None})["x"] is None


# ---- lookup ---------------------------------------------------------------------------------

@pytest.fixture
def countries(tmp_path):
    (tmp_path / "countries.csv").write_text(
        "code,name,region\nGB,United Kingdom,EU\nUS,United States,NA\nXX,,none\n", encoding="utf-8"
    )
    return tmp_path


def lookup_spec(**overrides):
    spec = {
        "type": "lookup", "path": "countries.csv", "on": "cc", "key": "code",
        "columns": {"country": "name"},
    }
    spec.update(overrides)
    return spec


def test_lookup_joins_the_requested_columns(countries):
    step = make(lookup_spec(columns={"country": "name", "area": "region"}), countries)
    assert step.apply({"cc": "GB"}) == {"cc": "GB", "country": "United Kingdom", "area": "EU"}


def test_lookup_miss_rejects_by_default_and_names_the_value(countries):
    step = make(lookup_spec(), countries)
    with pytest.raises(RowError) as excinfo:
        step.apply({"cc": "ZZ"})
    assert "cc='ZZ'" in excinfo.value.reason and "countries.csv" in excinfo.value.reason


def test_lookup_miss_can_be_a_left_join(countries):
    step = make(lookup_spec(on_missing="null"), countries)
    assert step.apply({"cc": "ZZ"}) == {"cc": "ZZ", "country": None}
    assert step.apply({"cc": None}) == {"cc": None, "country": None}


def test_lookup_null_join_value_never_matches(countries):
    step = make(lookup_spec(), countries)
    with pytest.raises(RowError):
        step.apply({"cc": None})


def test_lookup_blank_cell_in_reference_file_joins_as_null(countries):
    step = make(lookup_spec(), countries)
    assert step.apply({"cc": "XX"}) == {"cc": "XX", "country": None}


def test_lookup_matches_integers_against_text_keys(tmp_path):
    (tmp_path / "ids.csv").write_text("id,label\n42,answer\n", encoding="utf-8")
    step = make(
        {"type": "lookup", "path": "ids.csv", "on": "n", "key": "id", "columns": {"label": "label"}}, tmp_path
    )
    assert step.apply({"n": 42}) == {"n": 42, "label": "answer"}


def test_lookup_reads_a_semicolon_file_when_told_to(tmp_path):
    (tmp_path / "c.csv").write_text("code;name\nGB;UK\n", encoding="utf-8")
    step = make(lookup_spec(path="c.csv", delimiter=";"), tmp_path)
    assert step.apply({"cc": "GB"})["country"] == "UK"


def test_lookup_reads_a_file_in_another_encoding_when_told_to(tmp_path):
    (tmp_path / "pl.csv").write_bytes("code,name\nPL,Łódź\n".encode("cp1250"))
    step = make(lookup_spec(path="pl.csv", encoding="cp1250"), tmp_path)
    assert step.apply({"cc": "PL"})["country"] == "Łódź"
    with pytest.raises(SourceError):  # the same bytes are not valid UTF-8
        make(lookup_spec(path="pl.csv"), tmp_path)


def test_lookup_matching_is_exact_and_case_sensitive(countries):
    step = make(lookup_spec(on_missing="null"), countries)
    assert step.apply({"cc": "GB"})["country"] == "United Kingdom"
    assert step.apply({"cc": "gb"})["country"] is None


def test_lookup_bind_adds_text_columns_and_refuses_overwrites(countries):
    step = make(lookup_spec(), countries)
    assert step.bind({"cc": "TEXT"}) == {"cc": "TEXT", "country": "TEXT"}
    with pytest.raises(ConfigError, match="overwrite"):
        step.bind({"cc": "TEXT", "country": "TEXT"})
    with pytest.raises(ConfigError, match="unknown column"):
        step.bind({"other": "TEXT"})


def test_lookup_file_problems_abort_instead_of_joining_wrongly(tmp_path):
    def build(text):
        (tmp_path / "bad.csv").write_text(text, encoding="utf-8")
        return make(lookup_spec(path="bad.csv"), tmp_path)

    with pytest.raises(SourceError, match="duplicate key 'GB'"):
        build("code,name\nGB,United Kingdom\nGB,Great Britain\n")
    with pytest.raises(SourceError, match="empty key"):
        build("code,name\n,Nowhere\n")
    with pytest.raises(SourceError, match="expected 2 fields"):
        build("code,name\nGB\n")
    with pytest.raises(ConfigError, match="no column"):
        build("code,title\nGB,United Kingdom\n")
    with pytest.raises(SourceError, match="cannot open"):
        make(lookup_spec(path="missing.csv"), tmp_path)


# ---- building -------------------------------------------------------------------------------

@pytest.mark.parametrize(
    "spec, message",
    [
        ("rename", "must be an object"),
        ({"mapping": {}}, "unknown type None"),
        ({"type": "explode"}, "unknown type 'explode'"),
        ({"type": ["rename"]}, "unknown type"),
        ({"type": "rename"}, "missing key"),
        ({"type": "rename", "mapping": {"a": "b"}, "extra": 1}, "unknown key"),
        ({"type": "rename", "mapping": {"a": ""}}, "non-empty strings"),
        ({"type": "filter", "expr": "a > 1", "on_false": "explode"}, "must be one of"),
        ({"type": "derive", "column": "", "expr": "1"}, "non-empty string"),
    ],
)
def test_malformed_steps_are_refused_with_a_reason(tmp_path, spec, message):
    with pytest.raises(ConfigError, match=message):
        make(spec, tmp_path)


def test_build_step_labels_follow_position_and_type(tmp_path):
    step = build_step({"type": "filter", "expr": "a > 1"}, 3, tmp_path)
    assert step.label == "step 3 (filter)"
