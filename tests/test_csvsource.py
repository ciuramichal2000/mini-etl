import pytest

from minietl.csvsource import open_csv
from minietl.errors import RowError, SourceError


def write(path, text, encoding="utf-8"):
    path.write_bytes(text.encode(encoding))
    return path


def read_all(path, **options):
    with open_csv(path, **options) as table:
        return table.header, [(row.line_number, table.to_record(row)) for row in table.rows()]


def test_rows_carry_line_numbers_and_blank_cells_become_none(tmp_path):
    # Line 1 header, 2 first row, 3 blank (skipped), 4-5 a quoted cell spanning two lines, 6 last row.
    path = write(tmp_path / "a.csv", 'id,name,note\n1, Ada ,\n\n2,Grace,"two\nlines"\n3,Linus,x\n')
    header, rows = read_all(path)
    assert header == ["id", "name", "note"]
    assert rows == [
        (2, {"id": "1", "name": "Ada", "note": None}),
        (5, {"id": "2", "name": "Grace", "note": "two\nlines"}),
        (6, {"id": "3", "name": "Linus", "note": "x"}),
    ]


def test_utf8_bom_from_excel_is_not_part_of_the_first_column_name(tmp_path):
    path = write(tmp_path / "bom.csv", "﻿id,name\n1,a\n")
    header, rows = read_all(path)
    assert header == ["id", "name"]
    assert rows == [(2, {"id": "1", "name": "a"})]


def test_header_only_file_yields_no_rows(tmp_path):
    header, rows = read_all(write(tmp_path / "h.csv", "id,name\n"))
    assert header == ["id", "name"]
    assert rows == []


def test_leading_blank_lines_before_the_header_are_skipped(tmp_path):
    header, rows = read_all(write(tmp_path / "lead.csv", "\n\nid,name\n1,a\n"))
    assert header == ["id", "name"]
    assert rows == [(4, {"id": "1", "name": "a"})]


def test_custom_delimiter_and_quoting(tmp_path):
    path = write(tmp_path / "semi.csv", 'a;b\n1;"x;y"\n')
    _, rows = read_all(path, delimiter=";")
    assert rows == [(2, {"a": "1", "b": "x;y"})]


@pytest.mark.parametrize(
    "text, message",
    [
        ("", "file is empty"),
        ("\n\n", "file is empty"),
        ("id,,name\n1,2,3\n", "empty column name"),
        ("id,name,id\n1,2,3\n", "duplicate column name"),
    ],
)
def test_unusable_headers_abort_with_a_clear_message(tmp_path, text, message):
    path = write(tmp_path / "bad.csv", text)
    with pytest.raises(SourceError, match=message):
        with open_csv(path):
            pass


def test_missing_file_is_a_source_error(tmp_path):
    with pytest.raises(SourceError, match="cannot open"):
        with open_csv(tmp_path / "nope.csv"):
            pass


def test_undecodable_bytes_abort_instead_of_guessing(tmp_path):
    path = tmp_path / "latin.csv"
    path.write_bytes(b"id,name\n1,Za\xbf\xf3\xb3\xe6\n")  # cp1250 bytes read as UTF-8
    with pytest.raises(SourceError):
        read_all(path)


@pytest.mark.parametrize(
    "text, found",
    [("a,b\n1\n", 1), ("a,b\n1,2,3\n", 3)],
)
def test_wrong_field_count_rejects_only_that_row(tmp_path, text, found):
    path = write(tmp_path / "w.csv", text)
    with open_csv(path) as table:
        (row,) = list(table.rows())
        with pytest.raises(RowError) as excinfo:
            table.to_record(row)
    assert excinfo.value.reason == f"expected 2 fields, found {found}"
    assert excinfo.value.stage == "extract"
