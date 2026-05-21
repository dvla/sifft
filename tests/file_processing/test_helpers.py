import tempfile
from pathlib import Path

import pytest
from file_processing.helpers import _read_sample_lines, detect_delimiter, detect_header


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


class TestReadSampleLines:
    def test_reads_n_lines(self, temp_dir):
        f = temp_dir / "data.txt"
        f.write_text("\n".join(f"line{i}" for i in range(50)))

        lines = _read_sample_lines(str(f), n=5)

        assert len(lines) == 5
        assert lines[0] == "line0"
        assert lines[4] == "line4"

    def test_file_shorter_than_n(self, temp_dir):
        f = temp_dir / "short.txt"
        f.write_text("a\nb\n")

        lines = _read_sample_lines(str(f), n=20)

        assert len(lines) == 2

    def test_strips_line_endings(self, temp_dir):
        f = temp_dir / "crlf.txt"
        f.write_bytes(b"a,b,c\r\n1,2,3\r\n")

        lines = _read_sample_lines(str(f), n=5)

        assert lines[0] == "a,b,c"
        assert lines[1] == "1,2,3"


class TestDetectDelimiter:
    def test_detects_comma(self, temp_dir):
        f = temp_dir / "data"
        f.write_text("name,age,city\nAlice,25,London\nBob,30,Paris\n")

        assert detect_delimiter(str(f)) == ","

    def test_detects_pipe(self, temp_dir):
        f = temp_dir / "data"
        f.write_text("name|age|city\nAlice|25|London\nBob|30|Paris\n")

        assert detect_delimiter(str(f)) == "|"

    def test_detects_tab(self, temp_dir):
        f = temp_dir / "data"
        f.write_text("name\tage\tcity\nAlice\t25\tLondon\nBob\t30\tParis\n")

        assert detect_delimiter(str(f)) == "\t"

    def test_detects_semicolon(self, temp_dir):
        f = temp_dir / "data"
        f.write_text("name;age;city\nAlice;25;London\nBob;30;Paris\n")

        assert detect_delimiter(str(f)) == ";"

    def test_single_column_falls_back_to_comma(self, temp_dir):
        f = temp_dir / "data"
        f.write_text("hello\nworld\nfoo\n")

        assert detect_delimiter(str(f)) == ","

    def test_single_line_falls_back_to_comma(self, temp_dir):
        f = temp_dir / "data"
        f.write_text("only one line\n")

        assert detect_delimiter(str(f)) == ","

    def test_empty_lines_ignored(self, temp_dir):
        f = temp_dir / "data"
        f.write_text("\n\nname|age\nAlice|25\nBob|30\n\n")

        assert detect_delimiter(str(f)) == "|"

    def test_comma_wins_tie_over_pipe(self, temp_dir):
        # Both produce 2 columns consistently — comma wins by priority
        f = temp_dir / "data"
        f.write_text("a,b|c\nd,e|f\ng,h|i\n")

        result = detect_delimiter(str(f))
        # Both comma and pipe produce 2 columns, comma has higher priority
        assert result == ","

    def test_many_rows_pipe_delimited(self, temp_dir):
        f = temp_dir / "data"
        rows = ["col1|col2|col3|col4"] + [f"a{i}|b{i}|c{i}|d{i}" for i in range(25)]
        f.write_text("\n".join(rows))

        assert detect_delimiter(str(f)) == "|"

    def test_quoted_fields_with_commas_inside(self, temp_dir):
        """Quoted fields containing commas shouldn't break pipe detection."""
        f = temp_dir / "data"
        f.write_text(
            'id|name|description\n'
            '1|Widget|"A small, useful thing"\n'
            '2|Gadget|"Big, heavy item"\n'
            '3|Gizmo|"Cheap, fast, reliable"\n'
        )

        assert detect_delimiter(str(f)) == "|"

    def test_all_fields_quoted_csv(self, temp_dir):
        """Fully quoted CSV should still detect comma."""
        f = temp_dir / "data"
        f.write_text(
            '"name","age","city"\n'
            '"Alice","25","London"\n'
            '"Bob","30","Paris"\n'
        )

        assert detect_delimiter(str(f)) == ","

    def test_csv_with_commas_in_quoted_fields(self, temp_dir):
        """CSV where quoted fields contain commas — naive split produces inconsistent
        counts but comma still wins because majority of lines are consistent."""
        f = temp_dir / "data"
        # 1 header + 4 data rows; only 2 rows have embedded commas
        f.write_text(
            'name,description,price\n'
            'Widget,"A small, useful thing",29.99\n'
            'Gadget,Simple gadget,15.00\n'
            'Gizmo,Basic gizmo,9.99\n'
            'Thingamajig,"Heavy, large, expensive",89.99\n'
        )

        # Comma still wins: 3 of 5 lines split to 3 cols, 2 split to more
        # Score = 3/5 = 0.6 which beats other delimiters
        assert detect_delimiter(str(f)) == ","

    def test_majority_rows_have_embedded_commas_in_quotes(self, temp_dir):
        """Edge case: most rows have commas inside quoted fields.
        Naive split produces inconsistent counts — comma still selected
        because no other delimiter scores higher, but column count is wrong.
        This is a known limitation of naive splitting (documented in issue #11).
        """
        f = temp_dir / "data"
        f.write_text(
            'name,description,price\n'
            'Widget,"Small, light, cheap",29.99\n'
            'Gadget,"Big, heavy, expensive",15.00\n'
            'Gizmo,"Fast, reliable, sturdy",9.99\n'
            'Thing,"Odd, unusual, rare",89.99\n'
        )

        # Comma is still detected (no other delimiter is better) but the
        # most_common column count will be 5 not 3. The file will still
        # parse — Spark's PERMISSIVE mode handles the quoted commas correctly
        # because Spark's CSV reader IS quote-aware. Our detection just needs
        # to pick the right delimiter character.
        assert detect_delimiter(str(f)) == ","


class TestDetectHeaderRefactored:
    """Verify detect_header still works after refactoring to use _read_sample_lines."""

    def test_header_detected_when_first_row_text_second_numeric(self, temp_dir):
        f = temp_dir / "data.csv"
        f.write_text("name,age,score\nAlice,25,99.5\n")

        assert detect_header(str(f), ",") is True

    def test_no_header_when_all_numeric(self, temp_dir):
        f = temp_dir / "data.csv"
        f.write_text("1,2,3\n4,5,6\n")

        assert detect_header(str(f), ",") is False

    def test_header_with_pipe_delimiter(self, temp_dir):
        f = temp_dir / "data.dat"
        f.write_text("name|age|city\nAlice|25|London\n")

        assert detect_header(str(f), "|") is True

    def test_single_line_defaults_to_true(self, temp_dir):
        f = temp_dir / "data.csv"
        f.write_text("name,age,city\n")

        assert detect_header(str(f), ",") is True

    def test_storage_options_accepted(self, temp_dir):
        """Verify the new storage_options parameter doesn't break anything."""
        f = temp_dir / "data.csv"
        f.write_text("name,age\nAlice,25\n")

        # None is the default — should work fine
        assert detect_header(str(f), ",", storage_options=None) is True
