import re
from unittest.mock import MagicMock

import pytest
from psycopg2.errors import DuplicateSchema

from utils.helpers import (
    archive_input,
    clip_csv_columns,
    copy_csv,
    ensure_schema,
    ensure_table_from_csv_header,
    read_csv_header,
    sanitize_identifier,
    timestamp_suffix,
)


class TestSanitizeIdentifier:
    def test_lowercases_and_strips(self):
        assert sanitize_identifier("  Host Name  ") == "host_name"

    def test_replaces_non_alphanumeric_with_underscore(self):
        assert sanitize_identifier("price ($AUD)") == "price_aud"

    def test_collapses_repeated_underscores(self):
        assert sanitize_identifier("a---b") == "a_b"

    def test_prefixes_leading_digit(self):
        assert sanitize_identifier("30_day_avail") == "c_30_day_avail"

    def test_empty_input_falls_back_to_col(self):
        assert sanitize_identifier("***") == "col"


def test_timestamp_suffix_matches_expected_format():
    assert re.fullmatch(r"\d{8}-\d{6}", timestamp_suffix())


def test_read_csv_header(tmp_path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("id,name,value\n1,a,10\n2,b,20\n")

    assert read_csv_header(str(csv_path)) == ["id", "name", "value"]


def test_clip_csv_columns_keeps_only_leading_columns(tmp_path):
    src = tmp_path / "dirty.csv"
    src.write_text("a,b,c,d\n1,2,3,4\n5,6,7,8\n")
    dst = tmp_path / "out" / "clean.csv"

    clip_csv_columns(str(src), str(dst), keep_cols=2)

    assert dst.read_text() == "a,b\n1,2\n5,6\n"


def test_archive_input_moves_file_with_timestamp_suffix(tmp_path):
    src = tmp_path / "05_2020.csv"
    src.write_text("id\n1\n")
    archive_dir = tmp_path / "archive"

    archive_input(str(src), str(archive_dir))

    archived = list(archive_dir.iterdir())
    assert not src.exists()
    assert len(archived) == 1
    assert re.fullmatch(r"05_2020_\d{8}-\d{6}\.csv", archived[0].name)


def test_ensure_schema_swallows_duplicate_schema_error():
    pg_hook = MagicMock()
    pg_hook.run.side_effect = DuplicateSchema()

    ensure_schema(pg_hook, "bronze")

    pg_hook.run.assert_called_once_with(
        "CREATE SCHEMA IF NOT EXISTS bronze;",
    )


def test_ensure_table_from_csv_header_reuses_table_with_matching_columns(
    tmp_path,
):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("id,name\n1,a\n")

    pg_hook = MagicMock()
    pg_hook.get_first.return_value = ("bronze.t",)
    pg_hook.get_records.return_value = [("id",), ("name",)]

    result = ensure_table_from_csv_header(
        pg_hook, "bronze", "t", str(csv_path)
    )

    assert result == ["id", "name"]
    pg_hook.run.assert_not_called()


def test_ensure_table_from_csv_header_recreates_on_column_mismatch(
    tmp_path,
):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("id,name,extra\n1,a,x\n")

    pg_hook = MagicMock()
    pg_hook.get_first.return_value = ("bronze.t",)
    pg_hook.get_records.return_value = [("id",), ("name",)]

    result = ensure_table_from_csv_header(
        pg_hook, "bronze", "t", str(csv_path)
    )

    assert result == ["id", "name", "extra"]
    assert pg_hook.run.call_count == 2
    assert "DROP TABLE" in pg_hook.run.call_args_list[0].args[0]
    assert "CREATE TABLE" in pg_hook.run.call_args_list[1].args[0]


def test_copy_csv_raises_when_file_missing(tmp_path):
    pg_hook = MagicMock()

    with pytest.raises(FileNotFoundError):
        copy_csv(pg_hook, "bronze", "t", str(tmp_path / "missing.csv"))
