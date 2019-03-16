# -*- coding: utf-8 -*-
from context import file_writer


def test_get_table_tsv_default_path():
    fw = file_writer.FileWriter(db="test", engine_type="postgresql")
    ts = fw.timestamp
    assert \
        fw.get_table_tsv_default_path() == \
        "data/postgresql_test_table_column_metadata_{}.tsv".format(ts)


def test_get_view_ddl_sql_default_path():
    fw = file_writer.FileWriter(db="test", engine_type="redshift")
    ts = fw.timestamp
    assert \
        fw.get_view_ddl_sql_default_path() == \
        "data/redshift_test_view_ddl_{}.sql".format(ts)


def test_output_table_metadata_to_tsv(tmpdir):
    # Create temporary location
    test_dir = tmpdir.mkdir("table")

    fw = file_writer.FileWriter(db="test", engine_type="postgresql")
    table_metadata = {
        "public": {
            "transactions": [
               {"default": 0, "type": "BIGINT()",
                "name": "id", "nullable": False},
               {"default": None, "type": "VARCHAR(length = 500)",
                "name": "content", "nullable": True},
               {"default": None, "type": "VARCHAR(length = 30)",
                "name": "category", "nullable": True},
               {"default": None, "type": "TIMESTAMP(timezone = True)",
                "name": "transaction_tz", "nullable": True},
               {"default": None, "type": "BIGINT()",
                "name": "user_id", "nullable": True},
               {"default": None, "type": "VARCHAR(length = 50)",
                "name": "authorization_id", "nullable": True},
               {"default": None,
                "type": "TIMESTAMP(timezone = True)", "name": "insert_tz",
                "nullable": True}],
            "alerts": [
                {"default": 0, "type": "BIGINT()",
                 "name": "id", "nullable": False},
                {"default": None, "type": "VARCHAR(length = 500)",
                 "name": "content", "nullable": True},
                {"default": None, "type": "VARCHAR(length = 30)",
                 "name": "severity", "nullable": True},
                {"default": None, "type": "TIMESTAMP(timezone = True)",
                 "name": "date_sent_tz", "nullable": True},
                {"default": None, "type": "TIMESTAMP(timezone = True)",
                 "name": "date_read_tz", "nullable": True},
                {"default": None,
                 "type": "TIMESTAMP(timezone = True)", "name": "insert_tz",
                 "nullable": True}]}}
    output_file = test_dir.join("test_table_metadata.tsv")
    fw.output_table_metadata_to_tsv(
        table_metadata=table_metadata, path=str(output_file))

    with open("tests/fixtures/test_table_metadata.tsv") as f:
        reference_content = f.read().replace("\n", "")
    test_content = output_file.read_text(encoding="UTF-8")

    # Haven't figured out why replaces are needed but normalizes results
    assert \
        test_content.replace("\n", "") == \
        reference_content.replace("\n", "")


def test_output_view_ddl_to_sql(tmpdir):
    # Create temporary location
    test_dir = tmpdir.mkdir("view")

    fw = file_writer.FileWriter(db="test", engine_type="postgresql")
    view_ddl = {
        "public": {
            "transactions_max":
                "CREATE OR REPLACE VIEW transactions_max AS "
                "SELECT MAX(id) FROM public.transactions;"}}
    output_file = test_dir.join("test_view.sql")
    fw.output_view_ddl_to_sql(view_ddl=view_ddl, path=str(output_file))

    with open("tests/fixtures/test_view.sql") as f:
        reference_content = f.read().replace("\n", "")
    test_content = output_file.read_text(encoding="UTF-8")

    # Haven't figured out why replaces are needed but normalizes results
    assert \
        test_content.replace("\n", "") == \
        reference_content.replace("\n", "")
