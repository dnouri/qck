import pytest
import duckdb
from click.testing import CliRunner
from qck import qck, main


class TestQckFunction:
    def test_qck_with_sql_content(self):
        sql_content = "SELECT 42 as answer, 'hello' as greeting"
        result = qck(sql_content=sql_content)
        df = result.df()
        assert len(df) == 1
        assert df.iloc[0]["answer"] == 42
        assert df.iloc[0]["greeting"] == "hello"

    def test_qck_with_sql_content_and_params(self):
        sql_content = "SELECT {{ num }} as answer, '{{ text }}' as greeting"
        result = qck(sql_content=sql_content, params={"num": 100, "text": "world"})
        df = result.df()
        assert len(df) == 1
        assert df.iloc[0]["answer"] == 100
        assert df.iloc[0]["greeting"] == "world"

    def test_qck_with_sql_content_and_limit(self):
        sql_content = "SELECT * FROM generate_series(1, 100)"
        result = qck(sql_content=sql_content, limit=5)
        df = result.df()
        assert len(df) == 5

    def test_qck_backward_compatibility_with_file(self, tmp_path):
        temp_file = tmp_path / "test.sql"
        temp_file.write_text("SELECT 123 as num")

        result = qck(str(temp_file))
        df = result.df()
        assert len(df) == 1
        assert df.iloc[0]["num"] == 123


class TestCLI:
    def test_cli_with_stdin(self):
        runner = CliRunner()
        sql_input = "SELECT 42 as answer"

        result = runner.invoke(main, ["-"], input=sql_input)
        assert result.exit_code == 0
        assert "42" in result.output
        assert "answer" in result.output

    def test_cli_with_stdin_and_params(self):
        runner = CliRunner()
        sql_input = "SELECT {{ num }} as answer"

        result = runner.invoke(main, ["-", "num=100"], input=sql_input)
        assert result.exit_code == 0
        assert "100" in result.output

    def test_cli_with_stdin_to_parquet(self, tmp_path):
        runner = CliRunner()
        sql_input = "SELECT 42 as answer"
        output_file = tmp_path / "output.parquet"

        result = runner.invoke(
            main, ["-", "--to-parquet", str(output_file)], input=sql_input
        )
        assert result.exit_code == 0
        assert "Wrote 1 rows" in result.output

        df = duckdb.sql(f"SELECT * FROM '{output_file}'").df()
        assert len(df) == 1
        assert df.iloc[0]["answer"] == 42

    def test_cli_with_stdin_to_csv(self, tmp_path):
        runner = CliRunner()
        sql_input = "SELECT 42 as answer"
        output_file = tmp_path / "output.csv"

        result = runner.invoke(
            main, ["-", "--to-csv", str(output_file)], input=sql_input
        )
        assert result.exit_code == 0
        assert "Wrote 1 rows" in result.output

        df = duckdb.sql(f"SELECT * FROM '{output_file}'").df()
        assert len(df) == 1
        assert df.iloc[0]["answer"] == 42

    def test_cli_backward_compatibility_with_file(self, tmp_path):
        runner = CliRunner()
        temp_file = tmp_path / "test.sql"
        temp_file.write_text("SELECT 999 as num")

        result = runner.invoke(main, [str(temp_file)])
        assert result.exit_code == 0
        assert "999" in result.output

    def test_cli_stdin_with_verbose(self):
        runner = CliRunner()
        sql_input = "SELECT 42 as answer"

        result = runner.invoke(main, ["-", "-v"], input=sql_input)
        assert result.exit_code == 0
        assert "```sql" in result.output
        assert "SELECT 42 as answer" in result.output
        assert "42" in result.output

    def test_cli_sql_parser_error(self):
        """Test that parser errors show clean error messages with query"""
        runner = CliRunner()
        sql_input = "SELECTX * FROM table1"

        result = runner.invoke(main, ["-"], input=sql_input)
        assert result.exit_code == 1
        assert "Failed SQL query:" in result.output
        assert "```sql" in result.output
        assert "SELECTX * FROM table1" in result.output
        assert "SQL Error:" in result.output
        assert "Parser Error:" in result.output
        assert "syntax error at or near \"SELECTX\"" in result.output
        # Should not contain Python traceback elements
        assert "Traceback" not in result.output
        assert "File \"" not in result.output

    def test_cli_sql_catalog_error(self):
        """Test that catalog errors show clean error messages with hints"""
        runner = CliRunner()
        sql_input = "SELECT * FROM non_existent_table"

        result = runner.invoke(main, ["-"], input=sql_input)
        assert result.exit_code == 1
        assert "Failed SQL query:" in result.output
        assert "```sql" in result.output
        assert "SELECT * FROM non_existent_table" in result.output
        assert "SQL Error:" in result.output
        assert "Catalog Error:" in result.output
        assert "Table with name non_existent_table does not exist" in result.output
        # Should preserve DuckDB's helpful hints
        assert "Did you mean" in result.output
        # Should not contain Python traceback
        assert "Traceback" not in result.output
        assert "File \"" not in result.output

    def test_cli_sql_binder_error(self):
        """Test that binder errors show clean error messages"""
        runner = CliRunner()
        sql_input = "SELECT unknown_column FROM (SELECT 1 as a)"

        result = runner.invoke(main, ["-"], input=sql_input)
        assert result.exit_code == 1
        assert "Failed SQL query:" in result.output
        assert "```sql" in result.output
        assert "SELECT unknown_column FROM (SELECT 1 as a)" in result.output
        assert "SQL Error:" in result.output
        assert "Binder Error:" in result.output
        assert "unknown_column" in result.output
        # Should not contain Python traceback
        assert "Traceback" not in result.output

    def test_cli_file_not_found_error(self):
        """Test that file not found errors show clean error messages"""
        runner = CliRunner()

        result = runner.invoke(main, ["non_existent_file.sql"])
        assert result.exit_code == 1
        assert "File Error:" in result.output
        assert "non_existent_file.sql" in result.output
        assert "not found" in result.output
        # Should not contain Python traceback
        assert "Traceback" not in result.output
        assert "TemplateNotFound" not in result.output

    def test_cli_template_error(self):
        """Test that template errors show clean error messages"""
        runner = CliRunner()
        sql_input = "SELECT {{ undefined_var }}"

        result = runner.invoke(main, ["-"], input=sql_input)
        assert result.exit_code == 1
        assert "Template Error:" in result.output
        assert "undefined_var" in result.output
        # Should not contain Python traceback
        assert "Traceback" not in result.output


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
