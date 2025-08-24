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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
