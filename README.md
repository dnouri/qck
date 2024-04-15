# Qck 🦆👩‍💻

Qck (pronounced "quack") is a CLI script to conveniently run
[DuckDB](https://duckdb.org/) SQL scripts with support for
[Jina](https://jinja.palletsprojects.com/) templating.

## 🛠️ Installation

Use `pip install qck` to install.  This will make available the `qck`
script.

## 🚀 Usage

Run `qck --help` to view the built-in documentation.

Running `qck` with just a SQL file will execute the query and print
the results to the terminal:

```bash
qck myquery.sql
```

The default is to `LIMIT` the output to 100 lines.  You can override
this with the `--limit` option:

```bash
qck myquery.sql --limit 10  # will only print 10 rows
```

To execute a query and write the result to a Parquet file, use
`--to-parquet`:

```bash
qck myquery.sql --to-parquet myresult.parquet
```

You can also call `qck` from within Python:

```python
from qck import qck
rs = qck("myquery.sql")
rs.to_parquet("myresult.parquet")
```

For a full list of arguments to `qck`, please refer to the
[source](qck.py).

## 🖋️ Templating

Qck can interpret SQL files as Jinja templates, enabling the use of
control structures like for loops within SQL files. Additionally, Qck
introduces a special variable, `import`, in templates, enabling access
to arbitrary Python functions. For instance, consider the following
example, where we import the `glob` function and utilize it to list
files to query from:

```jinja
{% for fname in import('glob.glob')('data/*xlsx') %}
SELECT
    "Value" AS value,
    "Région" AS region,
FROM
    st_read('{{ fname }}')
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
```
