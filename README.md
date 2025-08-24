# Qck 🦆👩‍💻

Qck (pronounced "quack") is a CLI script to conveniently run
[DuckDB](https://duckdb.org/) SQL scripts with support for
[Jinja](https://jinja.palletsprojects.com/) templating.

## 🛠️ Installation

Use `pip install qck` to install. This will make available the `qck`
command-line tool.

## 🚀 Usage

### Command Line Interface

The basic usage is to run a SQL file and display results in the terminal:

```bash
qck myquery.sql
```

Key command line options:

- `--limit N`: Limit output to N rows (default: 99 rows)
- `--to-parquet FILE`: Save results to a Parquet file
- `--to-csv FILE`: Save results to a CSV file
- `--interactive`: Open Python shell with query results
- `--verbose, -v`: Print the generated SQL query
- `args`: Pass template parameters as key=value pairs

Examples:

```bash
# Limit output to 10 rows
qck myquery.sql --limit 10

# Save to Parquet with verbose output
qck myquery.sql --to-parquet results.parquet -v

# Pass template parameters
qck template.sql date=2024-01-01 region=EU

# Interactive mode
qck myquery.sql --interactive

# Pipe SQL from stdin (use - as filename)
echo "SELECT 42 as answer" | qck -

# With template parameters from stdin
echo "SELECT '{{ name }}' as greeting" | qck - name=World
```

### Python API

You can use Qck programmatically in Python:

```python
from qck import qck

# Basic query execution
rs = qck("myquery.sql")

# With template parameters
rs = qck("template.sql", params={"date": "2024-01-01"})

# Control output limit
rs = qck("myquery.sql", limit=1000)

# Print generated SQL
rs = qck("myquery.sql", print_query=True)

# Access results as pandas DataFrame
df = rs.df()
```

## 🖋️ Templating

Qck uses Jinja2 for SQL templating with some special features:

1. Parameter substitution:
```sql
SELECT *
FROM orders
WHERE date = '{{ date }}'
```

2. Python function imports:
```sql
-- Use the special 'import' variable to access Python functions
{% set files = import('glob.glob')('data/*.parquet') %}
{% for file in files %}
SELECT * FROM read_parquet('{{ file }}')
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
```

3. Control structures:
```sql
SELECT
    customer_id,
    {% if include_details %}
    first_name,
    last_name,
    email,
    {% endif %}
    total_orders
FROM customers
```

4. Custom Python functions:
```sql
-- Import and use your own functions
{% set helper = import('mymodule.helpers:format_date') %}
SELECT *
FROM events
WHERE date = '{{ helper(date) }}'
```

The `import` variable allows importing any Python module or function using:
- Module syntax: `import('module_name')`
- Function syntax: `import('module:function')`
- Nested syntax: `import('module.submodule:function')`

## 🧪 Testing

To run the test suite:

```bash
# Install development dependencies
pip install -e .
pip install pytest

# Run tests
pytest test_qck.py -v
```
