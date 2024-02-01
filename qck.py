import code
from time import time

import click
import duckdb
import jinja2


@click.command()
@click.argument("sql-file")
@click.argument("args", nargs=-1)
@click.option(
    "--interactive",
    is_flag=True,
    help="Enter Python prompt after running the query.",
)
@click.option(
    "--to-parquet",
    help="Save output to a given Parquet file.",
)
@click.option(
    "--to-csv",
    help="Save output to a given CSV file.",
)
@click.option(
    "--limit",
    help="Limit the output to n rows.",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
)
def main(sql_file, args, interactive, to_parquet, to_csv, limit, verbose):
    """Run DuckDB SQL scripts

    By default, will write output to terminal.  Use --limit to
    restrict number of output rows.
    """
    t0 = time()

    params = {}
    for arg in args:
        key, value = arg.split('=')
        params[key] = value

    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader('.'),
        undefined=jinja2.StrictUndefined,
        autoescape=False,
        trim_blocks=False,
        lstrip_blocks=True,
        line_comment_prefix="--",
    )
    template = env.get_template(sql_file)
    query = template.render(**params)
    if limit:
        query += f"\nLIMIT {limit}"

    if verbose:
        print("```sql")
        print(query.strip())
        print("```")
        print()

    rs = duckdb.sql(query)
    if interactive:
        local = globals().copy()
        local.update(locals())
        code.interact(
            "'rs' is the DuckDB result set",
            local=local,
        )
    elif to_parquet:
        duckdb.sql(f"COPY rs TO '{to_parquet}' (FORMAT 'PARQUET')")
        n_rows = duckdb.sql(f"SELECT COUNT(*) FROM '{to_parquet}'").fetchone()[0]
        print(f"Wrote {n_rows:,} rows to {to_parquet}")
    elif to_csv:
        duckdb.sql(f"COPY rs TO '{to_csv}' (FORMAT 'CSV')")
        n_rows = duckdb.sql(f"SELECT COUNT(*) FROM '{to_csv}'").fetchone()[0]
        print(f"Wrote {n_rows:,} rows to {to_csv}")
    else:
        if not limit:
            rs2 = duckdb.sql("SELECT * FROM rs LIMIT 99")
        else:
            rs2 = rs
        df = rs2.df()
        print(df.to_markdown())
        if not limit:
            if len(df) == 99:
                print("...")
        print()
    print(f"Done in {time() - t0:.3} sec")


if __name__ == '__main__':
    main()
