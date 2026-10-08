CREATE OR REPLACE TABLE raw.{table} AS
SELECT
    * EXCLUDE (filename),
    parse_filename(filename) AS _source_file,
    CAST(? AS VARCHAR) AS _pipeline_run_id
FROM read_parquet(?, union_by_name = true, filename = true);
