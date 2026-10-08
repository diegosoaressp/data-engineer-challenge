CREATE OR REPLACE TABLE raw.settlements AS
SELECT
    * EXCLUDE (filename),
    CAST(? AS VARCHAR) AS processor,
    -- Apenas rastreabilidade (ordem aproximada do arquivo); não é chave, nunca use em join ou dedupe.
    row_number() OVER () AS _row_number,
    parse_filename(filename) AS _source_file,
    CAST(? AS VARCHAR) AS _pipeline_run_id
FROM read_csv(?, header = true, all_varchar = true, filename = true);
