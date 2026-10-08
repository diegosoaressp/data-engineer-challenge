# Arquitetura do pipeline — Settlement Reconciliation

## Contexto

O repo traz só os dados de exemplo e o gerador. Vamos construir um pipeline em DuckDB que lê os extracts CDC + CSV do PaySettler e gera tabelas analíticas para Operações, CFO e Compliance. Ele precisa ser idempotente por `reference_date`, observável, ter checagens de qualidade e ter testes. **A reconciliação é recalculada pelo pipeline** a partir das regras do glossário. O `reconciliation_results` do serviço entra como fonte de comparação.

### O que os dados de exemplo mostram (`docs/sample-data/`)

- **transactions**: 1060 linhas e 1000 `transaction_id`. Tem `Op` = I/U/**D** (6 deletes). Os pares I→U mudam o `status`, por exemplo FAILED→COMPLETED. Só o batch_2 tem `payment_method`.
- **reconciliation_runs**: 10 runs, um a cada 3 dias. O run 7 tem só a linha I, com status FAILED, e não tem resultados. O `file_name` segue o padrão `settlement_YYYYMMDD_02_00.csv`.
- **reconciliation_results**: 837 linhas. Tem 5 linhas com `run_id=99` (FK órfã), 132 linhas MATCHED com |dif| > 0,01 e 5 linhas com `difference` ≠ |interno − processador|.
- **CSV** (2025-03-16): 510 linhas, sendo **10 duplicadas idênticas**. O `settled_at` aparece em 3 formatos (`Z`, `+00:00`, sem fuso). **22 valores estão no formato BR `R$ 32.245,91`** (prefixo, ponto de milhar, vírgula decimal), não só com vírgula decimal.
- **REVERSED**: as 27 linhas têm **`amount` positivo** (nenhuma negativa ou zero). O gerador também só produz valores positivos. Na amostra, nenhum `transaction_id` aparece como SETTLED e REVERSED ao mesmo tempo.
- **Liquidação de transação interna FAILED**: 20 linhas do CSV. Também há 2 linhas que apontam para transações deletadas (Op D).
- **Atenção:** na amostra, os valores do CSV **nunca** batem com o interno (0 de 510), e só 127 transações estão na janela de 7 dias. A amostra serve para testar a limpeza dos dados sujos. A correção da reconciliação é validada com o **gerador**, cujos dados são coerentes.

## 1. Estrutura de pastas

```
src/recon/
  __init__.py
  __main__.py         # python -m recon
  cli.py              # recon run --reference-date YYYY-MM-DD [--source-dir DIR] [--settlement-file PATH]
                      # recon backfill --start D1 --end D2 --source-dir DIR
  config.py           # DATA_PATH, tolerância (0.01), janela (7d), limite de MISMATCHED (5%), processadores
  db.py               # conexão DuckDB, context manager de transação, aplicação de DDL
  logging.py          # logger JSON estruturado (sempre com run_id e reference_date)
  sql/
    ddl/              # CREATE SCHEMA / CREATE TABLE IF NOT EXISTS (marts, ops)
    raw/*.sql
    staging/*.sql
    marts/*.sql
  ingest/
    cdc.py            # parquet CDC -> raw.* (union_by_name)
    settlement.py     # localiza o CSV de D (ou usa --settlement-file) -> raw.* em VARCHAR
  transform/
    staging.py
    marts.py          # escrita por reference_date
  quality/
    checks.py         # Check(name, layer, severity: block|alert, sql, threshold)
    registry.py       # lista declarativa de checks
    runner.py         # grava ops.dq_results e levanta BlockingCheckFailed
  pipeline.py         # orquestra raw → staging → checks block → marts → checks alert
queries/              # ops.sql, cfo.sql, compliance.sql
tests/
  conftest.py         # fixtures pequenas montadas em tmp + DuckDB descartável
  test_*.py
docs/architecture.md  # este plano
```

O `docker-compose.yml` ganha os volumes `./src:/app/src` e `./queries:/app/queries`. O banco fica em `$DATA_PATH/warehouse.duckdb`.

## 2. Camadas e tabelas

### raw: CDC e CSV como chegaram, **recriados a cada execução** (`CREATE OR REPLACE`)
Todas as colunas são VARCHAR/originais, mais `_source_file` e `_pipeline_run_id`.
- `raw.transactions`: batch_1 + batch_2 lidos com `union_by_name`. `payment_method` fica NULL no batch_1.
- `raw.reconciliation_runs`, `raw.reconciliation_results`, `raw.enterprise_company`
- `raw.settlements`: o CSV de D com todas as colunas em VARCHAR, mais `processor` e `_row_number`.

**Arquivo de D**: se `--settlement-file` for passado, ele é usado e pronto. Caso contrário, o pipeline procura em `<source-dir>/paysettler/` por `settlement_YYYY-MM-DD.csv` (padrão do gerador) ou `settlement_YYYYMMDD_*.csv` (padrão do `file_name` da amostra). Para a amostra: `--settlement-file docs/sample-data/settlement_paysettler.csv --reference-date 2025-03-16`.

### staging: estado atual, tipado e limpo (`CREATE OR REPLACE`)
- `stg_transactions`: último registro por **`transaction_id`**, ordenado por `_timestamp` e depois `id`. `is_deleted` vale true quando o último Op é D; essas linhas ficam de fora do matching. Dinheiro em DECIMAL(18,2), timestamps em TIMESTAMPTZ UTC e `merchant_id` com `upper(trim())`.
- `stg_reconciliation_runs`: último registro por `id`.
- `stg_reconciliation_results`: último registro por `id`, com a flag `has_orphan_run`.
- `stg_merchants_history`: SCD2 (`valid_from`/`valid_to`/`is_current`).
- `stg_settlements`: uma linha por `(processor, transaction_id)`.
  - **Valor**: tira o prefixo `R$` e os espaços. Se houver vírgula, remove os pontos de milhar e troca a vírgula por ponto. Depois converte para DECIMAL(18,2).
  - **Datas e merchant**: `settled_at` sem fuso é tratado como UTC, e `merchant_id` fica em maiúsculas.
  - **Duplicados**: linhas idênticas são deduplicadas e a contagem fica em `dup_count`. Se o mesmo `transaction_id` vier com conteúdo diferente, fica o **último por `settled_at`**, com a flag **`has_reversal`** (true se alguma versão for REVERSED) e `version_count`.
  - **Valor com sinal**: `signed_amount` = `amount` para SETTLED e `-amount` para REVERSED.

### marts
| Tabela | Grão | Consumidor |
|---|---|---|
| `dim_merchant` | merchant, SCD2 | todos |
| `fct_reconciliation` | (reference_date, processor, transaction_id) | Ops / Compliance. Recalculada: full outer join entre `stg_settlements` de D e `stg_transactions` (não deletadas) com `created_at::date ∈ [D-7, D]`; |dif| ≤ 0,01 → MATCHED. Guarda os valores, a diferença, a categoria, `settlement_status`, `has_reversal`, `internal_status`, `is_late_settlement`, `settled_date`, `created_date` e `source_category` (vinda do serviço). |
| `fct_reconciliation_daily` | (reference_date, processor) | Ops: contagem e valor por categoria, taxa de MISMATCHED, taxa de REVERSED, divergência com o serviço e contagem de alertas do dia. |
| `fct_reconciliation_run` | run do serviço | Ops / Compliance: status, duração, total declarado × linhas no arquivo × resultados. |
| `fct_financial_daily` | (date, date_basis, currency, merchant_id) | CFO: volume interno por status, volume liquidado, volume revertido e líquido. `date_basis` ∈ {`settled`, `created`}, com **`settled` como padrão** nas queries. Sem câmbio: os valores ficam separados por moeda. |
| `audit_reconciliation_history` | (pipeline_run_id, reference_date, transaction_id) | Compliance: snapshot de cada execução, só acrescentado. |

### ops: observabilidade
- `ops.pipeline_runs`: run_id, reference_date, status (RUNNING/SUCCESS/FAILED), started/finished, duração e contagem de linhas por etapa, settlement_file, git sha e erro.
- `ops.dq_results`: run_id, reference_date, check, severidade, aprovado/reprovado, valor observado, limite e amostra de chaves.

## 3. Idempotência por `reference_date`

1. **CLI**: `recon run --reference-date D`. Rodar de novo para a mesma data substitui o resultado anterior. Não há lock: o DuckDB permite um único processo escrevendo no arquivo.
2. **raw e staging**: recriados por inteiro a cada execução (`CREATE OR REPLACE`). Sem estado entre execuções, não há duplicação possível.
3. **marts**: numa **única transação**, `DELETE WHERE reference_date = D` seguido de `INSERT ... SELECT` em todos os fatos com escopo por data. `dim_merchant` é recriada a partir do staging. `audit_reconciliation_history` é a única tabela só acrescentada, com chave por `pipeline_run_id`.
4. **Falha no meio**: os checks `block` rodam **antes** da transação dos marts. Se um falhar, nada é escrito e o run fica FAILED com o erro em `ops.pipeline_runs`. Uma exceção dentro da transação dispara ROLLBACK e os marts continuam como antes.
   - **Padrão stage → check → publish**: os fatos são montados primeiro em tabelas temporárias. Os checks `block` (incluindo conservação e unicidade da chave) rodam sobre essas tabelas e gravam `ops.dq_results`. Só então a transação faz `DELETE` + `INSERT` a partir delas. Assim `ops.dq_results` nunca é gravado dentro de uma transação que pode sofrer rollback, e se um block falhar nenhum mart é tocado.
5. **backfill**: chama `run` para cada data, em sequência. Cada data é atômica e independente.

## 4. Checagens de qualidade

**BLOCK** (bloqueiam antes de escrever nos marts):
- Arquivo de liquidação de D ausente ou vazio.
- Colunas obrigatórias ausentes no CSV ou no parquet.
- `transaction_id` nulo ou fora do formato UUID (CSV ou transações).
- `amount` impossível de converter depois da normalização, ou **`amount` ≤ 0 em qualquer status**. Isso vale também para REVERSED, porque nos dados ele sempre vem positivo e o sinal vem do status.
- Moeda fora de {BRL, USD, EUR}, ou status fora dos domínios (SETTLED/REVERSED; COMPLETED/PENDING/FAILED).
- `settled_at` impossível de interpretar.
- Chave `(reference_date, processor, transaction_id)` duplicada no fato.
- Conservação: settlements deduplicados = MATCHED + MISMATCHED + UNRECONCILED_PROCESSOR, e transações internas na janela = MATCHED + MISMATCHED + UNRECONCILED_INTERNAL.

**ALERT** (o pipeline publica e o alerta fica em `ops.dq_results`):
- Linhas duplicadas idênticas no CSV.
- Mesmo `transaction_id` com versões diferentes no CSV (`version_count` > 1), incluindo `has_reversal`.
- Normalizações aplicadas: valor em formato BR, `settled_at` sem fuso, `merchant_id` em minúsculas.
- **Liquidação de transação interna com status FAILED.**
- `merchant_id` nulo nas transações, ou merchant inexistente em `dim_merchant`.
- Replay de CDC: mais de uma linha I para o mesmo `transaction_id`.
- Empate de `_timestamp` no CDC: versões distintas do mesmo `transaction_id` com o mesmo `_timestamp` (desempatadas por D > U > I).
- REVERSED órfão (sem par interno).
- Liquidação tardia (`settled_at − created_at` > 7 dias).
- `reconciliation_results` com `run_id` órfão.
- Categoria recalculada ≠ `source_category`, ou `difference` gravado ≠ |interno − processador|.
- `total_transactions` do run ≠ linhas do arquivo, ou run do serviço FAILED/sem resultados para D.
- Taxa de MISMATCHED > **5%** (`config.MISMATCH_RATE_ALERT`, configurável).

## 5. Ordem de implementação (cada etapa com seu teste)

0. **Salvar este plano em `docs/architecture.md`.**
1. **Esqueleto**: `src/recon`, config, logging JSON, `db.py`, CLI e volumes no compose. *Teste*: `--help` funciona e o log tem `run_id`/`reference_date`.
2. **DDL + `ops.pipeline_runs`**. *Teste*: aplicar o DDL duas vezes não dá erro, e o run vai de RUNNING para SUCCESS/FAILED.
3. **Raw CDC** com `union_by_name`. *Teste*: batch_1 fica com `payment_method` NULL, e rodar duas vezes mantém a mesma contagem.
4. **Raw do CSV**: localização do arquivo de D e `--settlement-file`. *Teste*: os dois padrões de nome são encontrados, `--settlement-file` tem prioridade e, sem arquivo, o run é bloqueado.
5. **Staging CDC**: último registro por chave, D e SCD2. *Teste*: fixtures com I→U, D, replay e merchant com 3 updates.
6. **Staging do CSV**. *Teste* parametrizado: `R$ 32.245,91` → 32245.91, `1234,56` → 1234.56, três formatos de `settled_at`, dedupe de idênticas, último por `settled_at` + `has_reversal`.
7. **Framework de DQ**. *Teste*: um block interrompe sem escrever nos marts e um alert registra e segue.
8. **`fct_reconciliation`**. *Teste*: dif 0,01 → MATCHED, 0,02 → MISMATCHED, bordas D−7 e D, REVERSED órfão, interna deletada ignorada.
9. **Escrita idempotente dos marts**. *Teste*: duas execuções dão o mesmo resultado, e uma exceção injetada faz rollback sem alterar nada.
10. **Agregados** (`fct_reconciliation_daily`, `fct_reconciliation_run`, `fct_financial_daily`, `dim_merchant`). *Teste*: os totais batem com o fato, e as visões `settled` e `created` somam o mesmo total.
11. **Auditoria** só acrescentada. *Teste*: duas execuções geram dois snapshots e um único fato atual.
12. **Checks block/alert restantes**, com um novo tipo de check **`metric`**: o SQL retorna um valor observado (escalar) em vez de linhas, e o check passa se o valor respeita o limite. Os alertas de taxa (ex.: MISMATCHED > 5%) gravam a taxa real em `ops.dq_results.observed_value`. Os checks de linhas (modelo da etapa 7) continuam valendo para os demais. Inclui alerta de empate de `_timestamp` no CDC (mais de uma versão com o mesmo `transaction_id` e `_timestamp`, resolvida pelo desempate D > U > I). *Teste*: um por check, usando a amostra (10 duplicadas, 22 em formato BR, 20 FAILED liquidadas, run 99).
    - **ALERT**: SETTLED posterior a REVERSED para o mesmo `transaction_id` (última versão SETTLED com `has_reversal` = true).
    - **BLOCK**: `amount_precision_loss` = true em `stg_settlements` (valor com mais de 2 casas decimais, ex.: `"1.234"`, que seria arredondado; o ponto sem vírgula é ambíguo entre milhar e decimal). *Teste*: CSV com `1.234` bloqueia; a amostra passa (0 ocorrências).
13. **Backfill ponta a ponta com o gerador** (`--rows 10000 --days 30`, CSVs diários). *Teste*: nenhum block, distribuição de categorias próxima de 85/5/5/5 e nova execução sem diferença.
14. **Queries de exemplo** em `queries/`. *Teste*: cada uma roda sem erro contra o banco gerado no teste 13.
15. **README**: seções "Entrega" e Parte 3.

## Verificação

- `uv run pytest` e `uv run ruff check .` verdes em cada etapa.
- `docker compose exec pipeline python -m recon run --reference-date 2025-03-16 --settlement-file docs/sample-data/settlement_paysettler.csv`, executado duas vezes, com as contagens de `marts.*` iguais nas duas. Em `ops.dq_results` devem aparecer os alertas esperados.
- `make generate` seguido de `recon backfill` no intervalo gerado. `fct_reconciliation_daily` deve ficar coerente.

## Premissas

1. A categoria oficial é a recalculada. A do serviço é guardada para comparação.
2. A chave do estado atual das transações é `transaction_id`, com desempate por `_timestamp` e depois `id`. Linhas com Op D saem do matching. Em empate total (mesmo `transaction_id`, `_timestamp` e `id`), vale a ordem do ciclo de vida **D > U > I**: o delete é o último evento.
3. A janela é `created_at::date ∈ [D-7, D]` em UTC. Transações FAILED/PENDING entram no matching, e o status vira atributo e alerta.
4. `amount` é sempre positivo, e REVERSED entra com sinal negativo em `signed_amount`.
5. Valores ficam em moeda original, sem câmbio.
6. Para a amostra, a data de referência vem do usuário (`--reference-date` + `--settlement-file`). A data não é inferida a partir do conteúdo do arquivo.

7. No CSV, se o mesmo `transaction_id` tiver versões distintas com o mesmo `settled_at`, ganha `REVERSED`, depois o maior `processor_reference`, depois o maior `amount`.
8. Vale a última versão por `settled_at`, mesmo que seja SETTLED e exista um REVERSED mais antigo. `has_reversal` fica true e o caso gera alerta (etapa 12).

## Evoluções (vão para o README, fora do escopo)

- `dim_date` (calendário, dia útil, feriados).
- Alerta de anomalia de volume (comparar com a média móvel de 7 dias).
- Raw/staging incrementais e particionados por data, em vez de recriar tudo a cada execução (pensando em ~5M/dia).
- Controle de arquivos já ingeridos (hash) quando o raw passar a ser acumulado.
