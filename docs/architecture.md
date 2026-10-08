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
| `fct_reconciliation` | (reference_date, processor, transaction_id) | Ops / Compliance. Recalculada: full outer join entre `stg_settlements` de D e `stg_transactions` (não deletadas) (todas, sem filtro de data; `in_window` marca `created_at::date ∈ [D-7, D]`); |dif| ≤ 0,01 → MATCHED. Guarda os valores, a diferença, a categoria, `settlement_status`, `has_reversal`, `internal_status`, `is_late_settlement`, `settled_date`, `created_date` e `source_category` (vinda do serviço). |
| `fct_reconciliation_daily` | (reference_date, processor, currency) | Ops: contagem e valor por categoria, taxa de MISMATCHED (`MISMATCHED / (MATCHED + MISMATCHED)`), taxa de REVERSED e divergência com o serviço. Não tem contagem de alertas (removida na etapa 10): os alertas ficam em `ops.dq_results`. |
| `fct_reconciliation_run` | run do serviço | Ops / Compliance: status, duração, total declarado × linhas no arquivo × resultados. |
| `fct_financial_daily` | (reference_date, processor, settled_date, currency, merchant_id) | CFO: só liquidações, por data de liquidação: volume liquidado, revertido e líquido, e contagens. Escopo por `reference_date` e `processor` (cada liquidação vem de um único CSV). Sem câmbio: valores separados por moeda. |
| `fct_internal_transactions` | transaction_id | CFO: volume interno, uma linha por transação não deletada de `stg_transactions` (valor, moeda, status, `created_date`). **Reconstrução total a cada run**, sem escopo por data nem janela: a mesma transação nunca é contada duas vezes. Agregar por `created_date`/moeda/status nas queries. |
| `audit_reconciliation_history` | (pipeline_run_id, reference_date, processor, transaction_id) | Compliance: snapshot de cada execução, só acrescentado. |

### ops: observabilidade
- `ops.pipeline_runs`: run_id, reference_date, status (RUNNING/SUCCESS/FAILED), started/finished, duração e contagem de linhas por etapa, settlement_file, git sha e erro.
- `ops.dq_results`: run_id, reference_date, check, severidade, aprovado/reprovado, valor observado, limite e amostra de chaves.

## 3. Idempotência por `reference_date`

1. **CLI**: `recon run --reference-date D`. Rodar de novo para a mesma data substitui o resultado anterior. Não há lock: o DuckDB permite um único processo escrevendo no arquivo.
2. **raw e staging**: recriados por inteiro a cada execução (`CREATE OR REPLACE`). Sem estado entre execuções, não há duplicação possível.
3. **marts**: numa **única transação**, `DELETE WHERE reference_date = D` seguido de `INSERT ... SELECT` em todos os fatos com escopo por data. `dim_merchant` e `fct_internal_transactions` são recriadas por inteiro a partir do staging (substituição completa, na mesma transação). `audit_reconciliation_history` é a única tabela só acrescentada, com chave por `pipeline_run_id`.
4. **Falha no meio**: os checks `block` rodam **antes** da transação dos marts. Se um falhar, nada é escrito e o run fica FAILED com o erro em `ops.pipeline_runs`. Uma exceção dentro da transação dispara ROLLBACK e os marts continuam como antes.
   - **Padrão stage → check → publish**: os fatos são montados primeiro em tabelas temporárias. Os checks `block` (incluindo conservação e unicidade da chave) rodam sobre essas tabelas e gravam `ops.dq_results`. Só então a transação faz `DELETE` + `INSERT` a partir delas. Assim `ops.dq_results` nunca é gravado dentro de uma transação que pode sofrer rollback, e se um block falhar nenhum mart é tocado.
   - **Invariante: run `FAILED` ⇒ marts intocados.** Todos os checks (block e alert) rodam sobre staging e stage **antes** do publish, e `publish_marts` é o último passo antes de `finish_run`. Falha em qualquer etapa anterior, ou exceção dentro do publish (ROLLBACK), deixa os marts como estavam. Todo motivo de bloqueio fica em `ops.dq_results`, inclusive falhas na carga raw, que gravam um block na camada `raw` antes de o run virar FAILED: arquivo de liquidação ausente/ambíguo/vazio (`settlement_file_missing`), parquet ausente ou ilegível (`raw_cdc_load_failed`) e erro de leitura do CSV (`raw_settlement_read_failed`). Esses três são registrados pelo pipeline (`record_block`), fora da lista declarativa `CHECKS`.
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
- Conservação: settlements deduplicados = MATCHED + MISMATCHED + UNRECONCILED_PROCESSOR, e transações internas com `in_window` = MATCHED + MISMATCHED + UNRECONCILED_INTERNAL (internas fora da janela só contam se casaram com o CSV).

**ALERT** (o pipeline publica e o alerta fica em `ops.dq_results`):
- Linhas duplicadas idênticas no CSV.
- Mesmo `transaction_id` com versões diferentes no CSV (`version_count` > 1), incluindo `has_reversal`.
- Normalizações aplicadas: valor em formato BR, `settled_at` sem fuso, `merchant_id` em minúsculas.
- **Liquidação de transação interna com status FAILED.**
- `merchant_id` nulo nas transações, ou merchant inexistente em `dim_merchant`.
- Replay de CDC: mais de uma linha I para o mesmo `transaction_id`.
- Empate de `_timestamp` no CDC: versões distintas do mesmo `transaction_id` com o mesmo `_timestamp` (desempatadas por D > U > I).
- REVERSED órfão (sem par interno).
- Liquidação tardia: `settled_date − created_date` > 7 dias, **por data (UTC), não por timestamp** (decisão da etapa 8, coerente com a janela por `created_at::date`).
- `reconciliation_results` com `run_id` órfão.
- Categoria recalculada ≠ `source_category`, ou `difference` gravado ≠ |interno − processador|.
- `total_transactions` do run ≠ linhas do arquivo, ou run do serviço FAILED/sem resultados para D.
- Taxa de MISMATCHED > **5%** (`config.MISMATCH_RATE_ALERT`, configurável). **Definição: `MISMATCHED / (MATCHED + MISMATCHED)`**, só os pares efetivamente comparados (UNRECONCILED_* ficam fora do denominador). Sem pares comparados a taxa é NULL e não alerta. Mesma definição em `fct_reconciliation_daily.mismatch_rate` e no check `mismatch_rate_high`.
- `amount` NULL ou ≤ 0 em transação interna não deletada (`transaction_amount_nonpositive`).

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
9b. **Orquestração (`pipeline.py` + CLI `run`)**: raw → staging → checks (block + alert) → stage → checks (block + alert) → `publish_marts` (último passo), com `ops.pipeline_runs` e `ops.dq_results`. *Teste*: um block que falha deixa o run FAILED em `ops.pipeline_runs`, o motivo registrado em `ops.dq_results` e os marts intactos.
10. **Agregados** (`fct_reconciliation_daily`, `fct_reconciliation_run`, `fct_financial_daily`, `fct_internal_transactions`, `dim_merchant`). *Teste*: os totais batem com o fato; com CSVs diferentes em 3 `reference_date` de janelas sobrepostas, a soma de `fct_internal_transactions` = soma de `stg_transactions` e a de `fct_financial_daily` = soma das liquidações dos CSVs.
11. **Auditoria** só acrescentada. *Teste*: duas execuções geram dois snapshots e um único fato atual.
12. **Checks block/alert restantes**, com um novo tipo de check **`metric`**: o SQL retorna um valor observado (escalar) em vez de linhas, e o check passa se o valor respeita o limite. Os alertas de taxa (ex.: MISMATCHED > 5%) gravam a taxa real em `ops.dq_results.observed_value`. Os checks de linhas (modelo da etapa 7) continuam valendo para os demais. Inclui alerta de empate de `_timestamp` no CDC (mais de uma versão com o mesmo `transaction_id` e `_timestamp`, resolvida pelo desempate D > U > I). *Teste*: um por check, usando a amostra (10 duplicadas, 22 em formato BR, 20 FAILED liquidadas, run 99).
    - **ALERT**: SETTLED posterior a REVERSED para o mesmo `transaction_id` (última versão SETTLED com `has_reversal` = true).
    - **BLOCK**: `stg_settlements` deve vir de exatamente 1 `_source_file` (`count(DISTINCT _source_file) = 1`). `fct_reconciliation` não filtra o arquivo de D no SQL e depende de `raw.settlements` conter só o CSV de D. *Teste*: staging com 2 arquivos bloqueia.
    - **BLOCK**: conservação do diário: `fct_reconciliation_daily_stage` deve bater com `fct_reconciliation_stage` (soma de `total_count` = linhas do fato; contagens e valores por categoria iguais, por moeda). *Teste*: stage do diário adulterado bloqueia.
    - **BLOCK**: `amount_precision_loss` = true em `stg_settlements` (valor com mais de 2 casas decimais, ex.: `"1.234"`, que seria arredondado; o ponto sem vírgula é ambíguo entre milhar e decimal). *Teste*: CSV com `1.234` bloqueia; a amostra passa (0 ocorrências).
13. **Backfill ponta a ponta com o gerador** (`--rows 10000 --days 30`, CSVs diários). *Teste*: nenhum block, distribuição de categorias próxima de 85/5/5/5 e nova execução sem diferença.
14. **Queries de exemplo** em `queries/`. *Teste*: cada uma roda sem erro contra o banco gerado no teste 13.
15. **README**: seções "Entrega" e Parte 3.

## Resultados medidos (etapa 13)

Dataset do gerador: `--rows 10000 --days 30 --merchants 100 --seed 42`, backfill de 2025-03-01 até o último CSV diário.

- **Concordância com o gabarito**: 100% (10.000 de 10.000 transações) entre a categoria recalculada e `reconciliation_results` do gerador, com dados sujos e com `--clean`, sem nenhuma divergência. Comparação por `transaction_id`; os REVERSED órfãos plantados (30 no sujo) não têm linha no gabarito e ficam de fora. Coberto por `tests/test_agreement_generated.py` (2.000 linhas, 10 dias, seed fixa, sujo e clean).
- **Mistura de categorias** (uma categoria final por transação, dados sujos): MATCHED 85,4% / MISMATCHED 4,8% / UNRECONCILED_PROCESSOR 5,0% / UNRECONCILED_INTERNAL 4,9%, contra 85/5/5/5 do gerador.
- **Blocks**: nenhum nas datas com CSV. Com dados sujos, as únicas falhas são `settlement_file_missing` em 4 datas sem arquivo (2025-04-02, 04-05, 04-10, 04-13): só as liquidações tardias (D+8 a D+15) chegam depois de 04-01, e o gerador não escreve CSV para data sem linhas. Com `--clean` nenhuma data falta.
- **Idempotência**: rodar o backfill duas vezes deixa todos os marts idênticos (contagem e hash do conteúdo); só `audit_reconciliation_history` cresce.
- **Desempenho**: cerca de **0,9 s por `reference_date`** (45 datas em 38,8 s no sujo; 32 datas em 29,6 s no clean).
- **Custo dominante**: reler o CDC inteiro a cada run (raw e staging são recriados por execução). Cada run reprocessa todas as ~9,5 mil linhas de transações, mesmo que só o CSV de D mude (soma de `raw.transactions` nas 82 runs bem-sucedidas: ~780 mil linhas, para ~9,5 mil linhas de CSV). É o ponto a atacar na escala de ~5M/dia (ver Evoluções).

## Verificação

- `uv run pytest` e `uv run ruff check .` verdes em cada etapa.
- `docker compose exec pipeline python -m recon run --reference-date 2025-03-16 --settlement-file docs/sample-data/settlement_paysettler.csv`, executado duas vezes, com as contagens de `marts.*` iguais nas duas. Em `ops.dq_results` devem aparecer os alertas esperados.
- `make generate` seguido de `recon backfill` no intervalo gerado. `fct_reconciliation_daily` deve ficar coerente.

## Premissas

1. A categoria oficial é a recalculada. A do serviço é guardada para comparação.
2. A chave do estado atual das transações é `transaction_id`, com desempate por `_timestamp` e depois `id`. Linhas com Op D saem do matching. Em empate total (mesmo `transaction_id`, `_timestamp` e `id`), vale a ordem do ciclo de vida **D > U > I**: o delete é o último evento.
3. A janela `created_at::date ∈ [D-7, D]` (UTC) define o **escopo de `UNRECONCILED_INTERNAL`**, não o matching. O matching é por `transaction_id` contra todas as internas não deletadas: liquidação cuja interna está fora da janela (ex.: tardia, D-10) é classificada por valor/moeda (MATCHED/MISMATCHED), com `in_window = false` e `is_late_settlement = true`, como faz o serviço. Interna fora da janela sem par no CSV não entra no fato. `UNRECONCILED_PROCESSOR` só vale quando a transação não existe internamente (ou foi deletada). Transações FAILED/PENDING entram no matching, e o status vira atributo e alerta.
4. `amount` é sempre positivo, e REVERSED entra com sinal negativo em `signed_amount`.
5. Valores ficam em moeda original, sem câmbio.
6. Para a amostra, a data de referência vem do usuário (`--reference-date` + `--settlement-file`). A data não é inferida a partir do conteúdo do arquivo.

7. No CSV, se o mesmo `transaction_id` tiver versões distintas com o mesmo `settled_at`, ganha `REVERSED`, depois o maior `processor_reference`, depois o maior `amount`.
8. Vale a última versão por `settled_at`, mesmo que seja SETTLED e exista um REVERSED mais antigo. `has_reversal` fica true e o caso gera alerta (etapa 12).

## Limites

- **Sem migrations.** O DDL usa `CREATE TABLE IF NOT EXISTS`: mudança de schema em tabela existente não é aplicada. Após qualquer mudança de schema, apague `data/warehouse.duckdb` e reprocesse (`recon backfill`).

## Evoluções (vão para o README, fora do escopo)

- `dim_date` (calendário, dia útil, feriados).
- Alerta de anomalia de volume (comparar com a média móvel de 7 dias).
- Raw/staging incrementais e particionados por data, em vez de recriar tudo a cada execução (pensando em ~5M/dia).
- Controle de arquivos já ingeridos (hash) quando o raw passar a ser acumulado.
- Atribuição por processor: hoje as linhas só internas de `fct_reconciliation` recebem o `processor` passado ao build (válido com um único processor). Com 2+ processadores será preciso atribuir cada transação interna ao seu processor (campo na origem ou regra de roteamento).
- Escala do lado interno de `fct_reconciliation`: hoje o join varre todas as transações internas não deletadas, sem filtro de data. Para ~5M/dia, restringir o lado interno aos `transaction_id` do CSV de D mais as internas da janela `[D-7, D]`.
- Volume da auditoria: `audit_reconciliation_history` copia o fato inteiro a cada run (só acrescentado). Para ~5M/dia, particionar por mês e definir política de retenção (ex.: arquivar partições antigas em Parquet).
- Leitura tolerante do CSV: o `read_csv` do DuckDB aceita CSV malformado sem erro (aspas abertas podem fundir linhas, e um arquivo só com cabeçalho e aspas soltas vira "vazio"). Em produção, adicionar um check (block) que compare a contagem física de linhas do arquivo com as linhas lidas em `raw.settlements`.
