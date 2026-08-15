# Modelo Semântico — Medidas DAX (Power BI)

Camada de consumo sobre o star schema Gold (`gold_fact_loan`, `gold_dim_borrower`,
`gold_dim_loan`, `gold_dim_date`). Todas as medidas vivem em uma tabela dedicada
`_Measures`, sem relacionamentos, para manter o painel de campos organizado.

## Volume

| Medida | DAX | Formato |
|---|---|---|
| Total Loans | `COUNTROWS(gold_fact_loan)` | `#,##0` |
| Total Emprestado | `SUM(gold_fact_loan[loan_amnt])` | `"R$" #,##0` |
| Total Recebido | `SUM(gold_fact_loan[total_pymnt])` | `"R$" #,##0` |
| Ticket Médio | `DIVIDE([Total Emprestado], [Total Loans])` | `"R$" #,##0` |

## Risco

| Medida | DAX | Formato |
|---|---|---|
| Defaulted Loans | `CALCULATE(COUNTROWS(gold_fact_loan), gold_fact_loan[is_default] = 1)` | `#,##0` |
| Default Rate % | `DIVIDE([Defaulted Loans], [Total Loans])` | `0.00%` |
| Total Charge-Off (bruto) | `CALCULATE(SUM(gold_fact_loan[funded_amnt]) - SUM(gold_fact_loan[total_rec_prncp]), gold_fact_loan[is_default] = 1)` | `"R$" #,##0` |
| Total Recuperado | `CALCULATE(SUM(gold_fact_loan[recoveries]), gold_fact_loan[is_default] = 1)` | `"R$" #,##0` |
| Portfolio Loss Rate % (líquida) | `DIVIDE([Total Charge-Off] - [Total Recuperado], [Total Emprestado])` | `0.00%` |
| Recovery Rate % | `DIVIDE([Total Recuperado], [Total Charge-Off])` | `0.00%` |

## Taxa

| Medida | DAX | Formato |
|---|---|---|
| Taxa Média Ponderada | `DIVIDE(SUMX(gold_fact_loan, gold_fact_loan[loan_amnt] * gold_fact_loan[int_rate]), [Total Emprestado]) / 100` | `0.00%` |
| FICO Médio | `AVERAGE(gold_dim_borrower[fico_avg])` | `#,##0` |

`int_rate` é armazenado na Silver como número de 0–40 (ex.: 14.5 = 14,5%), não como
fração 0–1 — daí o `/ 100` na medida, para poder usar o mesmo formato `Percentual`
das demais taxas sem duplicar a multiplicação por 100.

---

## Achados de qualidade de dados

Duas descobertas feitas ao validar as medidas contra o modelo ao vivo, antes de
publicar o dashboard.

### 1. Recovery Rate > 100% — proxy de perda errado

A primeira versão de `Total Charge-Off` usava `out_prncp` (principal em aberto)
como proxy de prejuízo, filtrado por `is_default = 1`. Esse campo reflete o saldo
**atual**, não o saldo **no momento do default** — para contratos já baixados como
prejuízo ele tende a zerar. Como `Total Recuperado` (`recoveries`) acumula valores
de cobrança pós-charge-off ao longo de vários anos sobre uma base de comparação
artificialmente pequena, o resultado era `Recovery Rate % = 288,6%`.

**Correção:** `Total Charge-Off` passou a usar `funded_amnt - total_rec_prncp`
(principal não recuperado via pagamento normal) como prejuízo bruto — a base
padrão para esse tipo de análise no dataset Lending Club. Com isso:

| Métrica | Antes | Depois |
|---|---|---|
| Total Charge-Off | R$ 35,0 Mi (`out_prncp`) | R$ 901,4 Mi (`funded_amnt − total_rec_prncp`) |
| Recovery Rate % | 288,59% | 11,20% |
| Portfolio Loss Rate % (líquida) | 0,45% | 10,40% |

### 2. `gold_dim_date` não cobre 2007–2018

O README do pipeline descreve uma carteira 2007–2018, mas `gold_dim_date` tem
apenas **15 linhas**: jan–dez/2015 e jan–mar/2018. Um gráfico de tendência anual
direto (2015 → 2018) mostra uma queda de ~18% para ~6% de default rate que
**não é melhora real de risco** — é viés de safra imatura: contratos de
jan–mar/2018 ainda não tiveram tempo de entrar em default. O dashboard trata os
dois períodos como painéis separados em vez de uma única série temporal, com essa
ressalva explícita.

Vale investigar a etapa de amostragem da Bronze (ver `Limitações` no README
principal) antes de reportar qualquer tendência multi-ano a partir deste modelo.
