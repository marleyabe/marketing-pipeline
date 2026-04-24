# Regras de Negócio

| ID | Regra | Teste |
|----|-------|-------|
| RN01 | `yesterday` = D-1 UTC | `test_rn01_*` |
| RN02 | `lastweek` = seg a dom da semana anterior | `test_rn02_*` |
| RN03 | `lastmonth` = dia 1 ao último do mês anterior | `test_rn03_*` |
| RN04 | Range: `end-date` ≥ `start-date`; ausente = mesmo dia | `test_rn04_*` |
| RN05 | Cliente por id retorna plataformas onde existe (0/1/2) | `test_rn05_*` |
| RN06 | Token expira se `last_used_at` < now − 14 dias | `test_rn06_*` |
| RN07 | Uso atualiza `last_used_at` (rolling) | `test_rn07_*` |
| RN08 | Token revogado rejeita sempre | `test_rn08_*` |
| RN09 | Apenas `admin` em `/users`, `/createuser` | `test_rn09_*` |
| RN10 | Divisor 0 → NULL (NULLIF no SQL) | `test_rn10_*` |
| RN11 | Bronze nunca exposto via API | `test_rn11_*` |
| RN12 | User `disabled_at` → token rejeita | `test_rn12_*` |
| RN13 | Email único em `users.app_users` | `test_rn13_*` |
| RN14 | Gold só linhas com `spend > 0` (`HAVING SUM(spend) > 0`) | `test_rn14_*` |
| RN15 | `/clientes` lista quem teve spend > 0 alguma vez | `test_rn15_*` |
| RN16 | DAG: ambos vazios = ontem; ambos = range; só 1 = erro | `test_rn16_*` |
| RN17 | `/review` consolida performance, campaigns, keywords, search_terms, negatives, budget + signals | `test_rn_review_queries` |
| RN18 | `ops.client_budget` é fonte única do teto mensal; pacing = spend_mtd vs budget vs dias corridos | `test_rn_budget_queries` |
| RN19 | Google search_term_view + negatives são snapshots diários (carimbados com `snapshot_date`) | dbt schema tests |
| RN20 | Signals são regras puras com thresholds centralizados em `review_flags.py` | `test_rn_review_flags` |

## Detalhes

### RN10 — métricas derivadas

| Métrica | Fórmula | Zero |
|---|---|---|
| ctr | clicks × 100 / impressions | NULL |
| cpc | spend / clicks | NULL |
| cost_per_conversion | spend / conversions | NULL |
| conversion_rate | conversions × 100 / clicks | NULL |
| roas | conversion_value / spend | NULL |

### RN17 — /review

`GET /review/{account_id}?start-date=&end-date=` devolve JSON para MCP/dashboard com:

- `performance` (spend, CTR, CPC, CPA, ROAS, conversion_value)
- `campaigns` (status + ROAS)
- `keywords_top` (top 20 por spend + quality_score)
- `search_terms` (buckets `top_by_spend_no_conv` e `high_roas`)
- `negatives` (total + campanhas sem cobertura)
- `budget` (pacing: `over` | `on_track` | `under` | `unknown`)
- `signals` (flags disparadas)

Range ausente ⇒ `lastmonth()`.

### RN18 — budget pacing

- Cadastro manual em `ops.client_budget` via `PUT /budget/{id}` (chave `(account_id, platform)`).
- View `gold.budget_pacing` compara `spent_mtd` com `monthly_budget` e percentual de dias corridos do mês.
- `pace_flag`: `over` se pct_consumed > pct_mês+10pp; `under` se < pct_mês−10pp; `on_track` caso contrário; `unknown` se sem cadastro ou budget=0.

### RN19 — snapshots diários

- `bronze.google_search_terms_raw`: 1 linha por (date, customer, ad_group, search_term, matched_keyword).
- `bronze.google_negatives_raw`: 1 linha por (snapshot_date, customer, scope, criterion_id). `scope ∈ {campaign, ad_group}`. `gold.google_negatives` mantém só o snapshot mais recente.
- Extractor normaliza NULL → `''` nos campos da chave natural para permitir ON CONFLICT em re-runs.

### RN20 — signals

Regras puras em `src/api/reviews/review_flags.py`. Thresholds:

| Flag | Disparo | Severity |
|---|---|---|
| `roas_below_one` | ROAS < 1 e spend ≥ R$ 50 | critical |
| `budget_over_pace` | `pace_flag = over` | warning |
| `budget_under_pace` | `pace_flag = under` | info |
| `campaigns_with_zero_conversions` | ≥ 1 campanha com spend ≥ R$ 50 e 0 conversões | warning |
| `search_terms_wasting_spend` | ≥ 5 search terms sem conversão no bucket `top_by_spend_no_conv` | warning |
| `campaigns_without_negatives` | campanhas com spend no período sem nenhuma negativa associada | info |
