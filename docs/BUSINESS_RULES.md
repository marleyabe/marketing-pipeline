# Regras de Negócio

| ID | Regra | Teste |
|----|-------|-------|
| RN01 | `yesterday` = D-1 UTC | `test_rn01_yesterday_is_d_minus_1` |
| RN02 | `lastweek` = segunda a domingo da semana anterior | `test_rn02_lastweek_*` |
| RN03 | `lastmonth` = dia 1 ao último do mês anterior | `test_rn03_lastmonth_*` |
| RN04 | Range: `end-date` ≥ `start-date`; ausente = mesmo dia | `test_rn04_range_*` |
| RN05 | Cliente por id retorna plataformas onde existe (0/1/2) | `test_rn05_cliente_*` |
| RN06 | Token expira se `last_used_at` < now - 14 dias | `test_rn06_*` |
| RN07 | Uso atualiza `last_used_at` (rolling) | `test_rn07_token_use_refreshes_validity` |
| RN08 | Token revogado rejeita sempre | `test_rn08_revoked_token_rejected` |
| RN09 | Apenas role `admin` em `/users`, `/createuser` | `test_rn09_*` |
| RN10 | Métricas: divisor 0 → NULL (NULLIF no SQL) | `test_rn10_*` |
| RN11 | Bronze nunca exposto via API | `test_rn11_*` |
| RN12 | User com `disabled_at` → token rejeita | `test_rn12_disabled_user_token_rejected` |
| RN13 | Email único em `users.app_users` | `test_rn13_*` |
| RN14 | Gold só contém linhas com `spend > 0` (filtro `HAVING SUM(spend) > 0`) | `test_rn14_*` |
| RN15 | `/clientes` lista quem teve `spend > 0` alguma vez (lê de `gold.daily_performance`) | `test_rn15_*` |
| RN16 | DAG de extração: ambos `start_date` e `end_date` vazios = ontem; ambos preenchidos = range; 1 só = erro | `test_rn16_*` |

## Detalhes

### RN10 — métricas derivadas

| Métrica | Fórmula | Zero |
|---------|--------|------|
| ctr | clicks * 100 / impressions | NULL |
| cpc | spend / clicks | NULL |
| cost_per_conversion | spend / conversions | NULL |
| conversion_rate | conversions * 100 / clicks | NULL |

### RN06 — janela de inatividade

Token novo (`last_used_at IS NULL`) **não** expira. Após primeiro uso, vale 14 dias por uso. Cada hit reinicia janela.

### RN14 — gold filtra spend zero

Cliente sem investimento no agrupamento (dia, semana) some do gold. Silver mantém tudo p/ debug. Critério: `HAVING SUM(spend) > 0`.

### RN15 — cliente "ativo"

`/clientes` e `/clientes/{id}` leem `gold.daily_performance`. Cliente listado se algum dia já teve `spend > 0`. Sem janela rolling.

### RN16 — datas na DAG

DAGs de extração (`google_ads`, `meta_ads`) aceitam params `start_date` e `end_date` (ISO `YYYY-MM-DD`):

| Caso | Comportamento |
|------|---------------|
| Cron diário (sem trigger manual) | params vazios → busca ontem |
| Trigger manual sem params | ontem |
| Ambos preenchidos | range inclusivo `start..end` |
| `end_date < start_date` | erro |
| Só `start_date` ou só `end_date` | erro |
