# Melhorias — Ads2u Marketing Pipeline

## Dados (Extração)

**1. Mais granularidade no Google Ads — palavras-chave** ✅
- Extractor usa `keyword_view` com métricas por keyword (customer, campaign, ad_group, keyword, impressions, clicks, spend, conversions)

**2. Mais granularidade no Meta Ads — nível de anúncio** ✅
- Extractor já usa `level: "ad"` com `ad_id` e `ad_name` nos campos extraídos

**3. Dados históricos (backfill)** ✅
- DAGs `backfill_meta_ads` e `backfill_google_ads` com trigger manual e param `days_back` (30/90/180). Cada conta itera os dias e retorna uma linha por dia por anúncio/keyword

**4. Google Ads — mais métricas** ✅
- `view_through_conversions`, `all_conversions`, `search_impression_share`, `quality_score` por keyword
- `segments.device` (MOBILE, DESKTOP, TABLET, CONNECTED_TV, OTHER) por keyword
- Demographics separados em `bronze.google_ads_demographics_raw`: gender, age_range, income_range via DAG `daily_extract_google_ads_demographics`

**5. Meta Ads — breakdown por dispositivo e posicionamento** ✅
- `device_platform` e `publisher_platform` adicionados ao extract principal via `breakdowns` da API
- Demographics separados em `bronze.meta_ads_demographics_raw`: age e gender via DAG `daily_extract_meta_ads_demographics`

---

## Análise (Gold Layer)

**6. Comparativo mês a mês (MoM)**
- Hoje só tem day-over-day nos alertas. Adicionar tabela gold com comparativo mensal

**7. Projeção de gasto mensal**
- Com base nos dias rodados, projetar quanto vai gastar no mês para comparar com o orçamento

**8. Ranking de campanhas por eficiência**
- ROAS, CPA, CTR rankeados por conta para identificar o melhor e pior performer

**9. Detecção de anomalias mais sofisticada**
- Hoje os thresholds são fixos (30%/50%). Usar média móvel dos últimos 7 dias como baseline

---

## Entregas (Output)

**10. Envio de relatórios por WhatsApp**
- Usar a Evolution API ou Z-API para enviar os textos já gerados em `gold.generated_reports` diretamente para o cliente

**11. Envio de alertas por WhatsApp/Telegram**
- Quando um alerta `critical` for detectado, disparar mensagem imediata (não esperar o dia seguinte)

**12. Dashboard com Evidence.dev ou Metabase**
- Conectar direto no DuckDB para visualização — sem banco extra

**16. Migrar banco de dados para PostgreSQL** ⚠️ URGENTE
- DuckDB não suporta múltiplas conexões simultâneas — bloqueia quando Airflow escreve e dashboard lê ao mesmo tempo
- Postgres já está no stack (usado pelo Airflow), basta apontar o pipeline para ele
- Requer reescrita do `DuckDBBronzeLoader` (remover sintaxe `BY NAME`) e dos SQLs de transformação em `sql/silver/` e `sql/gold/`

---

## Operacional

**13. Retry automático por conta**
- Hoje se uma conta falha, silencia o erro. Adicionar reprocessamento individual por conta com 3 tentativas

**14. Monitoramento do pipeline**
- Tabela `pipeline_runs` com tempo de execução, contas processadas, erros por DAG — para auditar o histórico

**15. Alertas de falha do próprio pipeline**
- Se uma DAG falhar, receber notificação (email/WhatsApp) antes de perceber que os dados não chegaram
