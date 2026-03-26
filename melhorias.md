# Melhorias — Ads2u Marketing Pipeline

## Concluído ✅

1. **Mais granularidade no Google Ads — palavras-chave**
   - Extractor usa `keyword_view` com métricas por keyword (customer, campaign, ad_group, keyword, impressions, clicks, spend, conversions)

2. **Mais granularidade no Meta Ads — nível de anúncio**
   - Extractor já usa `level: "ad"` com `ad_id` e `ad_name` nos campos extraídos

3. **Dados históricos (backfill)**
   - DAGs `backfill_meta_ads` e `backfill_google_ads` com trigger manual e param `days_back` (30/90/180). Cada conta itera os dias e retorna uma linha por dia por anúncio/keyword

4. **Google Ads — mais métricas**
   - `view_through_conversions`, `all_conversions`, `search_impression_share`, `quality_score` por keyword
   - `segments.device` (MOBILE, DESKTOP, TABLET, CONNECTED_TV, OTHER) por keyword
   - Demographics separados em `bronze.google_ads_demographics_raw`: gender, age_range, income_range via DAG `daily_extract_google_ads_demographics`

5. **Meta Ads — breakdown por dispositivo e posicionamento**
   - `device_platform` e `publisher_platform` como breakdowns no método `extract` principal

6. **Migrar banco de dados para PostgreSQL**
   - DuckDB não suportava múltiplas conexões simultâneas — bloqueava quando Airflow escrevia e dashboard lia ao mesmo tempo
   - Postgres já estava no stack (usado pelo Airflow)

7. **Retry automático por conta**
   - Extractors propagam erros em vez de silenciar (removido `try/except`)
   - `extract_account` nas 4 DAGs de extração tem `retries=3, retry_delay=5min`
   - Airflow faz retry por conta individualmente via dynamic task mapping (`.expand()`)

---

## Próximos — Por Prioridade

### Operacional (estabilidade antes de crescer)

**8. Alertas de falha do próprio pipeline**
- Se uma DAG falhar, receber notificação (email/WhatsApp) antes de perceber que os dados não chegaram

**9. Monitoramento do pipeline**
- Tabela `pipeline_runs` com tempo de execução, contas processadas, erros por DAG — para auditar o histórico

---


### Análise (Gold Layer)

**10. Detecção de anomalias mais sofisticada**
- Hoje os thresholds são fixos (30%/50%). Usar média móvel dos últimos 7 dias como baseline

**11. Ranking de campanhas por eficiência**
- ROAS, CPA, CTR rankeados por conta para identificar o melhor e pior performer

**12. Comparativo mês a mês (MoM)**
- Hoje só tem day-over-day nos alertas. Adicionar tabela gold com comparativo mensal

**13. Projeção de gasto mensal**
- Com base nos dias rodados, projetar quanto vai gastar no mês para comparar com o orçamento

---

### Entregas (Output)

**14. Envio de alertas por WhatsApp/Telegram**
- Quando um alerta `critical` for detectado, disparar mensagem imediata (não esperar o dia seguinte)

**15. Envio de relatórios por WhatsApp**
- Usar a Evolution API ou Z-API para enviar os textos já gerados em `gold.generated_reports` diretamente para o cliente

**16. Dashboard com Evidence.dev ou Metabase**
- Conectar direto no PostgreSQL para visualização
