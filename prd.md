# Documento de Requisitos do Produto (PRD)

**Nome do Produto:** Ads2u - Marketing Analytics Pipeline & AI Agent
**Data:** 4 de Março de 2026
**Status:** Em Especificação
**Documento Baseado em:** Arquitetura de Pipeline de Dados e Integração com Discord.

## 1. Resumo Executivo / Visão do Produto
O projeto consiste em uma plataforma de engenharia e análise de dados focada em marketing digital. O objetivo é automatizar a extração de dados de plataformas de anúncios (Meta Ads e Google), consolidá-los e tratá-los em um Data Warehouse relacional estruturado (PostgreSQL) e democratizar o acesso a esses dados através de Dashboards, envio automatizado de relatórios e um Agente de Inteligência Artificial interativo via Discord. O produto visa reduzir o tempo operacional de geração de relatórios e facilitar a tomada de decisão baseada em dados.

## 2. Objetivos e Metas
* **Centralização de Dados:** Criar uma "Fonte Única de Verdade" (Single Source of Truth) para dados de performance de marketing do Meta e Google.
* **Automação de Relatórios:** Eliminar o trabalho manual de exportação e formatação de planilhas.
* **Democratização do Acesso:** Permitir que stakeholders de marketing façam perguntas em linguagem natural diretamente no Discord e recebam respostas baseadas em dados atualizados.
* **Escalabilidade:** Utilizar a arquitetura medalhão para garantir que a entrada de novas fontes de dados no futuro não quebre os modelos de análise existentes.

## 3. Arquitetura Geral do Sistema
O sistema é orquestrado pelo **Apache Airflow**, armazenado no **PostgreSQL** (Arquitetura Medalhão) e consumido por um **Agente Analítico / Dashboards**, tendo o **Discord** como interface final.

* **Orquestração:** Apache Airflow
* **Armazenamento:** PostgreSQL (Bronze, Silver, Gold)
* **Frontend/Interface:** Dashboards (Ferramenta a definir, ex: Metabase/Superset) e Discord (Chatbot)
* **Integrações:** Meta Ads API, Google API, Discord API.

## 4. Casos de Uso (User Stories)
* **Como** Analista de Marketing, **eu quero** que meus dados do Meta e Google sejam atualizados diariamente sem intervenção manual, **para que** eu possa focar na análise e não na extração.
* **Como** Gestor de Tráfego, **eu quero** visualizar Dashboards de performance a partir de uma base de dados limpa (Camada Gold), **para que** eu possa acompanhar as métricas das campanhas em tempo real.
* **Como** Stakeholder/Diretor, **eu quero** poder perguntar ao Agente no Discord "Qual foi o ROI das campanhas do Meta ontem?" e receber uma resposta instantânea, **para que** eu possa tomar decisões rápidas sem precisar abrir um Dashboard complexo.
* **Como** Equipe de Marketing, **eu quero** receber um relatório diário automático no canal do Discord, **para que** todos comecem o dia alinhados sobre a performance do dia anterior.

## 5. Requisitos Funcionais (Funcionalidades)

### Epic 1: Ingestão de Dados (Data Extraction)
* **RF 01 - DAG `get_meta_data`:** O sistema deve possuir uma DAG no Airflow programada para conectar na API do Meta Ads, extrair as métricas de campanhas/conjuntos de anúncios/anúncios e carregar os dados no formato raw na camada Bronze do Postgres.
* **RF 02 - DAG `get_google_data`:** O sistema deve possuir uma DAG no Airflow para extrair dados das APIs do Google (Google Ads/Analytics) e carregar os dados em formato raw na camada Bronze.
* **RF 03 - Tratamento de Erros de Ingestão:** As DAGs devem possuir mecanismos de *retry* em caso de falha de conexão com as APIs externas (limites de taxa, indisponibilidade temporária).

### Epic 2: Processamento e Armazenamento (Data Transformation)
* **RF 04 - Camada Bronze (Raw):** O Postgres deve possuir esquemas/tabelas para armazenar os dados brutos exatamente como chegam das APIs, preservando o histórico.
* **RF 05 - Camada Silver (Enriched/Cleansed):** O sistema deve processar os dados da camada Bronze (limpeza de nulos, padronização de tipos de dados, deduplicação) e carregá-los na camada Silver.
* **RF 06 - Camada Gold (Business/Aggregated):** O sistema deve unir e agregar os dados da camada Silver para criar tabelas focadas em regras de negócio (ex: `fatos_performance_diaria`, `dimensao_campanha`), prontas para consumo direto.

### Epic 3: Geração de Relatórios e Dashboards
* **RF 07 - DAG `gen_report`:** O Airflow deve orquestrar a execução e formatação de relatórios consolidados baseados nos dados da camada Gold.
* **RF 08 - Envio de Relatórios para Discord:** O módulo de "Reports" deve ser capaz de postar as saídas da DAG `gen_report` (textos formatados, PDFs ou imagens de gráficos) em canais específicos do Discord.
* **RF 09 - Suporte a Dashboards:** A camada Gold deve ser otimizada (índices, views) para permitir a conexão de ferramentas externas de Dashboarding.

### Epic 4: Agente Analítico Conversacional (Discord AI)
* **RF 10 - Interface com Discord:** O "Analytics Agent" deve estar conectado ao Discord como um bot, ouvindo mensagens direcionadas a ele.
* **RF 11 - Consulta em Linguagem Natural:** O Agente deve converter perguntas do usuário em consultas (SQL ou via abstração de código) contra a camada Gold do banco de dados.
* **RF 12 - Comunicação Bidirecional (`gen_analytics`):** O Agente deve poder acionar a DAG `gen_analytics` para realizar rotinas analíticas pesadas (que demorariam muito tempo no runtime do bot) e a DAG deve retornar o status/resultados para o Agente notificar o usuário no Discord.

## 6. Requisitos Não Funcionais (NFRs)
* **Segurança e Credenciais:** Senhas de banco de dados, Tokens do Discord e Chaves de API do Google/Meta não devem ser hardcoded sob nenhuma hipótese. Devem utilizar gerenciadores de segredos (ex: Airflow Variables/Connections ou arquivos `.env` protegidos).
* **Desempenho (SLA):** O tempo de resposta do Analytics Agent no Discord para consultas simples na camada Gold não deve exceder 5 segundos. Consultas complexas devem avisar o usuário ("Estou processando...") e retornar via background.
* **Conformidade de Dados:** O sistema não deve extrair ou armazenar PII (Personally Identifiable Information - Informações Pessoais Identificáveis) dos usuários finais das plataformas de anúncios, focando apenas em métricas agregadas.
* **Log e Monitoramento:** O Airflow deve manter o histórico detalhado de execução (logs) nas pastas nativas (já visíveis na sua estrutura `/logs/dag_id=...`).

## 7. Fora de Escopo (Out of Scope - FASE 1)
* Criação de uma interface web/frontend proprietária para o Agente (o Discord será a única interface).
* Ingestão de dados de plataformas além do Google e Meta (ex: TikTok Ads, LinkedIn Ads) nesta fase inicial.
* Ações de leitura/escrita automatizada para *alterar* campanhas nas plataformas de anúncios (o sistema é estritamente de *leitura e análise*).
