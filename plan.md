# Plano de Implementação: Ads2u - Marketing Analytics Pipeline & AI Agent

## Fase 1: Fundação e Infraestrutura (A Base)
O objetivo desta fase é criar o ambiente onde tudo vai rodar. Sem isso, não temos onde colocar código ou dados.

1. **Configuração do Repositório e Variáveis de Ambiente:**
   - Criar arquivo `.env` para armazenar de forma segura senhas do banco, chaves de API (Meta, Google) e tokens do Discord (adicionando ao `.gitignore`).
   - Definir a estrutura base do projeto em Python (gerenciamento de dependências com `requirements.txt` ou `poetry`).
2. **Setup do PostgreSQL (Data Warehouse):**
   - Configurar o serviço do PostgreSQL no `docker-compose.yml`.
   - Criar os esquemas iniciais (`schema`) para separar as camadas da arquitetura medalhão: `bronze`, `silver` e `gold`.
3. **Setup do Apache Airflow:**
   - Configurar os serviços do Airflow (Webserver, Scheduler, Worker, Postgres de metadados) no `docker-compose.yml`.
   - Garantir o mapeamento correto de volumes para as pastas `./dags`, `./logs` e `./plugins`.
   - Inicializar o Airflow e garantir que a interface web esteja acessível.
4. **Conexões do Airflow:**
   - Configurar na interface do Airflow as conexões (Connections) para o Postgres e as APIs (Meta, Google), utilizando as variáveis seguras.

## Fase 2: Ingestão de Dados - Camada Bronze (A Coleta)
O objetivo aqui é buscar os dados das plataformas e guardá-los "crus" (raw) no nosso banco de dados.

1. **Desenvolvimento da DAG `get_meta_data`:**
   - Criar script em Python utilizando a SDK/API do Meta Ads para extrair métricas de campanhas, conjuntos de anúncios e anúncios.
   - Criar as tabelas correspondentes no esquema `bronze` do Postgres.
   - Implementar a tarefa na DAG para carregar os dados extraídos no banco.
2. **Desenvolvimento da DAG `get_google_data`:**
   - Criar script em Python utilizando as APIs do Google Ads/Analytics.
   - Criar as tabelas no esquema `bronze`.
   - Implementar a tarefa na DAG para carga de dados.
3. **Validação da Ingestão:**
   - Testar execuções manuais das DAGs e verificar se os dados estão chegando corretamente no Postgres (camada Bronze).

## Fase 3: Processamento e Transformação - Camadas Silver e Gold (O Refinamento)
Nesta fase, pegamos os dados crus e os transformamos em informações úteis para o negócio.

1. **Modelagem e Transformação Silver (Limpeza):**
   - Escrever consultas SQL para limpar dados nulos, padronizar tipos (ex: datas, moedas) e remover duplicatas dos dados da camada Bronze.
   - Criar tabelas/views no esquema `silver`.
   - Adicionar tarefas nas DAGs (ou criar uma DAG separada) para processar de Bronze para Silver.
2. **Modelagem e Transformação Gold (Regras de Negócio):**
   - Escrever consultas SQL para agregar dados e criar modelos focados em negócio (ex: `fato_performance_diaria_unificada`, `dimensao_campanha`).
   - Criar tabelas consolidadas no esquema `gold`.
   - Adicionar tarefas para processar de Silver para Gold.

## Fase 4: O Bot do Discord - Fundação (A Interface)
Criar a "casca" do Analytics Agent no Discord antes de conectá-lo aos dados.

1. **Criação do App no Discord Developer Portal:**
   - Registrar o bot, obter o Token e configurar as permissões necessárias (leitura de mensagens, envio de mensagens).
2. **Desenvolvimento Base do Bot (Python):**
   - Criar um script (ex: `agent/bot.py`) utilizando a biblioteca `discord.py`.
   - Implementar comandos básicos de teste (ex: `!ping`) para garantir que o bot responde no servidor.
   - Containerizar o bot (adicioná-lo ao `docker-compose.yml` como um serviço separado).

## Fase 5: Integração de Inteligência (O Analytics Agent)
Esta é a fase mais complexa: conectar o Bot aos dados refinados e ao Airflow.

1. **Conexão do Bot com a Camada Gold:**
   - Implementar funções no bot para consultar o banco de dados Postgres (camada `gold`) de forma segura.
2. **Processamento de Linguagem Natural (LLM):**
   - Integrar uma API de LLM (ex: OpenAI, Gemini) ao bot.
   - Desenvolver o prompt de sistema ("system prompt") do bot para traduzir texto em SQL.
3. **Comunicação Bidirecional (`gen_analytics`):**
   - Criar a DAG `gen_analytics` no Airflow.
   - Implementar a lógica no bot para acionar a DAG `gen_analytics` via API do Airflow.
   - Implementar um mecanismo para o Airflow enviar o resultado de volta para o Discord quando a DAG terminar.

## Fase 6: Relatórios Automatizados (Push)
Construir a funcionalidade de envio proativo de informações.

1. **Desenvolvimento da DAG `gen_report`:**
   - Criar a DAG que consolida as métricas principais do dia anterior.
2. **Módulo de Geração de Layout:**
   - Criar script para formatar os dados (Markdown, gráficos ou PDF).
3. **Integração com Discord Webhook:**
   - Enviar o relatório finalizado para o canal especificado no Discord.

## Fase 7: Consumo de Dashboards (Visualização)
1. **Conexão da Ferramenta de BI:**
   - Conectar uma ferramenta de Dashboard (Metabase, Superset, etc.) ao esquema `gold`.
   - Criar os visuais base (Gastos, Cliques, Conversões, ROI).

## Fase 8: Testes, Refinamento e Deploy Final
1. **Testes End-to-End:**
   - Simular um ciclo completo do pipeline.
2. **Monitoramento e Alertas:**
   - Configurar alertas de falha no Airflow para o Discord.
3. **Documentação Final:**
   - Atualizar o `README.md` com instruções completas.