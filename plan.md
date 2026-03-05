# Plano de Implementação: Ads2u - Marketing Analytics Pipeline & AI Agent

## Fase 1: Infraestrutura
Nessa fase é criado todo a infraestrutura do projeto, como a configuração das variáveis de ambiente.

1. **Configuração do Repositório e Variáveis de Ambiente:**
   - Criar arquivo `.env`
   - Definir a estrutura base do projeto em Python

2. **Setup do Apache Airflow:**
   - Configurar os serviços do Airflow no `docker-compose.yml`.
   - Garantir o mapeamento correto de volumes para as pastas `./dags`, `./logs` e `./plugins`.
   - Inicializar o Airflow e garantir que a interface web esteja acessível.


## Fase 2: Ingestão de Dados
O objetivo aqui é criar as primeiras dags de coleta de dados brutos e colocar na camada `bronze` do banco de dados.

1. **Desenvolvimento da DAG `get_meta_data`:**
   - Criar script em Python utilizando a SDK/API do Meta Ads para extrair métricas de anúncios.
   - Criar as tabelas correspondentes no esquema `bronze` do Postgres.
   - Implementar a tarefa na DAG para carregar os dados extraídos no banco.

2. **Desenvolvimento da DAG `get_google_data`:**
   - Criar script em Python utilizando as APIs do Google Ads/Analytics.
   - Criar as tabelas no esquema `bronze`.
   - Implementar a tarefa na DAG para carga de dados.

## Fase 3: Processamento e Transformação
Nesta fase, será criado as transformações de dados brutos para dados padronizados

1. **Modelagem e Transformação Silver:**
   - Escrever consultas SQL para limpar dados nulos, padronizar tipos (ex: datas, moedas) e remover duplicatas dos dados da camada Bronze.
   - Criar tabelas/views no esquema `silver`.
   - Adicionar tarefas nas DAGs para processar de `bronze` para `silver`.

2. **Modelagem e Transformação Gold:**
   - Escrever consultas SQL para agregar dados e criar modelos focados em negócio.
   - Criar tabelas consolidadas no esquema `gold`.
   - Adicionar tarefas nas DAGs para processar de `silver` para `gold`.

## Fase 4: Consumo de Dashboards (Visualização)
1. **Conexão da Ferramenta de BI:**
   - Conectar uma ferramenta de Dashboard ao esquema `gold`.
   - Criar os visuais base.

## Fase 5: Relatórios Automatizados
Construir a funcionalidade de envio proativo de informações.

1. **Desenvolvimento da DAG `gen_report`:**
   - Criar a DAG que consolida as métricas principais do dia anterior.

2. **Módulo de Geração de Layout:**
   - Criar script para formatar os dados.

3. **Integração com Discord Webhook:**
   - Enviar o relatório finalizado para o canal especificado no Discord.

## Fase 6: O Bot do Discord
Aqui se inícia a criação do bot do Discord

1. **Criação do App no Discord Developer Portal:**
   - Registrar o bot, obter o Token e configurar as permissões necessárias (leitura de mensagens, envio de mensagens).

2. **Desenvolvimento Base do Bot:**
   - Criar um script (ex: `agent/bot.py`) utilizando a biblioteca `discord.py`.
   - Implementar comandos básicos de teste (ex: `!ping`) para garantir que o bot responde no servidor.
   - Containerizar o bot (adicioná-lo ao `docker-compose.yml` como um serviço separado).

## Fase 7: Integração de Inteligência (O Analytics Agent)
Esta é a fase é on de a LLM entra para criar valor ao negócio.

1. **Conexão da LLM com a Camada Gold:**
   - A LLM deve receber os dados da camada `gold` e fazer uma análise de desempenho dos anúncios juntos e tirar valor dessa análise.

2. **Conexão com o Bot:**
   - O Bot irá comunicar o time qual foi a análise da LLM e então o time poderá tomar decisões

## Fase 8: Testes, Refinamento e Deploy Final
1. **Testes End-to-End:**
   - Simular um ciclo completo do pipeline.

2. **Monitoramento e Alertas:**
   - Configurar alertas de falha no Airflow para o Discord.
   
3. **Documentação Final:**
   - Atualizar o `README.md` com instruções completas.