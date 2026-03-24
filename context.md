# Contexto

Trabalho em uma agência de tráfego pago que tem vários clientes no Meta Ads e Google Ads, e preciso coletar os dados dessas plataformas para fazer relatórios diários para mandar para a equipe e relatórios semanais para mandar para os clientes. Também preciso criar alertas, de quando campanhas estão com desempenho ruim e quando a conta está com orçamento baixo.

Os relatórios não devem estar no streamlit, eles devem ficar no banco de dados.

# Requisitos

1. Pegar dados do Meta Ads
2. Pegad dados do Google Ads
3. Colocar dados no banco de dados
4. Criar dashboards
5. Criar relatórios diário
6. Criar relatórios semanais
7. Criar alertas de saldo
8. Criar alertas de baixo desempenho

# Tech Stack

Banco de dados: sqlite
Orquestrador: Airflow
Transformador: DBT
Dashboards: Streamlit

# Ideia de fluxo

Extrair os dados -> Carregador no Banco -> Tratar esses dados -> Criar o dashboard -> Criar relatórios -> Criar alertas

# Outputs

Relatórios Diário:
```
**Nome da Conta**
* Data : dd/mm/yyyy
* Investimento: R$0,0
* Impressões: 0
* Cliques: 0
* Conversões: 0
* Custo por conversão: R$0,0
* Taxa de Conversão: 0%
```


Relatório Semanal:
```
📊 Relatório Semanal – Nome da Conta
📅 Período: dd/mm a dd/mm

📢 Impressões: 0
🖱️ Cliques: 0
💬 Conversas iniciadas: 0
💲 Custo por mensagem: R$ 0,0
💰 Investimento: R$ 0,0
```