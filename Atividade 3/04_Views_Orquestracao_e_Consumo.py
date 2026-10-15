# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 04 · Views, Materialized Views e introdução a Jobs
# MAGIC
# MAGIC > **Objetivo:** transformar a Gold em um produto de dados governado, comparar os quatro tipos de view e preparar o primeiro Job no Databricks.
# MAGIC
# MAGIC **Pré-aula:** execute `03_Silver_to_Gold`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Da Gold ao consumo
# MAGIC
# MAGIC ![Fluxo da Arquitetura Medalhão](./images/04_medallion_flow.png)
# MAGIC
# MAGIC A Gold está modelada, mas acesso direto às fatos ainda exige conhecer grão, chaves substitutas e joins. A camada de consumo entrega um **contrato**:
# MAGIC
# MAGIC - **abstração:** a regra é escrita uma vez;
# MAGIC - **segurança:** o consumidor só enxerga as colunas autorizadas;
# MAGIC - **performance:** cálculos repetidos podem ser materializados;
# MAGIC - **estabilidade:** dashboards não quebram quando a implementação interna muda.

# COMMAND ----------

import pyspark.sql.functions as F

catalogo = "olist_medallion"
schema_ouro = f"{catalogo}.gold"

objetos_necessarios = [
    "dim_date", "dim_customer", "dim_product",
    "fact_order_items", "fact_orders",
]

for objeto in objetos_necessarios:
    nome_completo = f"{schema_ouro}.{objeto}"
    assert spark.catalog.tableExists(nome_completo), f"Execute o notebook 03: falta {nome_completo}"

print("✅ Contrato da Gold disponível")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Uma família de views — quatro ciclos de vida
# MAGIC
# MAGIC “View” não é um único objeto. A diferença principal é **onde a definição vive**, **por quanto tempo** e **se o resultado é armazenado**.
# MAGIC
# MAGIC ![Tabela, View e Materialized View](./images/04_table_view_mv.png)
# MAGIC
# MAGIC | Objeto | Escopo e duração | Armazena resultado? | Nome de acesso | Uso típico |
# MAGIC |---|---|---:|---|---|
# MAGIC | Temp View | sessão atual | Não | `minha_view` | transformação intermediária |
# MAGIC | Global Temp View | aplicação Spark atual | Não | `global_temp.minha_view` | compartilhar entre sessões do mesmo cluster |
# MAGIC | View persistente | catálogo; sobrevive ao cluster | Não | `catalogo.schema.view` | contrato, segurança e self-service |
# MAGIC | Materialized View | catálogo; sobrevive ao cluster | **Sim** | `catalogo.schema.mv` | consulta cara e repetida |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Views persistentes: abstração e segurança juntas
# MAGIC
# MAGIC A pergunta “quanto faturamos por estado, segmento e mês?” exige fato de itens, cliente e data. Se cada relatório repetir esses joins, teremos várias versões da mesma métrica.
# MAGIC
# MAGIC A view persistente registra a regra no catálogo. Ela não copia dados: ao consultar, o Spark expande a definição e lê as tabelas de origem.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {schema_ouro}.vw_receita_estado_mes AS
SELECT
    cliente.estado_cliente                          AS uf,
    cliente.segmento_cliente,
    data.year_month                                 AS ano_mes,
    COUNT(DISTINCT item.id_pedido)                  AS pedidos,
    SUM(item.item_price)                            AS receita_produtos,
    SUM(item.item_freight_value)                    AS receita_frete,
    SUM(item.item_total)                            AS receita_total,
    ROUND(AVG(item.item_total), 2)                  AS ticket_medio_item
FROM {schema_ouro}.fact_order_items item
JOIN {schema_ouro}.dim_customer cliente
  ON item.id_consumidor = cliente.id_consumidor
JOIN {schema_ouro}.dim_date data
  ON item.date_sk = data.date_sk
WHERE item.status_pedido <> 'cancelado'
GROUP BY cliente.estado_cliente, cliente.segmento_cliente, data.year_month
""")

display(
    spark.table(f"{schema_ouro}.vw_receita_estado_mes")
    .orderBy(F.desc("receita_total"))
    .limit(10)
)

# COMMAND ----------

# DBTITLE 1,Cell 7
spark.sql(f"""
CREATE OR REPLACE VIEW {schema_ouro}.vw_sla_entrega_estado_mes AS
SELECT
    cliente.estado_cliente                                              AS uf,
    data.year_month                                                     AS ano_mes,
    COUNT(*)                                                            AS pedidos_entregues,
    SUM(CASE WHEN pedido.kpi_sla_entrega = 'NO_PRAZO' THEN 1 ELSE 0 END) AS pedidos_no_prazo,
    ROUND(
        100 * SUM(CASE WHEN pedido.kpi_sla_entrega = 'NO_PRAZO' THEN 1 ELSE 0 END)
        / COUNT(*),
        2
    )                                                                   AS percentual_no_prazo,
    ROUND(AVG(pedido.tempo_entrega_dias), 1)                            AS media_dias_entrega
FROM {schema_ouro}.fact_orders pedido
JOIN {schema_ouro}.dim_customer cliente
  ON pedido.id_consumidor = cliente.id_consumidor
JOIN {schema_ouro}.dim_date data
  ON pedido.date_sk = data.date_sk
WHERE pedido.status_pedido = 'entregue'
GROUP BY cliente.estado_cliente, data.year_month
""")

print(f"View publicada: {schema_ouro}.vw_sla_entrega_estado_mes")
display(
    spark.table(f"{schema_ouro}.vw_sla_entrega_estado_mes")
    .orderBy(F.desc("pedidos_entregues"))
    .limit(10)
)


# COMMAND ----------

# MAGIC %md
# MAGIC Por conta de estarmos em uma versão free do databricks, não conseguimos criar Materilized, Temp e Global Views

# COMMAND ----------

# MAGIC %md
# MAGIC A governança acompanha o objeto:
# MAGIC
# MAGIC ```sql
# MAGIC GRANT SELECT ON VIEW olist_medallion.gold.vw_customer_publico TO `analistas`;
# MAGIC -- não conceder SELECT direto em gold.dim_customer
# MAGIC ```
# MAGIC
# MAGIC No Unity Catalog, *row filters* e *column masks* complementam essa proteção. A view continua útil porque também esconde joins e estabiliza o contrato.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Materialized View × tabela
# MAGIC
# MAGIC Uma MV **parece uma tabela na leitura**: ambas aceitam `SELECT` e ambas armazenam linhas. A diferença está em quem mantém o conteúdo e **quando vale cada uma**.
# MAGIC
# MAGIC | Caso de uso | Tabela | Materialized View |
# MAGIC |---|---|---|
# MAGIC | Definição | esquema + dados escritos pelo pipeline | consulta declarativa |
# MAGIC | Atualização em lote | código escreve `append`, `merge` ou `overwrite` | refresh agendado ou sob demanda |
# MAGIC | Streaming / refresh incremental | o notebook precisa processar o delta e gravar | o motor aplica só as mudanças desde o último refresh, quando a consulta é compatível |
# MAGIC | Custo em fonte contínua | cresce com o código de merge e com o volume reprocessado | tende a crescer com o **delta novo**, não com o histórico |
# MAGIC | Dependências | conhecidas pelo código | registradas na consulta e na linhagem |
# MAGIC | Regra de negócio | pode ficar espalhada em notebooks | centralizada no DDL |
# MAGIC | Acesso de BI | expõe o modelo físico se for a fato | expõe apenas o contrato agregado |
# MAGIC
# MAGIC Dar acesso à fato é entregar o **armazém**: grão, chaves e risco de soma incorreta. Dar acesso à MV é entregar o **produto**: mês, segmento e receita já definidos e governados.
# MAGIC
# MAGIC ```sql
# MAGIC CREATE MATERIALIZED VIEW olist_medallion.gold.mv_receita_estado_mes
# MAGIC AS
# MAGIC SELECT ...;
# MAGIC ```
# MAGIC
# MAGIC **Streaming, na prática:** chegaram vendas de setembro. A tabela só fica certa se o pipeline fizer merge do lote novo. A MV incremental atualiza os grupos afetados de setembro — não reagrupa todos os anos. Por isso a MV é o padrão de serving contínuo quando a consulta é cara e repetida.
# MAGIC
# MAGIC
# MAGIC > Prefira MV quando a consulta for cara e repetida, a regra precisar de governança e a latência de refresh atender ao negócio. Para transformação intermediária ou leitura rara, uma view comum costuma bastar.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Introdução a Databricks Jobs
# MAGIC
# MAGIC Notebook executado manualmente depende da memória de quem opera. Um Job transforma a ordem em um DAG reproduzível:
# MAGIC
# MAGIC O Job desta aula terá:
# MAGIC
# MAGIC 1. Bronze → Silver → Gold;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fechamento
# MAGIC
# MAGIC - Temp View: sessão atual.
# MAGIC - Global Temp View: aplicação Spark atual, namespace `global_temp`.
# MAGIC - View persistente: contrato governado sem armazenar resultado.
# MAGIC - Materialized View: contrato governado com resultado mantido por refresh.
# MAGIC - Job: DAG parametrizado, observável e seguro para reexecução.
# MAGIC
# MAGIC **Próximo notebook:** `05_Validacao_e_Orquestracao` — checks executáveis, task values e bloqueio do pipeline.
# MAGIC
# MAGIC ### Referências
# MAGIC
# MAGIC - [Spark SQL — Temporary Views](https://spark.apache.org/docs/latest/sql-getting-started.html#global-temporary-view)
# MAGIC - [Databricks — CREATE VIEW](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-view)
# MAGIC - [Databricks — Materialized Views](https://docs.databricks.com/aws/en/views/materialized)
# MAGIC - [Databricks — Jobs](https://docs.databricks.com/aws/en/jobs/)
# MAGIC - [Unity Catalog — Row filters and column masks](https://docs.databricks.com/aws/en/tables/row-and-column-filters)