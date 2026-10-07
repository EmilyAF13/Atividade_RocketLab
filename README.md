# CineData Analytics — Arquitetura Medalhão no Databricks Lakehouse

Este repositório contém a solução completa da **Atividade 2 – Arquitetura Medalhão (Projeto CineData Analytics)** desenvolvida em **Databricks com PySpark e Delta Lake**.

O objetivo do projeto é estruturar um catálogo de filmes de bases combinadas (TMDB e IMDb) — entregues inicialmente na pasta `Base_de_Dados` com dados fragmentados e intencionalmente sujos — implementando todo o pipeline de ETL/ELT nas camadas **Bronze**, **Silver** e **Gold** (Star Schema dimensional) e respondendo a perguntas estratégicas de negócio.

---

## 📁 Estrutura do Projeto

Os notebooks foram estruturados de forma modular e sequencial em formato `.ipynb`, seguindo as melhores práticas ensinadas em aula:

```text
├── 00_Organizacao_do_Ambiente.ipynb   # Configuração de catálogo, schemas (Unity Catalog), Volume e validação
├── 01_Ingestao_Bronze.ipynb           # Ingestão pura dos 5 CSVs + API Banco Central (PTAX Dólar) em modo Append
├── 02_Bronze_to_Silver.ipynb          # Higienização, tipagem, normalização, deduplicação e Forward Fill cambial
├── 03_Silver_to_Gold.ipynb            # Modelagem Dimensional (Star Schema) e Desafio de Analytics
├── Base_de_Dados/                     # Arquivos brutos de origem (.csv)
│   ├── movies_info_TMDB_IMDB.csv
│   ├── movies_financials_IMDB_TMDB.csv
│   ├── movies_metrics_IMDB_TMDB.csv
│   ├── credits_and_tags_IMDB_TMDB.csv
│   └── movies_reviews.csv
├── Atividade_2.pdf                    # Especificação e requisitos da atividade
└── README.md                          # Documentação técnica e guia de execução
```

---

## 🏛️ Arquitetura Medalhão Implementada

```
  +---------------------------------------------------------------------------------+
  |                                  LANDING ZONE                                   |
  |             Volume: /Volumes/cinedata_analytics/bronze/inputs                   |
  |  (movies_info, movies_financials, movies_metrics, credits_and_tags, reviews)    |
  +---------------------------------------+-----------------------------------------+
                                          |
                                          v
  +---------------------------------------------------------------------------------+
  |                                 CAMADA BRONZE                                   |
  |  - Ingestão pura sem alteração estrutural ou de conteúdo                        |
  |  - Adição do metadado 'ingestion_datetime' (timestamp exato)                    |
  |  - Gravação Delta em modo Append                                                |
  |  - Ingestão da API do Banco Central (PTAX Dólar) com widgets de data            |
  |  Tabelas:                                                                       |
  |    * bronze.tb_movies_info                                                      |
  |    * bronze.tb_movies_financials                                                |
  |    * bronze.tb_movies_metrics                                                   |
  |    * bronze.tb_credits_and_tags                                                 |
  |    * bronze.tb_movies_reviews                                                   |
  |    * bronze.tb_cotacao_dolar                                                    |
  +---------------------------------------+-----------------------------------------+
                                          |
                                          v
  +---------------------------------------------------------------------------------+
  |                                 CAMADA SILVER                                   |
  |  - Nenhuma alteração nas tabelas Bronze                                         |
  |  - Nomes em português e tipagem segura (try_cast)                               |
  |  - Deduplicação inteligente pelo 'ingestion_datetime' mais recente              |
  |  - Tradução e normalização de status dos filmes                                 |
  |  - Tratamento de datas multi-formato (yyyy-MM-dd, dd/MM/yyyy, MM-dd-yyyy, etc.)  |
  |  - Forward Fill na cotação do dólar para cobrir finais de semana e feriados     |
  |  - Conversão cambial (USD -> BRL), cálculo de Lucro e Margem de Lucro %         |
  |  - Explode de gêneros e filtragem de ruídos/Column Shift fora do domínio        |
  |  - Consolidação unificada de Atores, Diretores, Roteiristas e Produtoras        |
  |  Tabelas:                                                                       |
  |    * silver.tb_info_filmes                                                      |
  |    * silver.tb_cotacao_dolar (série contínua)                                   |
  |    * silver.tb_financeiro_filmes                                                |
  |    * silver.tb_metricas_engajamento                                             |
  |    * silver.tb_avaliacoes_usuarios                                              |
  |    * silver.tb_generos                                                          |
  |    * silver.tb_pessoas_empresas                                                 |
  +---------------------------------------+-----------------------------------------+
                                          |
                                          v
  +---------------------------------------------------------------------------------+
  |                                  CAMADA GOLD                                    |
  |                           Modelagem Dimensional (Star Schema)                   |
  |  - Chaves substitutas (Surrogate Keys - BIGINT) via window row_number()         |
  |  - Dimensões descritivas e tabelas ponte (Bridge) para relações N:N             |
  |  - Fato com métricas consolidadas por filme lançado (sem duplicação de grão)     |
  |  Tabelas:                                                                       |
  |    * gold.dim_movies                                                            |
  |    * gold.dim_genres                                                            |
  |    * gold.dim_people                                                            |
  |    * gold.dim_companies                                                         |
  |    * gold.dim_reviews                                                           |
  |    * gold.bridge_movie_genre                                                    |
  |    * gold.bridge_movie_person                                                   |
  |    * gold.bridge_movie_company                                                  |
  |    * gold.fact_movies_performance                                               |
  +---------------------------------------------------------------------------------+
```

---

## 🔍 Regras de Negócio e Tratamentos Especiais

### 1. Ingestão da API do Banco Central & Forward Fill Cambial
- **API Banco Central (PTAX)**: Realiza consulta automatizada ao serviço OData do BCB com datas parametrizadas via Widgets do Databricks (`data_inicio` e `data_fim`).
- **Série Temporal Contínua**: Como o BCB não opera em finais de semana e feriados, foi construído um calendário contínuo com `sequence()` diária e aplicado **Forward Fill** (`last(cotacao, ignorenulls=True).over(...)`), garantindo que qualquer dia sem cotação receba o valor do último dia útil anterior.

### 2. Tratamento de Datas Multi-Formato
- O campo original `release_date` possuía padrões mistos (`yyyy-MM-dd`, `dd/MM/yyyy`, `MM-dd-yyyy`, `MM/dd/yyyy`, `dd-MM-yyyy`).
- Implementado `F.coalesce()` testando todos os formatos antes de converter em `DateType`, tratando apenas valores estritamente impossíveis como `NULL`.
- Derivação automática de `ano_lancamento`.

### 3. Tratamento de Column Shift e Inconsistências Numéricas
- **Notas e Votos**: Textos espalhados decorrentes de quebras de delimitadores foram tratados com conversão segura (`try_cast`), evitando parada do pipeline.
- **Validação de Escala**: Notas médias de TMDB e IMDb fora do intervalo válido de 0 a 10 (incluindo valores com escala incorreta como 63.65 e 80.55) foram desconsideradas e convertidas para `NULL`. Votos negativos foram invalidados para `NULL`.
- **Popularidade**: Limpeza de vírgulas decimais e conversão robusta para `DoubleType`.
- **Financeiro**: Remoção de textos como `"Unknown"` e `"Não Informado"`, higienização de símbolos cambiais (`$`, `USD`), multiplicadores (`K` e `M`), conversão cambial para `BRL`, cálculo de Lucro e Margem de Lucro Percentual protegida contra divisão por zero.

### 4. Gêneros e Entidades (Pessoas e Empresas)
- **Gêneros**: Explode após unificação de delimitadores (`;` e `|` para vírgula), com filtro estrito sobre o domínio de gêneros válidos de cinema para expurgar resíduos de *Column Shift* (sinopses, números e nomes de arquivos `.jpg`).
- **Dimensão Unificada de Pessoas e Empresas**: Consolidação de `cast` ('Ator'), `directors` ('Diretor'), `writers` ('Roteirista') e `production_companies` ('Produtora'), com padronização de capitalização (`initcap`), descarte de ruídos numéricos e deduplicação.

---

## ⭐ Modelagem Dimensional na Camada Gold

### Tabela Fato
- **`gold.fact_movies_performance`**:
  - **Grão**: 1 linha por filme lançado (`status_filme == 'Lançado'`).
  - **Chave Estrangeira (FK)**: `sk_movie_id BIGINT`.
  - **Métricas Financeiras (`DECIMAL(18,2)`)**: `orcamento_usd`, `receita_usd`, `lucro_usd`, `orcamento_brl`, `receita_brl`, `lucro_brl`.
  - **Métricas de Engajamento**: `popularidade`, `nota_media_tmdb`, `qtd_votos_tmdb`, `nota_media_imdb`, `qtd_votos_imdb`.

### Tabelas Dimensão
- **`gold.dim_movies`**: `sk_movie_id` (PK), `id_filme`, `titulo`, `data_lancamento`, `ano_lancamento`, `duracao_minutos`, `idioma_original`, `status_filme`, `sinopse`.
- **`gold.dim_genres`**: `sk_genre_id` (PK), `nome_genero`.
- **`gold.dim_people`**: `sk_person_id` (PK), `nome_pessoa`, `tipo_pessoa` (`'Ator'`, `'Diretor'`, `'Roteirista'`).
- **`gold.dim_companies`**: `sk_company_id` (PK), `nome_produtora`.
- **`gold.dim_reviews`**: `sk_review_id` (PK), `sk_movie_id` (FK), `qtd_avaliacoes_usuarios`, `nota_media_usuarios`.

### Tabelas-Ponte (Bridge Tables)
Evitam a explosão de grão da Fato nas relações N:N:
- **`gold.bridge_movie_genre`**: `(sk_movie_id, sk_genre_id)`
- **`gold.bridge_movie_person`**: `(sk_movie_id, sk_person_id)`
- **`gold.bridge_movie_company`**: `(sk_movie_id, sk_company_id)`

---

## 📊 Desafio de Analytics — Resolução das Perguntas de Negócio

No notebook `03_Silver_to_Gold.ipynb`, as seguintes consultas foram desenvolvidas e executadas via `display()`:

| # | Pergunta de Negócio | Abordagem / Tabelas Utilizadas |
|---|---|---|
| **1** | **Qual é a receita total (em R$) somada de todos os filmes da base?** | `SUM(receita_brl)` na `gold.fact_movies_performance`. |
| **2** | **Quais são os 5 filmes com maior popularidade?** | Join entre `fact_movies_performance` e `dim_movies`, ordenado por `popularidade DESC` (`LIMIT 5`). |
| **3** | **Quantos filmes cada gênero possui?** | Join entre `bridge_movie_genre` e `dim_genres`, agrupando por `nome_genero` com `COUNT(DISTINCT sk_movie_id) DESC`. |
| **4** | **Para os 10 filmes de maior receita, mostre título, receita (em US$ e R$) e a posição no ranking?** | Função de janela `RANK().over(Window.orderBy(receita_usd.desc()))` aplicada sobre os filmes com receita preenchida. |
| **5** | **Qual ator teve a maior quantidade de participações nos filmes lançados nos últimos 2 anos?** | Janela temporal calculada a partir da data de lançamento mais recente realizada na base (`max_date <= current_date()`), conectando `bridge_movie_person` (`tipo_pessoa = 'Ator'`) e `dim_movies`. |
| **6** | **Qual a produtora de filmes teve o maior Lucro nos últimos 5 anos?** | Janela dos últimos 5 anos a partir da data máxima realizada, conectando `bridge_movie_company`, `dim_companies`, `dim_movies` e `fact_movies_performance` para somar `lucro_brl`. |

---

## 🚀 Como Executar no Databricks

1. **Importar os Notebooks**:
   - No Databricks Workspace, clique em **Workspace** → **Import** e faça o upload dos 4 arquivos `.ipynb`.
2. **Carregar os Arquivos CSV no Volume**:
   - Execute o notebook `00_Organizacao_do_Ambiente.ipynb` para criar o catálogo, schemas e o Volume `inputs`.
   - No Databricks, vá em **Catalog** → `cinedata_analytics` → `bronze` → `inputs` → **Upload to this volume** e faça o upload dos 5 arquivos da pasta `Base_de_Dados`.
3. **Executar Sequencialmente**:
   - Execute `00_Organizacao_do_Ambiente.ipynb` para validar a presença dos arquivos.
   - Execute `01_Ingestao_Bronze.ipynb` para popular a camada Bronze e a tabela de cotação do dólar.
   - Execute `02_Bronze_to_Silver.ipynb` para aplicar todas as transformações e gerar as 7 tabelas Silver.
   - Execute `03_Silver_to_Gold.ipynb` para construir o Star Schema e visualizar as respostas analíticas no Databricks.
