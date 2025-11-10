# Projeto Integrador 4: Análise de Dados II

## Análise de Ocorrências de Violência Contra a Mulher em Sorocaba e Votorantim

### Sumário
- [1. Sobre o Projeto](#1-sobre-o-projeto)
- [2. Tecnologias Utilizadas](#2-tecnologias-utilizadas)
- [3. O Processo de Dados (Pipeline ETL)](#3-o-processo-de-dados-pipeline-etl)
  - [3.1. ETL 1: Ocorrências (Script_DDM)](#31-etl-1-ocorrências-script_ddm)
  - [3.2. ETL 2: Perfil do Agressor (Script_Produtividade)](#32-etl-2-perfil-do-agressor-script_produtividade)
  - [3.3. Visualização (Looker Studio)](#33-visualização-looker-studio)
- [4. Estrutura do Projeto](#4-estrutura-do-projeto)
- [5. Próximos Passos](#5-próximos-passos)
- [6. Autores](#6-autores)

---

### 1. Sobre o Projeto
Este projeto foi desenvolvido para a disciplina de "Projeto Integrador 4" e tem como objetivo principal a criação de um pipeline de **ETL (Extract, Transform, Load)** para tratar e analisar dados públicos de criminalidade do estado de São Paulo.

O foco da análise são as ocorrências registradas nas Delegacias de Defesa da Mulher (DDM) dos municípios de **Sorocaba** e **Votorantim**, visando gerar insights que possam ser utilizados em futuras visualizações e estudos sobre o tema.

Atualmente, o projeto consiste em dois scripts em Python (executados no Google Colab) que realizam todo o processo de ETL, desde o download dos arquivos do site da Secretaria de Segurança Pública até a carga dos dados tratados no Google BigQuery.

**Status do Projeto:** `Em desenvolvimento`

### 2. Tecnologias Utilizadas
- **Linguagem:** Python 3.9+
- **Bibliotecas Principais:** Pandas, Google Cloud BigQuery, Requests
- **Ambiente de Desenvolvimento:** Google Colab
- **Banco de Dados (Data Warehouse):** Google BigQuery
- **Ferramenta de Visualização (BI):** Google Looker Studio

### 3. O Processo de Dados (Pipeline ETL)
O pipeline é dividido em dois processos de ETL distintos, que alimentam um dashboard centralizado.

#### 3.1. ETL 1: Ocorrências (Script_DDM)
Este script (`Script_DDM.ipynb`) é responsável por tratar os dados gerais das ocorrências.

* **Extração (Extract):**
    * Baixa as planilhas `SPDadosCriminais_*.xlsx` (anos 2022-2025) do site da Secretaria de Segurança Pública.
* **Transformação (Transform):**
    * Consolida todas as abas de todos os arquivos em um único DataFrame.
    * Filtra apenas registros dos municípios `SOROCABA` e `VOTORANTIM` e das delegacias `DDM SOROCABA` e `DDM VOTORANTIM`.
    * Converte `DATA_OCORRENCIA_BO` para datetime e trata valores nulos.
    * Cria colunas de enriquecimento, como `mes_ocorrencia` e `dia_semana`.
    * Renomeia as colunas para um padrão amigável (ex: `NUM_BO` -> `codigo_bo`).
* **Carga (Load):**
    * Carrega o DataFrame tratado na tabela `dados_ssp.dados_ddm` dentro do projeto `projetointegrador4-473718` no Google BigQuery.
    * Utiliza o modo `WRITE_TRUNCATE`, garantindo que a tabela seja sempre substituída pelos dados mais recentes a cada execução.

#### 3.2. ETL 2: Perfil do Agressor (Script_Produtividade)
Este script (`Script_Produtividade.ipynb`) foca em extrair dados de produtividade policial para traçar o perfil dos agressores.

* **Extração (Extract):**
    * Baixa as planilhas `DadosProdutividade_*.xlsx` (anos 2024-2025) do site da SSP.
* **Transformação (Transform):**
    * Lê apenas as abas que começam com `PRESOS E APREENDIDOS` de cada arquivo.
    * Aplica os mesmos filtros geográficos (Sorocaba e Votorantim) e de delegacia (DDM).
    * Renomeia colunas específicas do perfil do autor, como `SEXO_PESSOA` -> `sexo_autor`, `IDADE_PESSOA` -> `idade_autor`, `COR_CURTIS` -> `raca_autor`, etc.
    * Realiza a limpeza e formatação dos dados.
* **Carga (Load):**
    * Carrega o DataFrame tratado na tabela `dados_ssp.dados_produtividade` no mesmo projeto do BigQuery.
    * (Nota: A tabela `perfil_agressor` vista no BigQuery é uma *view* ou tabela derivada criada a partir da `dados_produtividade`).

#### 3.3. Visualização (Looker Studio)
O resultado final do pipeline é consumido por um dashboard no Looker Studio.

* **Fonte de Dados:**
    * O dashboard (visto no arquivo `Dashboard_-_Violência_Contra_a_Mulher.pdf`) conecta-se diretamente às tabelas `dados_ssp.dados_ddm`, `dados_ssp.dados_produtividade` e `dados_ssp.perfil_agressor` no BigQuery.
* **Análises:** O painel exibe visualizações sobre:
    * Evolução temporal das ocorrências (por data, mês, dia da semana, período).
    * Localização geográfica (mapa de calor e ranking de bairros).
    * Tipos de crime (Lesão Corporal Dolosa, Estupro, etc.).
    * Perfil do Agressor (Raça, Sexo, Escolaridade, Tipo de Prisão).
    * Perfil da Vítima (com base em dados históricos do SINAN).

### 4. Estrutura do Projeto

| Nome do arquivo | Descrição |
| -------- | ----- |
| `Script_DDM.ipynb` | Notebook Colab do ETL de Ocorrências (Fonte: SPDadosCriminais). |
| `Script_Produtividade.ipynb` | Notebook Colab do ETL de Perfil do Agressor (Fonte: DadosProdutividade). |
| `dados_ddm.csv` | Arquivo CSV com os dados baixados extraídos e tratados das ocorrências registradas nas DDMs de Sorocaba e Votorantim |
| `dados_produtividade.csv` | Arquivo CSV com os dados baixados extraídos e tratados das prisões e apreensões vinculadas à DDMs de Sorocaba e Votorantim |
| `perfil_agressor.csv` | Arquivo CSV derivado do arquivo dados_produtividade.csv |
| `perfil_vitima.csv` | Arquivo CSV com os dados extraídos de atendidos de agressão extraídos do SINAN |
| `Dashboard_-_Violência_Contra_a_Mulher.pdf` | PDF de exemplo do dashboard no Looker Studio. |
| `README.md` | Documentação do projeto. |
| `Relatório Final - PI4.docx` | Documento com o relatório completo do projeto. |
| `Referências/` | Pasta com arquivos utilizados como referência sobre o tema. |

### 5. Próximos Passos
O desenvolvimento deste projeto continua, com os seguintes objetivos em mente:
-   [ ] **Visualização de Dados:** Aprimorar o dashboard que foi criado no Google LookerStudio, incluindo mais filtros e gráficos.
-   [ ] **Validação:** Validar junto aos Stakeholders se a proposta atende os requisitos necessários.
-   [ ] **Automação:** Configurar a execução automática dos scripts (ex: via Google Cloud Functions ou Workflows) para manter os dados atualizados.

### 6. Autores
- Débora Kocks Nogueira
- Giovana Perugini Guenka
- Stefanie Mayumi Inacio Kobayashi
- Vitória Lisauskaz Ferraz da Silva
