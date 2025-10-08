# Projeto Integrador 4: Análise de Dados II

## Análise de Ocorrências de Violência Contra a Mulher em Sorocaba e Votorantim

### Sumário
- [1. Sobre o Projeto](#1-sobre-o-projeto)
- [2. Tecnologias Utilizadas](#2-tecnologias-utilizadas)
- [3. O Processo ETL](#3-o-processo-etl)
  - [3.1. Extração (Extract)](#31-extração-extract)
  - [3.2. Transformação (Transform)](#32-transformação-transform)
  - [3.3. Carga (Load)](#33-carga-load)
- [4. Estrutura do Projeto](#4-estrutura-do-projeto)
- [5. Próximos Passos](#5-próximos-passos)
- [6. Autores](#6-autores)

---

### 1. Sobre o Projeto
Este projeto foi desenvolvido para a disciplina de "Projeto Integrador 4" e tem como objetivo principal a criação de um processo de **ETL (Extract, Transform, Load)** para tratar e analisar dados públicos de criminalidade do estado de São Paulo.

O foco da análise são as ocorrências registradas nas Delegacias de Defesa da Mulher (DDM) dos municípios de **Sorocaba** e **Votorantim**, visando gerar insights que possam ser utilizados em futuras visualizações e estudos sobre o tema.

Atualmente, o projeto consiste em um script em Python que realiza todo o processo de ETL a partir de um arquivo CSV local.

**Status do Projeto:** `Em desenvolvimento`

### 2. Tecnologias Utilizadas
- **Linguagem:** Python 3.9+
- **Biblioteca Principal:** Pandas
- **Ambiente de Desenvolvimento:** Jupyter Notebook / Google Colab (inicialmente), VS Codium

### 3. O Processo ETL
O script é dividido nas três etapas clássicas do ETL:

#### 3.1. Extração (Extract)
-   **Fonte de Dados:** Um arquivo local no formato CSV (`SPDadosCriminais_2025.csv`).
-   **Processo:** A função `extrair_dados` utiliza a biblioteca Pandas para ler o arquivo CSV e carregá-lo em um DataFrame, que é a estrutura de dados principal usada nas etapas seguintes.

#### 3.2. Transformação (Transform)
Esta é a etapa mais complexa, onde os dados brutos são limpos, filtrados e enriquecidos. A função `transformar_dados` realiza as seguintes operações:

1.  **Filtragem Geográfica:**
    -   Mantém apenas os registros cujos municípios são `SOROCABA` ou `VOTORANTIM`.
    -   Filtra as ocorrências para que sejam exclusivamente das delegacias `DDM SOROCABA` e `DDM VOTORANTIM`.

2.  **Limpeza e Formatação:**
    -   Converte a coluna `DATA_OCORRENCIA_BO` de texto para o formato `datetime`. Registros com datas inválidas são descartados.
    -   Valores nulos (`NaN`) em colunas textuais importantes (`HORA_OCORRENCIA_BO`, `BAIRRO`, etc.) são preenchidos com o valor padrão "Não Informado".

3.  **Enriquecimento de Dados (Criação de Novas Features):**
    -   Cria a coluna `MES_OCORRENCIA` extraindo o mês da data da ocorrência.
    -   Cria a coluna `DIA_SEMANA` com o nome do dia da semana em português (ex: "Segunda-feira").

4.  **Seleção de Colunas:**
    -   Ao final, apenas as colunas mais relevantes para a análise são mantidas e reordenadas, gerando um DataFrame final mais enxuto e organizado.

#### 3.3. Carga (Load)
-   **Processo:** A função `carregar_dados_bigquery` pega o DataFrame transformado e o envia diretamente para o Google BigQuery.
-   **Destino:** Dentro do Google BigQuery o DataFrame é enviado para o projeto `projetointegrador4-473718`, dentro do projeto existe um conjunto de dados chamado `dados_ssp`, e, por fim, uma tabela `dados_2025`.

### 4. Estrutura do Projeto

|-- Referências                 # Pasta com arquivos utilizados como referência sobre o tema 
|-- dados_tratados.csv          # Arquivo de saída (gerado pelo script e carregado no Google BigQuery)  
|-- Problema_PI4.docx           # Documento com a descrição do problema   
|-- script_etl.py               # Script principal do ETL  
|-- README.md                   # Documentação do projeto  

### 5. Próximos Passos
O desenvolvimento deste projeto continua, com os seguintes objetivos em mente:
-   [ ] **Visualização de Dados:** Aprimorar o dashboard que foi criado no Google LookerStudio, incluindo mais filtros e gráficos.
-   [ ] **Validação:** Validar junto aos Stakeholders se a proposta atendo os requisitos necessários.

### 6. Autores
- Débora Kocks Nogueira
- Giovana Perugini Guenka
- Stefanie Mayumi Inacio Kobayashi
- Vitória Lisauskaz Ferraz da Silva
