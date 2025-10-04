# Célula 1: Autenticação do Google
from google.colab import auth
auth.authenticate_user()

print('Autenticado com sucesso!')

# Célula 2: Processo de ETL
# --- INSTALAÇÕES E IMPORTS ---

import pandas as pd
from google.cloud import bigquery
import requests
import os

# =============================================
# ETAPA 1: EXTRAÇÃO
# =============================================

def extrair_e_consolidar_dados(lista_de_links, pasta_downloads='downloads'):
    """
    Recebe uma lista de URLs de arquivos Excel, baixa todos,
    lê e consolida em um único DataFrame.
    """
    print(f"Iniciando download de {len(lista_de_links)} arquivos.")

    # 1. Baixa os arquivos da lista
    if not os.path.exists(pasta_downloads):
        os.makedirs(pasta_downloads)

    for url_arquivo in lista_de_links:
        nome_arquivo = url_arquivo.split('/')[-1]
        caminho_arquivo = os.path.join(pasta_downloads, nome_arquivo)
        print(f"Baixando '{nome_arquivo}'...")

        with requests.get(url_arquivo, stream=True) as r:
            r.raise_for_status()
            with open(caminho_arquivo, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

    print("\nTodos os arquivos foram baixados com sucesso.")

    # 2. Lê todos os arquivos baixados e os junta em um único DataFrame
    lista_dfs = []
    for arquivo in os.listdir(pasta_downloads):
        if arquivo.endswith('.xlsx'):
            caminho_completo = os.path.join(pasta_downloads, arquivo)
            print(f"Lendo arquivo: {arquivo}")
            df_temp = pd.read_excel(caminho_completo)
            lista_dfs.append(df_temp)

    if not lista_dfs:
        print("Nenhuma planilha lida.")
        return None

    # Concatena todos os DataFrames da lista em um só
    df_consolidado = pd.concat(lista_dfs, ignore_index=True)
    print(f"\nDados consolidados! Total de {len(df_consolidado)} registros.")
    return df_consolidado


# =============================================
# ETAPA 2: TRANSFORMAÇÃO
# =============================================

def transformar_dados(df):
    """
    Função para filtrar, limpar e transformar o DataFrame.
    """
    if df is None:
        return None

    # --- FILTRAGEM ---
    delegacias_desejadas = ['DDM SOROCABA', 'DDM VOTORANTIM']
    municipios_desejados = ['SOROCABA', 'VOTORANTIM']
    df_filtrado = df[
        (df['NOME_DELEGACIA'].str.upper().isin(delegacias_desejadas)) &
        (df['NOME_MUNICIPIO'].str.upper().isin(municipios_desejados))
    ].copy()
    print(f"\nDados filtrados. {len(df_filtrado)} registros encontrados.")

    # --- LIMPEZA E TRANSFORMAÇÃO ---
    df_filtrado['DATA_OCORRENCIA_BO'] = pd.to_datetime(df_filtrado['DATA_OCORRENCIA_BO'], format='%d/%m/%Y', errors='coerce')
    df_filtrado.dropna(subset=['DATA_OCORRENCIA_BO'], inplace=True)

    colunas_para_preencher = ['DESC_PERIODO', 'DESCR_CONDUTA', 'BAIRRO', 'LOGRADOURO']
    for coluna in colunas_para_preencher:
        df_filtrado[coluna] = df_filtrado[coluna].fillna('Não Informado')

    df_filtrado['MES_OCORRENCIA'] = df_filtrado['DATA_OCORRENCIA_BO'].dt.month

    df_filtrado['DIA_SEMANA_EN'] = df_filtrado['DATA_OCORRENCIA_BO'].dt.day_name()
    mapa_dias = {
        'Monday': 'Segunda-feira', 'Tuesday': 'Terça-feira', 'Wednesday': 'Quarta-feira',
        'Thursday': 'Quinta-feira', 'Friday': 'Sexta-feira', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
    }
    df_filtrado['DIA_SEMANA'] = df_filtrado['DIA_SEMANA_EN'].map(mapa_dias)
    df_filtrado.drop(columns=['DIA_SEMANA_EN'], inplace=True)

    # --- BLOCO FINAL DE GARANTIA DOS TIPOS ---
    print("\nGarantindo os tipos de dados corretos antes da carga...")

    # Converte colunas que devem ser inteiros
    for coluna in ['ANO_ESTATISTICA', 'MES_OCORRENCIA']:
        if coluna in df_filtrado.columns:
            df_filtrado[coluna] = pd.to_numeric(df_filtrado[coluna], errors='coerce')
            df_filtrado.dropna(subset=[coluna], inplace=True)
            df_filtrado[coluna] = df_filtrado[coluna].astype(int)

    # Converte colunas que devem ser de ponto flutuante (float)
    for coluna in ['LATITUDE', 'LONGITUDE']:
        if coluna in df_filtrado.columns:
            # Primeiro, garante que a coluna é do tipo string para usar o .str
            df_filtrado[coluna] = df_filtrado[coluna].astype(str)
            # Substitui vírgula por ponto
            df_filtrado[coluna] = df_filtrado[coluna].str.replace(',', '.', regex=False)
            # Converte para numérico
            df_filtrado[coluna] = pd.to_numeric(df_filtrado[coluna], errors='coerce')
    # --- FIM DO BLOCO FINAL ---

    colunas_relevantes = [
        'NOME_DELEGACIA', 'NOME_MUNICIPIO', 'DATA_OCORRENCIA_BO', 'HORA_OCORRENCIA_BO',
        'DESC_PERIODO', 'DIA_SEMANA', 'MES_OCORRENCIA', 'ANO_ESTATISTICA', 'DESCR_SUBTIPOLOCAL', 'BAIRRO',
        'LOGRADOURO', 'LATITUDE', 'LONGITUDE', 'RUBRICA', 'DESCR_CONDUTA', 'NATUREZA_APURADA'
    ]
    colunas_existentes = [col for col in colunas_relevantes if col in df_filtrado.columns]
    df_transformado = df_filtrado[colunas_existentes]

    print("Dados transformados com sucesso!")
    return df_transformado


# =============================================
# ETAPA 3: CARGA PARA O GOOGLE BIGQUERY
# =============================================

def carregar_dados_bigquery(df, project_id, table_id, schema):
    """
    Função para carregar um DataFrame do Pandas em uma tabela do BigQuery
    usando um schema pré-definido.
    """
    if df is None or df.empty:
        print("DataFrame está vazio. Nenhum dado para carregar.")
        return

    full_table_id = f"{project_id}.{table_id}"
    client = bigquery.Client(project=project_id)
    print(f"\nConectado ao projeto '{project_id}'.")

    # Usando o schema, em vez de autodetect
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
    )

    print(f"Iniciando o carregamento de {len(df)} linhas para a tabela '{full_table_id}'...")
    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result()
    print(f"Carga de dados concluída com sucesso!")
    table = client.get_table(full_table_id)
    print(f"A tabela agora contém {table.num_rows} linhas.")


# =============================================
# --- DEFINIÇÃO DO SCHEMA PARA O BIGQUERY ---
# =============================================

schema_definido = [
    bigquery.SchemaField("NOME_DELEGACIA", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("NOME_MUNICIPIO", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DATA_OCORRENCIA_BO", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("HORA_OCORRENCIA_BO", "TIME", mode="NULLABLE"),
    bigquery.SchemaField("DESC_PERIODO", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DIA_SEMANA", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("MES_OCORRENCIA", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("ANO_ESTATISTICA", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("DESCR_SUBTIPOLOCAL", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("BAIRRO", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("LOGRADOURO", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("LATITUDE", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("LONGITUDE", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("RUBRICA", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DESCR_CONDUTA", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("NATUREZA_APURADA", "STRING", mode="NULLABLE"),
]


# =============================================
# --- FUNÇÃO DE DEBUG ---
# =============================================
def encontrar_valores_nao_numericos(df, colunas):
    """
    Verifica uma lista de colunas em um DataFrame e imprime os valores
    que não podem ser convertidos para números.
    """
    print("\n--- INICIANDO VERIFICAÇÃO DE VALORES NÃO NUMÉRICOS ---")
    problema_encontrado = False
    for coluna in colunas:
        if coluna in df.columns:
            # Força a conversão para número, erros viram NaN (Not a Number)
            numerico = pd.to_numeric(df[coluna], errors='coerce')

            # Encontra as linhas onde a conversão falhou (é NaN), mas o valor original não era vazio
            linhas_problematicas = df[numerico.isna() & df[coluna].notna()]

            if not linhas_problematicas.empty:
                problema_encontrado = True
                print(f"\n!! Problema na coluna '{coluna}'. Valores que não são números:")
                # Mostra os valores únicos que estão causando o problema
                print(linhas_problematicas[coluna].unique())

    if not problema_encontrado:
        print("--- NENHUM VALOR NÃO NUMÉRICO ENCONTRADO NAS COLUNAS VERIFICADAS ---")


# =============================================
# --- ROTEIRO PRINCIPAL COM DEBUG ---
# =============================================

# Lista manual com as URLs diretas para os arquivos
LINKS_DAS_PLANILHAS = [
    'https://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados/SPDadosCriminais_2022.xlsx',
    'https://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados/SPDadosCriminais_2023.xlsx',
    'https://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados/SPDadosCriminais_2024.xlsx',
    'https://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados/SPDadosCriminais_2025.xlsx'
]

# 1. Extração
dados_consolidados = extrair_e_consolidar_dados(LINKS_DAS_PLANILHAS)

# 2. Transformação
if dados_consolidados is not None:
    dados_finais = transformar_dados(dados_consolidados)

    # 3. ETAPA DE DEBUG ANTES DA CARGA
    if dados_finais is not None:
        print("\n--- RAIO-X DO DATAFRAME FINAL ANTES DA CARGA ---")
        dados_finais.info()

        # Lista de colunas que DEVEM ser numéricas
        colunas_para_verificar = ['MES_OCORRENCIA', 'ANO_ESTATISTICA', 'LATITUDE', 'LONGITUDE']
        encontrar_valores_nao_numericos(dados_finais, colunas_para_verificar)

        print("\n--- FIM DO RAIO-X ---")

        # 4. Carga para o BigQuery
        NOME_DO_PROJETO = "projetointegrador4-473718"
        ID_DA_TABELA = "dados_ssp.dados_2025"
        carregar_dados_bigquery(dados_finais, NOME_DO_PROJETO, ID_DA_TABELA, schema_definido)
