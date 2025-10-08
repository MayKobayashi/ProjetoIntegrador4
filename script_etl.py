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
    lê todas as abas e consolida em um único DataFrame.
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
            print(f"Lendo todas as abas do arquivo: {arquivo}")

            # Lê todas as abas do arquivo Excel para um dicionário de DataFrames
            dicionario_de_abas = pd.read_excel(caminho_completo, sheet_name=None)

            # Itera sobre cada aba (DataFrame) lida do arquivo
            for nome_aba, df_aba in dicionario_de_abas.items():
                print(f" -> Processando aba: '{nome_aba}'")
                lista_dfs.append(df_aba)

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

    colunas_para_preencher = ['DESC_PERIODO', 'BAIRRO', 'LOGRADOURO']
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

    # --- RENOMEAR COLUNAS ---
    mapa_renomear = {
        'NOME_MUNICIPIO': 'nome_municipio',
        'NOME_DELEGACIA': 'nome_delegacia',
        'ANO_ESTATISTICA': 'ano_ocorrencia',
        'MES_OCORRENCIA': 'mes_ocorrencia',
        'DATA_OCORRENCIA_BO': 'data_ocorrencia_bo',
        'HORA_OCORRENCIA_BO': 'hora_ocorrencia_bo',
        'DESC_PERIODO': 'periodo_ocorrencia',
        'DIA_SEMANA': 'dia_semana',
        'DESCR_SUBTIPOLOCAL': 'local_ocorrencia',
        'BAIRRO': 'bairro',
        'LOGRADOURO': 'logradouro',
        'LATITUDE': 'latitude',
        'LONGITUDE': 'longitude',
        'RUBRICA': 'artigo_ocorrencia',
        'NATUREZA_APURADA': 'tipo_ocorrencia'
    }
    df_renomeado = df_filtrado.rename(columns=mapa_renomear)

    # --- FORMATAR TEXTOS PARA "Title Case" ---
    colunas_texto = df_renomeado.select_dtypes(include=['object']).columns
    for coluna in colunas_texto:
        # Pula a coluna de hora para não convertê-la em texto
        if coluna == 'hora_ocorrencia_bo':
            continue

        df_renomeado[coluna] = df_renomeado[coluna].astype(str).str.title()
        df_renomeado[coluna] = df_renomeado[coluna].str.replace('Nao Informado', 'Não Informado')
        df_renomeado[coluna] = df_renomeado[coluna].str.replace('Nan', 'Não Informado')

    # --- BLOCO FINAL DE GARANTIA DOS TIPOS ---
    print("\nGarantindo os tipos de dados corretos antes da carga...")

    # Converte colunas que devem ser inteiros
    for coluna in ['ano_ocorrencia', 'mes_ocorrencia']:
        if coluna in df_renomeado.columns:
            df_renomeado[coluna] = pd.to_numeric(df_renomeado[coluna], errors='coerce')
            df_renomeado.dropna(subset=[coluna], inplace=True)
            df_renomeado[coluna] = df_renomeado[coluna].astype(int)

    # Converte colunas que devem ser de ponto flutuante (float)
    for coluna in ['latitude', 'longitude']:
        if coluna in df_renomeado.columns:
            # Substitui vírgula por ponto
            df_renomeado[coluna] = df_renomeado[coluna].astype(str).str.replace(',', '.', regex=False)
            # Converte para numérico
            df_renomeado[coluna] = pd.to_numeric(df_renomeado[coluna], errors='coerce')

    # --- ORDENAR E SELECIONAR COLUNAS FINAIS ---
    ordem_final_colunas = [
        'nome_municipio',
        'nome_delegacia',
        'ano_ocorrencia',
        'mes_ocorrencia',
        'data_ocorrencia_bo',
        'hora_ocorrencia_bo',
        'periodo_ocorrencia',
        'dia_semana',
        'local_ocorrencia',
        'bairro',
        'logradouro',
        'latitude',
        'longitude',
        'artigo_ocorrencia',
        'tipo_ocorrencia',
    ]
    # Filtra para garantir que apenas colunas existentes sejam selecionadas
    colunas_existentes = [col for col in ordem_final_colunas if col in df_renomeado.columns]
    df_transformado = df_renomeado[colunas_existentes]

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

    # Usamos o schema que definimos, em vez de autodetect
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
    bigquery.SchemaField("nome_municipio", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("nome_delegacia", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ano_ocorrencia", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("mes_ocorrencia", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("data_ocorrencia_bo", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("hora_ocorrencia_bo", "TIME", mode="NULLABLE"),
    bigquery.SchemaField("periodo_ocorrencia", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("dia_semana", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("local_ocorrencia", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("bairro", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("logradouro", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("latitude", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("longitude", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("artigo_ocorrencia", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("tipo_ocorrencia", "STRING", mode="NULLABLE"),
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
    if dados_finais is not None and not dados_finais.empty:
        print("\n--- RAIO-X DO DATAFRAME FINAL ANTES DA CARGA ---")
        dados_finais.info()

        # Lista de colunas que DEVEM ser numéricas
        colunas_para_verificar = ['mes_ocorrencia', 'ano_ocorrencia', 'latitude', 'longitude']
        encontrar_valores_nao_numericos(dados_finais, colunas_para_verificar)

        print("\n--- FIM DO RAIO-X ---")

        # 4. Carga para o BigQuery
        NOME_DO_PROJETO = "projetointegrador4-473718"
        ID_DA_TABELA = "dados_ssp.dados_2025"
        carregar_dados_bigquery(dados_finais, NOME_DO_PROJETO, ID_DA_TABELA, schema_definido)
