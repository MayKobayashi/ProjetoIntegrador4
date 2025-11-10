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
    lê APENAS as abas desejadas e consolida em um único DataFrame.
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

            # Lê todas as abas do arquivo Excel para um dicionário de DataFrames
            dicionario_de_abas = pd.read_excel(caminho_completo, sheet_name=None)

            # Itera sobre cada aba (DataFrame) lida do arquivo
            for nome_aba, df_aba in dicionario_de_abas.items():

                # Verifica se o nome da aba (removendo espaços) começa com o prefixo desejado
                if nome_aba.strip().startswith('PRESOS E APREENDIDOS'):
                    print(f" -> Processando aba: '{nome_aba}' (Corresponde ao filtro)")
                    lista_dfs.append(df_aba)
                else:
                    # Aba ignorada pois não corresponde ao filtro
                    print(f" -> Ignorando aba: '{nome_aba}'")

    if not lista_dfs:
        print("Nenhuma planilha correspondente ao filtro foi lida.")
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
    if 'NOME_DELEGACIA' in df.columns and 'NOME_MUNICIPIO' in df.columns:
        delegacias_desejadas = ['DDM SOROCABA', 'DDM VOTORANTIM']
        municipios_desejados = ['SOROCABA', 'VOTORANTIM']
        df_filtrado = df[
            (df['NOME_DELEGACIA'].str.upper().isin(delegacias_desejadas)) &
            (df['NOME_MUNICIPIO'].str.upper().isin(municipios_desejados))
        ].copy()
        print(f"\nDados filtrados. {len(df_filtrado)} registros encontrados.")
    else:
        print("Aviso: Colunas 'NOME_DELEGACIA' ou 'NOME_MUNICIPIO' não encontradas. Pulando filtragem.")
        df_filtrado = df.copy()


    # --- LIMPEZA E TRANSFORMAÇÃO ---
    if 'DATA_OCORRENCIA_BO' in df_filtrado.columns:
        df_filtrado['DATA_OCORRENCIA_BO'] = pd.to_datetime(df_filtrado['DATA_OCORRENCIA_BO'], format='%d/%m/%Y', errors='coerce')
        df_filtrado.dropna(subset=['DATA_OCORRENCIA_BO'], inplace=True)

        df_filtrado['MES_OCORRENCIA'] = df_filtrado['DATA_OCORRENCIA_BO'].dt.month
        df_filtrado['ano_ocorrencia'] = df_filtrado['DATA_OCORRENCIA_BO'].dt.year

        df_filtrado['DIA_SEMANA_EN'] = df_filtrado['DATA_OCORRENCIA_BO'].dt.day_name()
        mapa_dias = {
            'Monday': 'Segunda-feira', 'Tuesday': 'Terça-feira', 'Wednesday': 'Quarta-feira',
            'Thursday': 'Quinta-feira', 'Friday': 'Sexta-feira', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
        }
        df_filtrado['DIA_SEMANA'] = df_filtrado['DIA_SEMANA_EN'].map(mapa_dias)
        df_filtrado.drop(columns=['DIA_SEMANA_EN'], inplace=True)
    else:
        print("Aviso: Coluna 'DATA_OCORRENCIA_BO' não encontrada. Cálculos de data serão pulados.")


    colunas_para_preencher = ['DESCR_PERIODO', 'BAIRRO', 'LOGRADOURO', 'DESCR_PROFISSAO', 'DESCR_GRAU_INSTRUCAO']
    for coluna in colunas_para_preencher:

        # Verifica se a coluna realmente existe no DataFrame antes de tentar modificá-la
        if coluna in df_filtrado.columns:
            df_filtrado[coluna] = df_filtrado[coluna].fillna('Não Informado')
        else:
            # Apenas avisa que a coluna do script não foi encontrada
            print(f"Aviso: Coluna '{coluna}' não encontrada. Ignorando.")

    # --- RENOMEAR COLUNAS ---
    mapa_renomear = {
        'NUM_BO': 'codigo_bo',
        'NOME_MUNICIPIO': 'nome_municipio',
        'NOME_DELEGACIA': 'nome_delegacia',
        'MES_OCORRENCIA': 'mes_ocorrencia',
        'DATA_OCORRENCIA_BO': 'data_ocorrencia_bo',
        'HORA_OCORRENCIA_BO': 'hora_ocorrencia_bo',
        'DESCR_PERIODO': 'periodo_ocorrencia',
        'DIA_SEMANA': 'dia_semana',
        'DESCR_SUBTIPOLOCAL': 'local_ocorrencia',
        'BAIRRO': 'bairro',
        'LOGRADOURO': 'logradouro',
        'LATITUDE': 'latitude',
        'LONGITUDE': 'longitude',
        'NATUREZA_APURADA': 'tipo_ocorrencia',
        'FLAG_FLAGRANTE': 'flagrante',
        'DESCR_TIPO_PESSOA': 'natureza_autor',
        'SEXO_PESSOA': 'sexo_autor',
        'IDADE_PESSOA': 'idade_autor',
        'COR_CURTIS': 'raca_autor',
        'DESCR_PROFISSAO': 'profissao_autor',
        'DESCR_GRAU_INSTRUCAO': 'escolaridade_autor'
    }

    # Filtra o mapa de renomeação para incluir apenas colunas que REALMENTE existem
    mapa_renomear_valido = {k: v for k, v in mapa_renomear.items() if k in df_filtrado.columns}
    print(f"\nColunas renomeadas: {list(mapa_renomear_valido.keys())}")
    df_renomeado = df_filtrado.rename(columns=mapa_renomear_valido)

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
    colunas_inteiras = ['ano_ocorrencia', 'mes_ocorrencia', 'idade_autor']
    for coluna in colunas_inteiras:
        if coluna in df_renomeado.columns:
            df_renomeado[coluna] = pd.to_numeric(df_renomeado[coluna], errors='coerce')
            df_renomeado[coluna] = df_renomeado[coluna].astype(pd.Int64Dtype())
        else:
             print(f"Aviso: Coluna de inteiro '{coluna}' não encontrada para conversão de tipo.")

    # Converte colunas que devem ser de ponto flutuante (float)
    for coluna in ['latitude', 'longitude']:
        if coluna in df_renomeado.columns:
            # Substitui vírgula por ponto
            df_renomeado[coluna] = df_renomeado[coluna].astype(str).str.replace(',', '.', regex=False)
            # Converte para numérico
            df_renomeado[coluna] = pd.to_numeric(df_renomeado[coluna], errors='coerce')

    if 'hora_ocorrencia_bo' in df_renomeado.columns:
        print("Convertendo 'hora_ocorrencia_bo' para objetos 'time'...")
        horarios_dt = pd.to_datetime(df_renomeado['hora_ocorrencia_bo'], errors='coerce')

        df_renomeado['hora_ocorrencia_bo'] = horarios_dt.dt.time
        print("Conversão de hora concluída.")

    # --- ORDENAR E SELECIONAR COLUNAS FINAIS ---
    ordem_final_colunas = [
        'codigo_bo',
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
        'tipo_ocorrencia',
        'flagrante',
        'natureza_autor',
        'sexo_autor',
        'idade_autor',
        'raca_autor',
        'profissao_autor',
        'escolaridade_autor'
    ]

    # Filtra para garantir que apenas colunas existentes sejam selecionadas
    colunas_existentes = [col for col in ordem_final_colunas if col in df_renomeado.columns]
    print(f"\nColunas finais que serão carregadas: {colunas_existentes}")
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

    # Filtra o schema para carregar apenas as colunas que existem no DF final
    nomes_colunas_df = list(df.columns)
    schema_filtrado = [campo for campo in schema if campo.name in nomes_colunas_df]
    print(f"Schema filtrado para {len(schema_filtrado)} colunas existentes no DataFrame.")

    # Usamos o schema que definimos, em vez de autodetect
    job_config = bigquery.LoadJobConfig(
        schema=schema_filtrado, # Usa o schema filtrado
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
    bigquery.SchemaField("codigo_bo", "STRING", mode="NULLABLE"),
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
    bigquery.SchemaField("tipo_ocorrencia", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("flagrante", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("natureza_autor", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("sexo_autor", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("idade_autor", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("raca_autor", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("profissao_autor", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("escolaridade_autor", "STRING", mode="NULLABLE")
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
    'https://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados/DadosProdutividade_2024.xlsx',
    'https://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados/DadosProdutividade_2025.xlsx'
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
        ID_DA_TABELA = "dados_ssp.dados_produtividade"
        carregar_dados_bigquery(dados_finais, NOME_DO_PROJETO, ID_DA_TABELA, schema_definido)
