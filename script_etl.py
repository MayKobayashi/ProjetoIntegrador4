import pandas as pd


# =============================================
# EXTRAÇÃO
# =============================================


def extrair_dados(caminho_do_arquivo):
    """
    Função para extrair dados de um arquivo CSV.
    """
    try:
        df = pd.read_csv(caminho_do_arquivo)
        print("Dados extraídos com sucesso!")
        return df
    except FileNotFoundError:
        print(f"Erro: O arquivo '{caminho_do_arquivo}' não foi encontrado.")
        return None


# =============================================
# TRANSFORMAÇÃO
# =============================================


def transformar_dados(df):
    """
    Função para filtrar, limpar e transformar o DataFrame, com correção no parsing de datas.
    """
    if df is None:
        return None

    # --- ETAPA 1: FILTRAGEM ---
    delegacias_desejadas = ['DDM SOROCABA', 'DDM VOTORANTIM']
    municipios_desejados = ['SOROCABA', 'VOTORANTIM']

    df_filtrado = df[
        df['NOME_DELEGACIA'].str.upper().isin(delegacias_desejadas) &
        df['NOME_MUNICIPIO'].str.upper().isin(municipios_desejados)
    ].copy()

    print(f"\nDados filtrados. {len(df_filtrado)} registros encontrados.")


    # --- ETAPA 2: LIMPEZA E TRANSFORMAÇÃO ---

    # 1. Converte a coluna de data para o formato datetime
    df_filtrado['DATA_OCORRENCIA_BO'] = pd.to_datetime(df_filtrado['DATA_OCORRENCIA_BO'], format='%d/%m/%Y', errors='coerce')

    # 2. Remove registros onde a data não pôde ser convertida
    linhas_antes = len(df_filtrado)
    df_filtrado.dropna(subset=['DATA_OCORRENCIA_BO'], inplace=True)
    linhas_depois = len(df_filtrado)
    print(f"{linhas_antes - linhas_depois} registros com datas inválidas foram removidos.")


    # 3. Trata valores nulos nas outras colunas
    colunas_para_preencher = ['HORA_OCORRENCIA_BO', 'DESC_PERIODO', 'DESCR_CONDUTA', 'BAIRRO', 'LOGRADOURO']
    for coluna in colunas_para_preencher:
        df_filtrado[coluna] = df_filtrado[coluna].fillna('Não Informado')

    # 4. Cria novas colunas para análise
    df_filtrado['MES_OCORRENCIA'] = df_filtrado['DATA_OCORRENCIA_BO'].dt.month
    df_filtrado['DIA_SEMANA_IN'] = df_filtrado['DATA_OCORRENCIA_BO'].dt.day_name()

    mapa_dias = {
        'Monday': 'Segunda-feira', 'Tuesday': 'Terça-feira', 'Wednesday': 'Quarta-feira',
        'Thursday': 'Quinta-feira', 'Friday': 'Sexta-feira', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
    }
    df_filtrado['DIA_SEMANA'] = df_filtrado['DIA_SEMANA_IN'].map(mapa_dias)
    df_filtrado = df_filtrado.drop(columns=['DIA_SEMANA_IN'])

    # 5. Seleciona e reordena as colunas finais
    colunas_relevantes = [
        'NOME_DELEGACIA', 'NOME_MUNICIPIO', 'DATA_OCORRENCIA_BO', 'HORA_OCORRENCIA_BO',
        'DESC_PERIODO', 'DIA_SEMANA', 'MES_OCORRENCIA', 'ANO_ESTATISTICA', 'DESCR_SUBTIPOLOCAL', 'BAIRRO',
        'LOGRADOURO', 'LATITUDE', 'LONGITUDE', 'RUBRICA', 'DESCR_CONDUTA', 'NATUREZA_APURADA'
    ]
    df_transformado = df_filtrado[colunas_relevantes]

    print("Dados transformados com sucesso!")
    return df_transformado

# --- ROTEIRO PRINCIPAL DO SCRIPT ---

# 1. Extração
caminho_arquivo = 'SPDadosCriminais_2025.csv'
dados_originais = extrair_dados(caminho_arquivo)

# 2. Transformação (que agora inclui a filtragem)
dados_transformados = transformar_dados(dados_originais)

# 3. Exibição do Resultado
if dados_transformados is not None:
    print("\nAmostra dos dados finais transformados:")
    print(dados_transformados.head())


# =============================================
# CARGA
# =============================================


def carregar_dados(df, caminho_destino):
    """
    Função para salvar os dados transformados em um novo arquivo CSV.
    """
    if dados_transformados is not None:
        dados_transformados.to_csv(caminho_destino, index=False, encoding='utf-8-sig')
        print(f"Dados carregados com sucesso em '{caminho_destino}'")

# Use a função
arquivo_destino = 'DadosCriminais_Tratados.csv'
carregar_dados(dados_transformados, arquivo_destino)
