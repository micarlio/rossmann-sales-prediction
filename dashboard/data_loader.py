import pandas as pd
import numpy as np
from pathlib import Path

def carregar_dados():
    """
    Carrega todos os datasets necessários (processados e brutos), realiza a engenharia
    de features inicial e retorna os DataFrames prontos para uso no dashboard.
    """
    # --- Define o diretório base do projeto para caminhos relativos ---
    DIRETORIO_BASE = Path(__file__).resolve().parent
    DIRETORIO_DADOS = DIRETORIO_BASE.parent / "dataset"

    # Modificando para usar os datasets reduzidos
    CAMINHO_ARQUIVO_TREINO = DIRETORIO_DADOS / "reduzidos/train_reduzido.csv"
    CAMINHO_ARQUIVO_LOJAS = DIRETORIO_DADOS / "reduzidos/store_reduzido.csv"
    CAMINHO_DF_COMPLETO = DIRETORIO_DADOS / "processados/df_completo_reduzido.csv"

    # --- Dicionário para armazenar os dados carregados ---
    dados = {
        "df_principal": pd.DataFrame(),
        "df_vendas_original": pd.DataFrame(),
        "df_lojas_original": pd.DataFrame(),
        "distancia_max_global": 0,
        "contagem_vendas_antes": 0,
        "media_vendas_antes": 0,
        "contagem_vendas_depois": 0,
        "media_vendas_depois": 0,
        "df_vendas_antes_preprocessamento": pd.DataFrame(),
        "df_vendas_depois_preprocessamento": pd.DataFrame(),
        "df_lojas_tratado": pd.DataFrame() # Adicionado aqui para consistência
    }

    # --- Carregamento do Dataset Principal (Processado) ---
    try:
        df_principal = pd.read_csv(CAMINHO_DF_COMPLETO, dtype={'StateHoliday': str})
        df_principal['Date'] = pd.to_datetime(df_principal['Date'])
        # Engenharia de features para filtros e gráficos
        df_principal['Year'] = df_principal['Date'].dt.year
        df_principal['Month'] = df_principal['Date'].dt.month
        df_principal['Day'] = df_principal['Date'].dt.day
        df_principal['DayOfWeek'] = df_principal['Date'].dt.dayofweek + 1 # +1 para ser 1 (Seg) a 7 (Dom)
        df_principal['WeekOfYear'] = df_principal['Date'].dt.isocalendar().week.astype(int)

        # Cálculo de SalesPerCustomer (OTIMIZADO COM NUMPY)
        df_principal['SalesPerCustomer'] = np.where(df_principal['Customers'] > 0, df_principal['Sales'] / df_principal['Customers'], 0)

        print(f"Arquivo df_completo_reduzido.csv carregado com sucesso de: {CAMINHO_DF_COMPLETO}")

        dados["df_principal"] = df_principal
        dados["distancia_max_global"] = df_principal['CompetitionDistance'].max()
        dados["contagem_vendas_depois"] = df_principal['Sales'].count() if not df_principal.empty else 0
        dados["media_vendas_depois"] = df_principal['Sales'].mean() if not df_principal.empty else 0

    except FileNotFoundError:
        print(f"ERRO: O arquivo 'df_completo_reduzido.csv' NÃO foi encontrado em '{CAMINHO_DF_COMPLETO}'.")
        print("Verifique se o caminho do arquivo está correto e se a estrutura de pastas corresponde à esperada.")
    except Exception as e:
        print(f"ERRO ao carregar ou processar o arquivo df_completo_reduzido.csv: {e}")

    # --- Carregamento dos Datasets Brutos (para a página de Análise Preliminar) ---
    try:
        df_vendas_original = pd.read_csv(CAMINHO_ARQUIVO_TREINO, dtype={'StateHoliday': str})
        dados['df_vendas_original'] = df_vendas_original
        df_lojas_bruto = pd.read_csv(CAMINHO_ARQUIVO_LOJAS)

        # Guardar uma cópia do original para o gráfico "antes"
        dados["df_lojas_original"] = df_lojas_bruto.copy()

        # Criar uma cópia para tratamento
        df_lojas_tratado = df_lojas_bruto.copy()

        # Cálculo dinâmico das estatísticas 'Antes da Limpeza'
        dados["contagem_vendas_antes"] = len(df_vendas_original)
        dados["media_vendas_antes"] = df_vendas_original['Sales'].mean()

        # Prepara dados para os histogramas comparativos de VENDAS
        dados["df_vendas_antes_preprocessamento"] = df_vendas_original.copy()
        dados["df_vendas_depois_preprocessamento"] = df_vendas_original[df_vendas_original['Open'] == 1].copy()

        # Prepara dados para os histogramas comparativos de LOJAS
        # Preenchemos valores ausentes no dataframe TRATADO, seguindo a lógica do notebook

        # 1. Demais colunas com NaN: Preencher com 0 ou "Nenhum" (conforme notebook)
        # Primeiro, tratamos PromoInterval para evitar tipos mistos.
        if 'PromoInterval' in df_lojas_tratado.columns:
            df_lojas_tratado['PromoInterval'] = df_lojas_tratado['PromoInterval'].fillna("Nenhum")

        colunas_preencher_zero = ['CompetitionOpenSinceMonth', 'CompetitionOpenSinceYear', 'Promo2SinceWeek', 'Promo2SinceYear']
        for col in colunas_preencher_zero:
            if col in df_lojas_tratado.columns:
                df_lojas_tratado[col] = df_lojas_tratado[col].fillna(0)

        # 2. CompetitionDistance: Preencher com a MÉDIA (depois dos outros, como no notebook)
        if 'CompetitionDistance' in df_lojas_tratado.columns:
            df_lojas_tratado['CompetitionDistance'] = df_lojas_tratado['CompetitionDistance'].fillna(df_lojas_tratado['CompetitionDistance'].mean())

        dados["df_lojas_tratado"] = df_lojas_tratado

        print(f"Arquivos train_reduzido.csv e store_reduzido.csv carregados com sucesso.")

    except FileNotFoundError:
        print(f"AVISO: Arquivos reduzidos (train_reduzido.csv ou store_reduzido.csv) NÃO encontrados. Comparativos de histogramas serão limitados.")
    except Exception as e:
        print(f"AVISO: ERRO ao carregar ou processar arquivos reduzidos: {e}.")

    return dados