import pandas as pd
import numpy as np
import logging
import os
import sys
from pathlib import Path

# Obtém o diretório atual
DIRETORIO_ATUAL = Path(__file__).resolve().parent

# Importa a função reduzir_dataset.main() e a constante N_AMOSTRAS
# Adiciona o diretório dataset ao path para garantir que a importação funcione
sys.path.append(str(DIRETORIO_ATUAL))
try:
    from reduzir_dataset import main as reduzir_datasets, N_AMOSTRAS
except ImportError:
    # Fallback caso não consiga importar diretamente
    import importlib.util
    spec = importlib.util.spec_from_file_location("reduzir_dataset", os.path.join(DIRETORIO_ATUAL, "reduzir_dataset.py"))
    reduzir_dataset = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(reduzir_dataset)
    reduzir_datasets = reduzir_dataset.main
    N_AMOSTRAS = reduzir_dataset.N_AMOSTRAS

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(DIRETORIO_ATUAL, 'gerar_df_completo.log'))
    ]
)

def verificar_diretorio(caminho):
    """Verifica se o diretório existe, se não, cria."""
    diretorio = os.path.dirname(caminho)
    if diretorio and not os.path.exists(diretorio):
        os.makedirs(diretorio)
        logging.info(f"Diretório criado: {diretorio}")

def processar_datasets():
    try:
        # Define caminhos absolutos para os arquivos
        caminho_train_reduzido = os.path.join(DIRETORIO_ATUAL, "reduzidos", "train_reduzido.csv")
        caminho_store_reduzido = os.path.join(DIRETORIO_ATUAL, "reduzidos", "store_reduzido.csv")
        caminho_df_completo = os.path.join(DIRETORIO_ATUAL, "processados", "df_completo_reduzido.csv")
        
        # Verifica se os diretórios existem
        verificar_diretorio(caminho_df_completo)
        
        # Reduzir datasets usando a função do módulo reduzir_dataset
        logging.info(f"Iniciando redução dos datasets com {N_AMOSTRAS} amostras por loja...")
        df_vendas, df_lojas = reduzir_datasets()
        
        if df_vendas is None or df_lojas is None:
            logging.error("Falha ao reduzir os datasets. Verifique os logs para mais detalhes.")
            return False
        
        logging.info("\nTratando dados...")
        # Convertendo a coluna Date para datetime
        df_vendas['Date'] = pd.to_datetime(df_vendas['Date'])

        # Tratamento do dataset de lojas
        # Preenchendo valores ausentes
        df_lojas['PromoInterval'] = df_lojas['PromoInterval'].fillna("Nenhum") 

        colunas_preencher_zero = ['CompetitionOpenSinceMonth', 'CompetitionOpenSinceYear', 'Promo2SinceWeek', 'Promo2SinceYear']
        for col in colunas_preencher_zero:
            df_lojas[col] = df_lojas[col].fillna(0)

        # CompetitionDistance: Preencher com a MÉDIA
        df_lojas['CompetitionDistance'] = df_lojas['CompetitionDistance'].fillna(df_lojas['CompetitionDistance'].mean())

        logging.info("\nRealizando merge dos datasets...")
        # Merge dos dataframes
        df_completo = pd.merge(df_vendas, df_lojas, on='Store', how='left')

        # Não precisamos mais filtrar lojas abertas, pois isso já foi feito na amostragem
        # Removemos a coluna Open que não é mais necessária
        if 'Open' in df_completo.columns:
            df_completo.drop(['Open'], axis=1, inplace=True)
            logging.info("Coluna 'Open' removida (filtro já aplicado na amostragem)")
        
        logging.info("\nSalvando novo df_completo...")
        # Salvando o novo df_completo
        df_completo.to_csv(caminho_df_completo, index=False)

        logging.info("\nEstatísticas do novo df_completo:")
        logging.info(f"Total de registros: {len(df_completo)}")
        logging.info(f"Número de lojas únicas: {df_completo['Store'].nunique()}")
        logging.info(f"Período: de {df_completo['Date'].min().strftime('%d/%m/%Y')} até {df_completo['Date'].max().strftime('%d/%m/%Y')}")

        # Verificando número de registros por loja
        registros_por_loja = df_completo['Store'].value_counts().sort_index()
        logging.info("\nDistribuição de registros por loja (primeiras 5 lojas):")
        logging.info(registros_por_loja.head())

        # Verificando lojas com menos registros que o solicitado
        lojas_com_menos_registros = registros_por_loja[registros_por_loja < N_AMOSTRAS]
        if not lojas_com_menos_registros.empty:
            logging.info(f"\nLojas com menos registros que os {N_AMOSTRAS} solicitados:")
            logging.info(f"Total de lojas com menos registros: {len(lojas_com_menos_registros)}")
            logging.info(f"Média de registros nestas lojas: {lojas_com_menos_registros.mean():.2f}")
            
        return True
    
    except Exception as e:
        logging.error(f"Erro ao processar e gerar o dataset completo: {str(e)}")
        return False

if __name__ == "__main__":
    # Processar os datasets
    sucesso = processar_datasets()
    
    if sucesso:
        logging.info("Processamento concluído com sucesso!")
    else:
        logging.error("Processamento falhou. Verifique os logs para mais detalhes.") 