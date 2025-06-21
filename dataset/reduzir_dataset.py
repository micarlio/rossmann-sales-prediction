import pandas as pd
import numpy as np
import logging
import os
import sys

# Configuração para reprodutibilidade
np.random.seed(42)

# Número fixo de amostras por loja
N_AMOSTRAS = 100

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('dataset_reducao.log')
    ]
)

# Função robusta para amostrar n linhas por loja
def amostrar_por_loja(df, n_amostras=N_AMOSTRAS, random_state=42, filtrar_abertas=True):
    """
    Amostra um número fixo de registros por loja.
    Se uma loja não possui registros suficientes, utiliza todos os registros disponíveis.
    
    Args:
        df (pd.DataFrame): DataFrame com os dados
        n_amostras (int): Número desejado de amostras por loja
        random_state (int): Seed para reprodutibilidade
        filtrar_abertas (bool): Se True, filtra apenas lojas abertas (Open=1) antes da amostragem
        
    Returns:
        pd.DataFrame: DataFrame com amostras selecionadas
    """
    if 'Store' not in df.columns:
        raise ValueError("O DataFrame deve conter a coluna 'Store'")
        
    resultado = []
    
    # Contagem de registros disponíveis por loja antes do filtro
    total_registros_por_loja = df['Store'].value_counts()
    logging.info(f"Total de lojas no dataset original: {len(total_registros_por_loja)}")
    
    # Aplicar filtro de lojas abertas se solicitado
    if filtrar_abertas and 'Open' in df.columns:
        df_filtrado = df[df['Open'] == 1].copy()
        registros_filtrados = len(df) - len(df_filtrado)
        logging.info(f"Removidos {registros_filtrados} registros de lojas fechadas ({registros_filtrados/len(df)*100:.2f}%)")
        df = df_filtrado
    
    # Contagem de registros disponíveis por loja após o filtro
    registros_por_loja_apos_filtro = df['Store'].value_counts()
    logging.info(f"Total de lojas após filtro: {len(registros_por_loja_apos_filtro)}")
    
    # Lojas com poucos registros
    lojas_com_poucos_registros = registros_por_loja_apos_filtro[registros_por_loja_apos_filtro < n_amostras]
    if not lojas_com_poucos_registros.empty:
        logging.warning(f"{len(lojas_com_poucos_registros)} lojas têm menos de {n_amostras} registros disponíveis após filtros")
        logging.warning(f"Média de registros nestas lojas: {lojas_com_poucos_registros.mean():.2f}")
    
    for loja, grupo in df.groupby('Store'):
        if len(grupo) >= n_amostras:
            # Se tiver registros suficientes, amostra a quantidade desejada
            amostra = grupo.sample(n=n_amostras, random_state=random_state)
        else:
            # Se não tiver registros suficientes, usa todos os disponíveis
            amostra = grupo
            logging.warning(f"Loja {loja} possui apenas {len(grupo)} registros após filtros, menos que os {n_amostras} solicitados.")
        
        resultado.append(amostra)
    
    return pd.concat(resultado, ignore_index=True)

def verificar_diretorio(caminho):
    """Verifica se o diretório existe, se não, cria."""
    diretorio = os.path.dirname(caminho)
    if diretorio and not os.path.exists(diretorio):
        os.makedirs(diretorio)
        logging.info(f"Diretório criado: {diretorio}")

def main():
    try:
        # Carregando os datasets
        logging.info("Carregando datasets originais...")
        
        # Verificar se os arquivos existem
        arquivos_necessarios = ['brutos/store.csv', 'brutos/train.csv']
        for arquivo in arquivos_necessarios:
            if not os.path.exists(arquivo):
                logging.error(f"Arquivo não encontrado: {arquivo}")
                return
        
        df_store = pd.read_csv('brutos/store.csv')
        df_train = pd.read_csv('brutos/train.csv', low_memory=False)
        
        # Amostrando dados do dataset de vendas, filtrando apenas lojas abertas
        logging.info(f"Amostrando dados com {N_AMOSTRAS} registros por loja (apenas lojas abertas)...")
        df_train_reduzido = amostrar_por_loja(df_train, N_AMOSTRAS, filtrar_abertas=True)
        
        # Pegando apenas as lojas que estão no dataset de vendas reduzido
        lojas_selecionadas = df_train_reduzido['Store'].unique()
        df_store_reduzido = df_store[df_store['Store'].isin(lojas_selecionadas)]
        
        # Verificando e criando diretórios se necessário
        verificar_diretorio('reduzidos/store_reduzido.csv')
        
        # Salvando os datasets reduzidos
        logging.info("Salvando datasets reduzidos...")
        df_store_reduzido.to_csv('reduzidos/store_reduzido.csv', index=False)
        df_train_reduzido.to_csv('reduzidos/train_reduzido.csv', index=False)
        
        # Imprimindo estatísticas
        logging.info("\nEstatísticas dos datasets:")
        logging.info(f"Dataset de lojas original: {len(df_store)} registros")
        logging.info(f"Dataset de lojas reduzido: {len(df_store_reduzido)} registros")
        logging.info(f"Dataset de vendas original: {len(df_train)} registros")
        logging.info(f"Dataset de vendas reduzido: {len(df_train_reduzido)} registros")
        
        # Verificando número de registros por loja no dataset reduzido
        registros_por_loja = df_train_reduzido['Store'].value_counts().sort_index()
        logging.info("\nNúmero de registros por loja (primeiras 5 lojas):")
        logging.info(registros_por_loja.head())
        
        # Verificando lojas com menos registros que o solicitado
        lojas_com_menos_registros = registros_por_loja[registros_por_loja < N_AMOSTRAS]
        if not lojas_com_menos_registros.empty:
            logging.info("\nLojas com menos registros que o solicitado:")
            logging.info(f"Total de lojas com menos registros: {len(lojas_com_menos_registros)}")
            logging.info(f"Média de registros nestas lojas: {lojas_com_menos_registros.mean():.2f}")
            
        return df_train_reduzido, df_store_reduzido
        
    except Exception as e:
        logging.error(f"Erro ao processar os datasets: {str(e)}")
        return None, None

# Este bloco só será executado se o script for chamado diretamente
if __name__ == "__main__":
    main() 