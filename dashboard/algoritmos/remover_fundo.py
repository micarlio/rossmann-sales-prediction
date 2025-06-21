from PIL import Image
import numpy as np

def remover_fundo_e_colorir_forma(caminho_entrada, caminho_saida, cor_desejada, tolerancia=10):
    """
    Remove o fundo de uma imagem PNG (assumindo que o fundo é a cor do pixel superior esquerdo)
    e colore a forma restante com uma cor desejada, tornando o fundo transparente.
    """
    # Abre a imagem e converte para RGBA
    imagem = Image.open(caminho_entrada).convert("RGBA")
    dados_pixel = np.array(imagem)

    # Considera o pixel do canto superior esquerdo como cor de fundo
    cor_fundo = dados_pixel[0, 0, :3]

    # Calcula a diferença entre cada pixel e a cor de fundo
    diferenca = np.abs(dados_pixel[:, :, :3] - cor_fundo)
    mascara = np.any(diferenca > tolerancia, axis=-1)

    # Cria nova imagem: pixels diferentes do fundo ficam na cor desejada; fundo fica transparente
    nova_imagem = np.zeros_like(dados_pixel)
    nova_imagem[:, :, 3] = 0  # Transparência total

    # Converte a cor desejada para RGB e adiciona alpha
    cor_rgba = list(cor_desejada) + [255]  # Adiciona alpha máximo

    # Define os pixels da forma com a cor desejada
    nova_imagem[mascara] = cor_rgba

    # Salva a nova imagem com fundo transparente
    resultado = Image.fromarray(nova_imagem)
    resultado.save(caminho_saida, format="PNG")
    print(f"Imagem processada e salva em: {caminho_saida}")

# Importa a cor do config (assumindo que config.py foi refatorado)
from ..config import AZUL_ESCURO as DARK_BLUE_RGB

# Cores do layout (usando a constante importada)
AZUL_ESCURO_RGB = DARK_BLUE_RGB # Renomeado para clareza local, mas o valor vem do config

# Caminhos das imagens
caminho_entrada = '/Users/micarloteixeira/Desktop/Data Science/projects/Rossman Store/dashboard/assets/images/rossmann_logo2.png'
caminho_saida = '/Users/micarloteixeira/Desktop/Data Science/projects/Rossman Store/dashboard/assets/images/rossmann_logo_colorida.png'

# Processa a imagem
remover_fundo_e_colorir_forma(caminho_entrada, caminho_saida, AZUL_ESCURO_RGB)