import dash_bootstrap_components as dbc
from dash import dcc, html
import plotly.express as px
import pandas as pd

from .componentes_compartilhados import criar_botoes_cabecalho # Refatorar nome do módulo e da função
from ..config import CINZA_NEUTRO, VERMELHO_ROSSMANN # Importar novas constantes

def criar_layout_limpeza_dados(dados): # Refatorar nome da função e parâmetro
    nome_pagina = "limpeza" # Refatorar nome da variável
    media_vendas_antes = dados["media_vendas_antes"] # Refatorar nome da variável
    media_vendas_depois = dados["media_vendas_depois"] # Refatorar nome da variável
    contagem_vendas_antes = dados["contagem_vendas_antes"] # Refatorar nome da variável
    contagem_vendas_depois = dados["contagem_vendas_depois"] # Refatorar nome da variável

    df_limpeza_media = pd.DataFrame({'Estado do Dataset': ['Antes da Limpeza', 'Após Limpeza (Open==1)'], 'Vendas Médias (€)': [media_vendas_antes, media_vendas_depois]}) # Refatorar nome da variável
    df_limpeza_contagem = pd.DataFrame({'Estado do Dataset': ['Antes da Limpeza', 'Após Limpeza (Open==1)'], 'Número de Registros': [contagem_vendas_antes, contagem_vendas_depois]}) # Refatorar nome da variável

    fig_limpeza_media = px.bar(df_limpeza_media, x='Estado do Dataset', y='Vendas Médias (€)', title="Impacto da Limpeza: Média de Vendas", text_auto='.2f', color='Estado do Dataset', color_discrete_map={'Antes da Limpeza': CINZA_NEUTRO, 'Após Limpeza (Open==1)': VERMELHO_ROSSMANN}) # Refatorar nome da variável e constantes
    fig_limpeza_contagem = px.bar(df_limpeza_contagem, x='Estado do Dataset', y='Número de Registros', title="Impacto da Limpeza: Contagem de Registros", text_auto=',', color='Estado do Dataset', color_discrete_map={'Antes da Limpeza': CINZA_NEUTRO, 'Após Limpeza (Open==1)': VERMELHO_ROSSMANN}) # Refatorar nome da variável e constantes

    registros_removidos = contagem_vendas_antes - contagem_vendas_depois # Refatorar nome da variável
    registros_originais = contagem_vendas_antes # Refatorar nome da variável

    return html.Div([
        html.Div([
            html.H1("Processo de Limpeza e Pré-processamento", className="page-title"),
            criar_botoes_cabecalho(nome_pagina) # Usar nova função e variável refatorada
        ], className="d-flex justify-content-between align-items-center mb-4"),
        dbc.Card([
            dbc.CardBody([
                dcc.Markdown(f"""
                    A primeira etapa do projeto envolveu a limpeza e a preparação dos dados brutos. As ações mais significativas foram:
                    * **União dos Datasets:** Foi realizada a junção dos dados de vendas (`train.csv`) com as informações detalhadas sobre cada loja (`store.csv`) utilizando o `Store ID` como chave comum.
                    * **Tratamento de Lojas Fechadas:** A decisão mais impactante foi a remoção dos {registros_removidos:,.0f} registros diários onde as lojas estavam fechadas (`Open == 0`), de um total de {registros_originais:,.0f} registros originais. Isso garante que a análise se concentre apenas nos dias de operação efetiva.
                    * **Tratamento de Dados Faltantes em `store_df`:** Preenchemos valores ausentes (`NaN`) em colunas como `CompetitionDistance` (com a média da coluna) e em campos relacionados a `Promo2` e `CompetitionOpenSince` (com 0, indicando "não aplicável" ou "desconhecido" para facilitar a modelagem futura).
                """, className="mb-4"),
                dbc.Row([
                    dbc.Col(dcc.Graph(figure=fig_limpeza_media), md=6), # Usar nova variável refatorada
                    dbc.Col(dcc.Graph(figure=fig_limpeza_contagem), md=6) # Usar nova variável refatorada
                ], className="g-4")
            ])
        ], className="custom-card")
    ])