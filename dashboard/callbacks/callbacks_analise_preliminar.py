# dashboard/callbacks/callbacks_analise_preliminar.py
from dash import Input, Output, html
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import statsmodels.api as sm
import pandas as pd

from ..utils import criar_figura_vazia # Importar a função utilitária refatorada
from ..config import VERMELHO_ROSSMANN, CINZA_NEUTRO, AZUL_DESTAQUE # Importar as novas constantes

LAYOUT_GRAFICO_COMUM = { # Refatorar nome da constante
    'title_x': 0.5,
    'margin': dict(l=80, r=40, b=40, t=90)
}

def registrar_callbacks_analise_preliminar(aplicativo, dados): # Refatorar nome da função e parâmetro 'app' para 'aplicativo'
    df_principal = dados["df_principal"] # Usar o novo nome do DataFrame principal
    df_vendas_antes_preprocessamento = dados["df_vendas_antes_preprocessamento"] # Usar o novo nome do DataFrame
    df_vendas_depois_preprocessamento = dados["df_vendas_depois_preprocessamento"] # Usar o novo nome do DataFrame
    df_lojas_original = dados["df_lojas_original"] # Usar o novo nome do DataFrame
    df_lojas_tratado = dados["df_lojas_tratado"] # Usar o novo nome do DataFrame

    # --- Callback para Scatter Plot da Matriz de Correlação ---
    @aplicativo.callback( # Usar 'aplicativo'
        Output('grafico-dispersao-correlacao', 'figure'), # Refatorar ID
        Input('grafico-matriz-correlacao', 'clickData') # Refatorar ID
    )
    def exibir_dados_clicados(dados_clicados): # Refatorar nome da função e parâmetro
        if dados_clicados is None:
            return criar_figura_vazia("Clique em uma célula da matriz") # Usar a função refatorada

        try:
            ponto = dados_clicados['points'][0] # Refatorar nome da variável
            col_x = ponto['x'] # Refatorar nome da variável
            col_y = ponto['y'] # Refatorar nome da variável

            if col_x not in df_principal.select_dtypes(include=np.number).columns or col_y not in df_principal.select_dtypes(include=np.number).columns: # Usar df_principal
                return criar_figura_vazia(f"Não é possível plotar '{col_x}' vs '{col_y}'. Selecione colunas numéricas.") # Usar a função refatorada

            tamanho_amostra = min(len(df_principal), 5000) # Refatorar nome da variável, usar df_principal
            df_amostra = df_principal.sample(n=tamanho_amostra, random_state=42) # Refatorar nome da variável, usar df_principal

            fig = px.scatter(df_amostra, x=col_x, y=col_y, title=f'Dispersão: {col_x} vs {col_y}', color_discrete_sequence=[VERMELHO_ROSSMANN], render_mode='webgl') # Usar df_amostra e constante refatorada
            fig.update_layout(
                **LAYOUT_GRAFICO_COMUM, # Usar constante refatorada
                plot_bgcolor='white'
            )

            df_temp = df_amostra[[col_x, col_y]].dropna() # Refatorar nome da variável
            if not df_temp.empty:
                X = sm.add_constant(df_temp[col_x])
                model = sm.OLS(df_temp[col_y], X)
                results = model.fit()
                fig.add_trace(go.Scatter(x=df_temp[col_x], y=results.predict(X), mode='lines', name='Linha de Tendência', line=dict(color=AZUL_DESTAQUE, width=3))) # Usar constante refatorada

            return fig
        except Exception as e:
            return criar_figura_vazia(f"Erro ao gerar gráfico de dispersão: {e}") # Usar a função refatorada

    # --- Callback para Histograma Comparativo de Vendas ---
    @aplicativo.callback( # Usar 'aplicativo'
        Output('histograma-vendas-comparativo', 'figure'), # Refatorar ID
        Input('dropdown-histograma-vendas', 'value') # Refatorar ID
    )
    def atualizar_histograma_vendas(coluna_selecionada): # Refatorar nome da função e parâmetro
        if df_vendas_antes_preprocessamento.empty or df_vendas_depois_preprocessamento.empty or coluna_selecionada is None: # Usar os novos nomes dos DataFrames
            return criar_figura_vazia("Dados de vendas não carregados ou coluna não selecionada.") # Usar a função refatorada

        if coluna_selecionada not in df_vendas_antes_preprocessamento.columns or coluna_selecionada not in df_vendas_depois_preprocessamento.columns: # Usar os novos nomes dos DataFrames
            return criar_figura_vazia(f"Coluna '{coluna_selecionada}' não disponível para comparação de histogramas.") # Usar a função refatorada

        fig = go.Figure()
        fig.add_trace(go.Histogram(x=df_vendas_antes_preprocessamento[coluna_selecionada], name='Antes', opacity=0.7, marker_color=CINZA_NEUTRO, nbinsx=40)) # Usar o novo nome do DataFrame e constante refatorada
        fig.add_trace(go.Histogram(x=df_vendas_depois_preprocessamento[coluna_selecionada], name='Depois', opacity=0.7, marker_color=VERMELHO_ROSSMANN, nbinsx=40)) # Usar o novo nome do DataFrame e constante refatorada

        if coluna_selecionada in ['Sales', 'Customers']:
            fig.update_yaxes(type="log", title_text='Frequência (Escala Log)')
        else:
            fig.update_yaxes(title_text='Frequência')

        fig.update_layout(
            **LAYOUT_GRAFICO_COMUM, # Usar constante refatorada
            barmode='overlay',
            title=f'Distribuição de {coluna_selecionada}',
            xaxis_title=coluna_selecionada,
            legend_title_text='Pré-processamento'
        )
        return fig

    # --- Callback para Histograma de Lojas (AGORA COMPARATIVO) ---
    @aplicativo.callback( # Usar 'aplicativo'
        Output('histograma-lojas', 'figure'), # Refatorar ID
        Input('dropdown-histograma-lojas', 'value') # Refatorar ID
    )
    def atualizar_histograma_lojas(coluna_selecionada): # Refatorar nome da função e parâmetro
        if df_lojas_original.empty or df_lojas_tratado.empty or coluna_selecionada is None: # Usar os novos nomes dos DataFrames
            return criar_figura_vazia("Dados de lojas não carregados ou coluna não selecionada.") # Usar a função refatorada

        if coluna_selecionada not in df_lojas_original.columns or coluna_selecionada not in df_lojas_tratado.columns: # Usar os novos nomes dos DataFrames
            return criar_figura_vazia(f"Coluna '{coluna_selecionada}' não disponível para comparação.") # Usar a função refatorada

        fig = go.Figure()
        # Adiciona o histograma "Antes"
        fig.add_trace(go.Histogram(
            x=df_lojas_original[coluna_selecionada], # Usar o novo nome do DataFrame
            name='Antes',
            opacity=0.7,
            marker_color=CINZA_NEUTRO, # Usar constante refatorada
            nbinsx=40
        ))
        # Adiciona o histograma "Depois"
        fig.add_trace(go.Histogram(
            x=df_lojas_tratado[coluna_selecionada], # Usar o novo nome do DataFrame
            name='Depois',
            opacity=0.7,
            marker_color=VERMELHO_ROSSMANN, # Usar constante refatorada
            nbinsx=40
        ))

        # Configura o eixo Y para escala logarítmica para colunas com grande variação
        if coluna_selecionada in ['CompetitionDistance', 'CompetitionOpenSinceYear', 'Promo2SinceYear']:
            fig.update_yaxes(type="log", title_text='Frequência (Escala Log)')
        else:
            fig.update_yaxes(title_text='Frequência')

        # Configura o layout geral do gráfico
        fig.update_layout(
            **LAYOUT_GRAFICO_COMUM, # Usar constante refatorada
            barmode='overlay', # Sobrepõe as barras para comparação direta
            title_text=f'Distribuição de {coluna_selecionada}',
            xaxis_title_text=coluna_selecionada,
            legend_title_text='Tratamento de Dados'
        )
        return fig

    # --- Callbacks para os Gráficos de Estatísticas COMPARATIVAS ---
    @aplicativo.callback( # Usar 'aplicativo'
        Output('grafico-estatisticas-vendas', 'figure'), # Refatorar ID
        Input('dropdown-histograma-vendas', 'value') # Refatorar ID
    )
    def atualizar_grafico_estatisticas_vendas(coluna_selecionada): # Refatorar nome da função e parâmetro
        if coluna_selecionada is None or df_vendas_depois_preprocessamento.empty or df_vendas_antes_preprocessamento.empty: # Usar os novos nomes dos DataFrames
            return criar_figura_vazia("Selecione uma variável para ver as estatísticas.") # Usar a função refatorada

        if coluna_selecionada not in df_vendas_antes_preprocessamento.columns: # Usar o novo nome do DataFrame
            return criar_figura_vazia(f"Coluna '{coluna_selecionada}' não encontrada.") # Usar a função refatorada

        estats_antes = df_vendas_antes_preprocessamento[coluna_selecionada].describe() # Refatorar nome da variável e do DataFrame
        estats_depois = df_vendas_depois_preprocessamento[coluna_selecionada].describe() # Refatorar nome da variável e do DataFrame

        df_estats = pd.DataFrame({'Antes': estats_antes, 'Depois': estats_depois}).reset_index().rename(columns={'index': 'Métrica'}) # Refatorar nome da variável

        # Renomear os percentis para Q1, Q2, Q3
        mapeamento_metricas = {'25%': 'Q1', '50%': 'Q2', '75%': 'Q3'} # Refatorar nome da variável
        df_estats['Métrica'] = df_estats['Métrica'].replace(mapeamento_metricas) # Usar nome da variável refatorada

        # Melt o dataframe para o formato longo, ideal para o px.bar
        df_estats_longo = df_estats.melt(id_vars='Métrica', var_name='Estado', value_name='Valor') # Refatorar nome da variável

        fig = px.bar(
            df_estats_longo, # Usar nome da variável refatorada
            x='Métrica',
            y='Valor',
            color='Estado',
            barmode='group',
            title=f"Estatísticas Descritivas de {coluna_selecionada}",
            text_auto='.2s',
            color_discrete_map={'Antes': CINZA_NEUTRO, 'Depois': VERMELHO_ROSSMANN} # Usar constantes refatoradas
        )

        # Definir a ordem das métricas no eixo x
        ordem_metricas = ['count', 'mean', 'std', 'min', 'Q1', 'Q2', 'Q3', 'max'] # Refatorar nome da variável

        fig.update_layout(
            **LAYOUT_GRAFICO_COMUM, # Usar constante refatorada
            legend_title_text='Pré-processamento',
            showlegend=True,
            yaxis_type="log",
            yaxis_title="Valor (Escala Log)",
            xaxis_title="Métrica Estatística"
        )
        # Ajustar ordem e tamanho da fonte do eixo X
        fig.update_xaxes(categoryorder='array', categoryarray=ordem_metricas, tickfont=dict(size=10)) # Usar nome da variável refatorada
        return fig

    @aplicativo.callback( # Usar 'aplicativo'
        Output('grafico-estatisticas-lojas', 'figure'), # Refatorar ID
        Input('dropdown-histograma-lojas', 'value') # Refatorar ID
    )
    def atualizar_grafico_estatisticas_lojas(coluna_selecionada): # Refatorar nome da função e parâmetro
        if coluna_selecionada is None or df_lojas_tratado.empty or df_lojas_original.empty: # Usar os novos nomes dos DataFrames
            return criar_figura_vazia("Selecione uma variável para ver as estatísticas.") # Usar a função refatorada

        if coluna_selecionada not in df_lojas_original.columns: # Usar o novo nome do DataFrame
            return criar_figura_vazia(f"Coluna '{coluna_selecionada}' não encontrada.") # Usar a função refatorada

        if df_lojas_original[coluna_selecionada].dtype == 'object': # Usar o novo nome do DataFrame
            return criar_figura_vazia(f"Estatísticas não aplicáveis para a variável '{coluna_selecionada}'.") # Usar a função refatorada

        estats_antes = df_lojas_original[coluna_selecionada].describe() # Refatorar nome da variável e do DataFrame
        estats_depois = df_lojas_tratado[coluna_selecionada].describe() # Refatorar nome da variável e do DataFrame

        df_estats = pd.DataFrame({'Antes': estats_antes, 'Depois': estats_depois}).reset_index().rename(columns={'index': 'Métrica'}) # Refatorar nome da variável

        # Renomear os percentis para Q1, Q2, Q3
        mapeamento_metricas = {'25%': 'Q1', '50%': 'Q2', '75%': 'Q3'} # Refatorar nome da variável
        df_estats['Métrica'] = df_estats['Métrica'].replace(mapeamento_metricas) # Usar nome da variável refatorada

        # Melt o dataframe para o formato longo
        df_estats_longo = df_estats.melt(id_vars='Métrica', var_name='Estado', value_name='Valor') # Refatorar nome da variável

        fig = px.bar(
            df_estats_longo, # Usar nome da variável refatorada
            x='Métrica',
            y='Valor',
            color='Estado',
            barmode='group',
            title=f"Estatísticas Descritivas de {coluna_selecionada}",
            text_auto='.2s',
            color_discrete_map={'Antes': CINZA_NEUTRO, 'Depois': VERMELHO_ROSSMANN} # Usar constantes refatoradas
        )

        # Definir a ordem das métricas no eixo x
        ordem_metricas = ['count', 'mean', 'std', 'min', 'Q1', 'Q2', 'Q3', 'max'] # Refatorar nome da variável

        fig.update_layout(
            **LAYOUT_GRAFICO_COMUM, # Usar constante refatorada
            legend_title_text='Tratamento de Dados',
            showlegend=True,
            yaxis_type="log",
            yaxis_title="Valor (Escala Log)",
            xaxis_title="Métrica Estatística"
        )
        # Ajustar ordem e tamanho da fonte do eixo X
        fig.update_xaxes(categoryorder='array', categoryarray=ordem_metricas, tickfont=dict(size=10)) # Usar nome da variável refatorada
        return fig