# dashboard/app.py
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import sys
import os

# Adicionar o diretório pai ao path para permitir importações absolutas
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importar layouts e dados com caminho absoluto
from dashboard.layouts import (
    barra_lateral,
    criar_layout_contextualizacao,
    criar_layout_limpeza_dados,
    criar_layout_analise_preliminar,
    criar_layout_dashboard_analise,
    criar_layout_previsao_vendas,
    criar_layout_analise_lojas,
    criar_layout_analise_3d
)
from dashboard.data_loader import carregar_dados
from dashboard.callbacks import registrar_callbacks

# ==============================================================================
# Inicialização do Aplicativo
# ==============================================================================
aplicativo = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        dbc.icons.FONT_AWESOME,
        # Adicione aqui o caminho para seu CSS customizado se tiver um
        '/assets/css/estilos_customizados.css',
        '/assets/css/estilos_barra_lateral.css'
    ],
    suppress_callback_exceptions=True,
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1.0"}
    ]
)
servidor = aplicativo.server
aplicativo.title = "Rossmann Sales Dashboard"

# Carregar os dados uma vez
dados = carregar_dados()

# Converter o DataFrame principal para JSON para armazenar no dcc.Store
df_principal_json = dados["df_principal"].to_json(date_format='iso', orient='split')

# ==============================================================================
# Layout do Aplicativo
# ==============================================================================
# Carregar todos os layouts no início
# A visibilidade será controlada por um callback que altera o 'display'
aplicativo.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    # Componente de armazenamento para o DataFrame principal
    dcc.Store(id='armazenamento-df-principal', data=df_principal_json),
    barra_lateral,
    html.Div(
        id='conteudo-pagina',
        className='content',
        children=[
            html.Div(criar_layout_contextualizacao(dados), id='conteudo-pagina-/', style={'display': 'block'}),
            html.Div(criar_layout_limpeza_dados(dados), id='conteudo-pagina-/limpeza-dados', style={'display': 'none'}),
            html.Div(criar_layout_analise_preliminar(dados), id='conteudo-pagina-/analise-preliminar', style={'display': 'none'}),
            html.Div(criar_layout_dashboard_analise(dados), id='conteudo-pagina-/dashboard', style={'display': 'none'}),
            html.Div(criar_layout_analise_lojas(dados), id='conteudo-pagina-/analise-lojas', style={'display': 'none'}),
            html.Div(criar_layout_analise_3d(dados), id='conteudo-pagina-/analise-3d', style={'display': 'none'}),
            html.Div(criar_layout_previsao_vendas(), id='conteudo-pagina-/previsao-vendas', style={'display': 'none'}),
        ]
    )
])

# ==============================================================================
# Registro de Callbacks
# ==============================================================================
registrar_callbacks(aplicativo, dados)

# ==============================================================================
# Execução do Aplicativo
# ==============================================================================
if __name__ == '__main__':
    # Obter a porta do ambiente (para Heroku/Render) ou usar 8050 como padrão
    port = int(os.environ.get('PORT', 8050))
    
    # Definir o host como 0.0.0.0 para permitir acesso externo
    aplicativo.run(host='localhost', port=8050, debug=False)