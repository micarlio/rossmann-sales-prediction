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

# Carregar os dados usando o novo sistema centralizado
# Você pode escolher entre o modo 'amostra' ou 'data' aqui
# Para desenvolvimento e testes rápidos, use o modo 'amostra' com um número baixo de amostras
# Para análises mais completas, use o modo 'data' com um intervalo de datas específico
modo_carregamento = os.environ.get('MODO_CARREGAMENTO', 'amostra')
n_amostras = int(os.environ.get('N_AMOSTRAS', '40'))
data_inicio = os.environ.get('DATA_INICIO', None)
data_fim = os.environ.get('DATA_FIM', None)
force_reprocess = os.environ.get('FORCE_REPROCESS', 'False').lower() == 'true'

print(f"Carregando dados no modo '{modo_carregamento}'...")
print(f"  - Amostras por loja: {n_amostras if modo_carregamento == 'amostra' else 'N/A'}")
print(f"  - Intervalo de datas: {data_inicio} a {data_fim if data_fim else 'hoje'}" if modo_carregamento == 'data' else "")
print(f"  - Forçar reprocessamento: {force_reprocess}")

dados = carregar_dados(
    modo=modo_carregamento,
    n_amostras=n_amostras,
    data_inicio=data_inicio,
    data_fim=data_fim,
    force_reprocess=force_reprocess
)

# Converter o DataFrame principal para JSON para armazenar no dcc.Store
df_principal_json = dados.get("df_principal_json", "{}")

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