import sys
import os

# Adicione o diretório raiz do seu projeto ao sys.path para que ele possa encontrar 'dashboard.app'
# Esta linha pode precisar ser ajustada dependendo da sua estrutura exata de diretórios
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from dash.app import server # Importa a instância 'server' do seu app.py original

# Esta é a função que o Netlify irá chamar
def handler(event, context):
    from werkzeug.wrappers import Request, Response
    from werkzeug.serving import run_simple

    # Adapte o evento Netlify para um objeto de requisição Werkzeug
    request = Request.from_values(
        query_string=event.get('queryStringParameters'),
        headers=event.get('headers'),
        method=event.get('httpMethod'),
        path=event.get('path'),
        json=event.get('body') if event.get('isBase64Encoded') else None,
        input_stream=event.get('body') if event.get('isBase64Encoded') else None
    )

    # Chame a aplicação Flask subjacente do Dash
    response = server.wsgi_app(request.environ, lambda status, headers: Response(status=status, headers=headers))

    # Adapte a resposta Werkzeug para o formato de resposta do Netlify Function
    return {
        'statusCode': response.status_code,
        'headers': dict(response.headers),
        'body': response.data.decode('utf-8')
    }

# Se você quiser testar localmente com Gunicorn (fora do Netlify Functions),
# o seu app.py original do Dash já deve ter a parte de 'app.run_server'.
# A parte 'handler' é especificamente para o ambiente de funções Netlify.
