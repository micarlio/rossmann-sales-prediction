# application.py
# Este arquivo serve como ponto de entrada para o servidor do Elastic Beanstalk.

# Importa a variável 'server' do seu arquivo principal da aplicação Dash
from dashboard.app import server

# Renomeia a variável 'server' para 'application' como o Elastic Beanstalk espera
application = server
