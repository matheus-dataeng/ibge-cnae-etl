import pandas as pd
import requests
import urllib
from sqlalchemy import create_engine, Integer, NVARCHAR, String
from datetime import datetime
import schedule
import time

url = 'https://servicodados.ibge.gov.br/api/v2/cnae/subclasses'
response = requests.get(url).json()
df = pd.json_normalize(response)

#TRATAMENTO DATAFRAME SEÇÃO
colunas_secao = ['classe.grupo.divisao.secao.id', 
                'classe.grupo.divisao.secao.descricao']

novos_nomes_secao = {'classe.grupo.divisao.secao.id': 'Secao_id',
                     'classe.grupo.divisao.secao.descricao' : 'Secao_descricao'}    

df_secao = df[colunas_secao].rename(columns=novos_nomes_secao).drop_duplicates()

agora = datetime.now()
df_secao['Data_Carga'] = agora.date().strftime('%d/%m/%Y')
df_secao['Hora_Carga'] = agora.time().strftime('%H:%M:%S')

#TRATAMENTO DATAFRAME DIVISÃO
colunas_divisao = ['classe.grupo.divisao.id',
                   'classe.grupo.divisao.descricao',
                   'classe.grupo.divisao.secao.id']

novos_nomes_divisao = {'classe.grupo.divisao.id' : 'Divisao_id',
                       'classe.grupo.divisao.descricao' : 'Divisao_descricao',
                      'classe.grupo.divisao.secao.id' : 'Secao_id'}

df_divisao = df[colunas_divisao].rename(columns=novos_nomes_divisao).drop_duplicates()

agora_divisao = datetime.now()
df_divisao['Data_Carga'] = agora_divisao.date().strftime('%d/%m/%Y')
df_divisao['Divisao_descricao'] = df_divisao['Divisao_descricao'].str.title() 

#TRATAMENTO DATAFRAME GRUPO
colunas_grupo = ['classe.grupo.id',
                 'classe.grupo.descricao',
                 'classe.grupo.divisao.id']

novos_nomes_grupo = {'classe.grupo.id' :'Grupo_id',
                     'classe.grupo.descricao' : 'Grupo_descricao',
                     'classe.grupo.divisao.id' : 'Divisao_id'}

df_grupo = df[colunas_grupo].rename(columns=novos_nomes_grupo).drop_duplicates()

#TRATAMENTO DATAFRAME CLASSE
colunas_classe = ['classe.id',
                  'classe.descricao',
                  'classe.grupo.id',
                  'classe.observacoes']

novos_nomes_classes = {'classe.id' : 'Classe_id',
                       'classe.descricao' : 'Classe_descricao',
                       'classe.grupo.id' : 'Grupo_id',
                       'classe.observacoes' : 'Classe_observacoes'}

df_classes = df[colunas_classe].rename(columns=novos_nomes_classes)
df_classes['Classe_observacoes'] = df_classes['Classe_observacoes'].apply(str)
df_classes.drop_duplicates(inplace=True)

#TRATAMENTO DATAFRAME SUBCLASSE
colunas_subclasse = ['id',
                     'descricao',
                     'atividades',
                     'observacoes',
                     'classe.id']

novos_nomes_subclasses = {'id' : 'Id',
                          'descricao' : 'Descricao',
                          'atividades' : 'Atividades',
                          'observacoes' : 'Observacoes',
                          'classe.id' :'Classe_id'}

df_subclasses = df[colunas_subclasse].rename(columns=novos_nomes_subclasses)
df_subclasses = df_subclasses.drop_duplicates(subset=['Id'])
df_subclasses['Atividades'] = df_subclasses['Atividades'].apply(str)

#CONEXÃO COM BANCO
#def carga_banco():
server = 'MATHEUS'
database = 'Projeto_API'
conexao_sqlserver = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={server};"
    f"DATABASE={database};"
    "Trusted_Connection=yes;"
)

params = urllib.parse.quote_plus(conexao_sqlserver)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

nome_tabela_secao = 'Secao'
nome_tabela_divisao = 'Divisao'
nome_tabela_grupo = 'Grupo'
nome_tabela_classes = 'Classe'
nome_tabela_subclasses = 'Subclasse'

df_secao.to_sql(name=nome_tabela_secao, con=engine, if_exists='replace', index=False, dtype={
    'Secao_id' : NVARCHAR(255),
    'Secao_descricao' : NVARCHAR(None)
})

df_divisao.to_sql(name=nome_tabela_divisao, con=engine, if_exists='replace', index=False, dtype={
    'Divisao_id' : NVARCHAR(255),
    'Divisao_descricao' : NVARCHAR(None),
    'Secao_id' : NVARCHAR(255)
})

df_grupo.to_sql(name=nome_tabela_grupo, con=engine, if_exists='replace', index=False, dtype={
    'Grupo_id' : NVARCHAR(255),
    'Grupo_descricao' : NVARCHAR(None),
    'Divisao_id' : NVARCHAR(255),
})

df_classes.to_sql(name=nome_tabela_classes, con=engine, if_exists='replace', index=False, dtype={
        'Classe_id' : NVARCHAR(255),
        'Classe_descricao' : NVARCHAR(None),
        'Grupo_id' : NVARCHAR(255),
        'Classe_observacoes' : NVARCHAR(None)
})

df_subclasses.to_sql(name=nome_tabela_subclasses, con=engine, if_exists='replace', index=False, dtype={
    'Id' : NVARCHAR(255),
    'Descricao' : NVARCHAR(None),
    'Atividades' : NVARCHAR(None),
    'Observacoes' : NVARCHAR(None),
    'Classe_id' : NVARCHAR(255),
})
engine.dispose()
print("Todas as tabelas atualizadas com sucesso!")

        
'''schedule.clear()
schedule.every().monday.at("12:00").do(carga_banco)

while True:
    schedule.run_pending()
    time.sleep(1)'''

'''
SELECT
    o.order_id,
    o.order_date,
    c.customer_id,
    c.customer_name,
    s.store_id,
    s.store_name,
    oi.product_id,
    oi.quantity,
    oi.price
FROM orders o
LEFT JOIN customers c
    ON o.customer_id = c.customer_id
LEFT JOIN stores s
    ON o.store_id = s.store_id
LEFT JOIN order_items oi
    ON o.order_id = oi.order_id;

'''