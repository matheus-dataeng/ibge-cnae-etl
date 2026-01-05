import pandas as pd
import tkinter as tk
import psycopg2
import os
from dotenv import load_dotenv
import tkinter.messagebox as messagebox

load_dotenv()
conexao_banco = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

cursor = conexao_banco.cursor()

query_postgres = '''
CREATE TABLE IF NOT EXISTS materiais (
    id_material SERIAL PRIMARY KEY,
    nome VARCHAR(100),
    quantidade INT,
    preco NUMERIC(10,2) ,
    categoria VARCHAR(50),
    status VARCHAR(20),
    fornecedor VARCHAR(100),
    unidade_medida VARCHAR(20) ,
    data_cadastro DATE
);
'''
cursor.execute(query_postgres)
conexao_banco.commit()
cursor.close()
conexao_banco.close()


def salvar_dados_banco():
    #CAPTURA DOS DADOS
    id_material = entry_id.get()
    nome = entry_nome.get()
    quantidade = entry_quantidade.get()
    preco = entry_preco.get()
    categoria = entry_categoria.get()
    status = entry_status.get()
    fornecedor = entry_fornecedor.get()
    unidade_medida = entry_unidade_medida.get()
    data_cadastrado = entry_data_cadastro.get()

    valores = ( 
        id_material,
        nome,
        quantidade,
        preco,
        categoria,
        status, 
        fornecedor,
        unidade_medida,
        data_cadastrado
    )
    conexao_banco = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)


    cursor = conexao_banco.cursor()
    query_postgres = '''

    INSERT INTO materiais
        (id_material, nome, quantidade, preco, categoria, status, fornecedor, unidade_medida, data_cadastro)
    VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s)    

    '''
    cursor.execute(query_postgres, valores)
    conexao_banco.commit()
    cursor.close()
    conexao_banco.close()

    #DELETE AUTOMATICO DOS CAMPOS
    entry_id.delete(0, 'end')
    entry_nome.delete(0, 'end')
    entry_quantidade.delete(0, 'end')
    entry_preco.delete(0, 'end')
    entry_categoria.delete(0, 'end')
    entry_status.delete(0, 'end')
    entry_fornecedor.delete(0, 'end')
    entry_unidade_medida.delete(0, 'end')
    entry_data_cadastro.delete(0, 'end')
    messagebox.showinfo('Sucesso!', 'Captura realizada!')

def arquivo_json():
    conexao_banco = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

    df = pd.read_sql("SELECT * FROM materiais", conexao_banco)
    conexao_banco.close()

    # Converter datas para string segura, sem usar .dt
    df['data_cadastro'] = pd.to_datetime(df['data_cadastro'], errors='coerce')
    df['data_cadastro'] = df['data_cadastro'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else "")

    # Salvar JSON
    df.to_json('material_construcao.json', orient='records', force_ascii=False, indent=4)

    messagebox.showinfo('Sucesso!', 'Arquivo JSON atualizado!')
    
# INTERFACE
janela = tk.Tk()
janela.title('Cadastro Material')

# Campo Material
janela_id = tk.Label(janela, text='Id do material:')
janela_id.grid(row=0, column=0, padx=10, pady=10)
entry_id = tk.Entry(janela, width=30)
entry_id.grid(row=0, column=1, padx=10, pady=10)

# Campo Nome Material
janela_nome = tk.Label(janela, text='Nome do material:')
janela_nome.grid(row=1, column=0, padx=10, pady=10)
entry_nome = tk.Entry(janela, width=30)
entry_nome.grid(row=1, column=1, padx=10, pady=10)

# Campo Quantidade
janela_quantidade = tk.Label(janela, text='Quantidade do material:')
janela_quantidade.grid(row=2, column=0, padx=10, pady=10)
entry_quantidade = tk.Entry(janela, width=30)
entry_quantidade.grid(row=2, column=1, padx=10, pady=10)

# Campo Preço
janela_preco = tk.Label(janela, text='Preço do material:')
janela_preco.grid(row=3, column=0, padx=10, pady=10)
entry_preco = tk.Entry(janela, width=30)
entry_preco.grid(row=3, column=1, padx=10, pady=10)

# Campo Categoria
janela_categoria = tk.Label(janela, text='Categoria do material:')
janela_categoria.grid(row=4, column=0, padx=10, pady=10)
entry_categoria = tk.Entry(janela, width=30)
entry_categoria.grid(row=4, column=1, padx=10, pady=10)

# Campo Status
janela_status = tk.Label(janela, text='Status do material:')
janela_status.grid(row=5, column=0, padx=10, pady=10)
entry_status = tk.Entry(janela, width=30)
entry_status.grid(row=5, column=1, padx=10, pady=10)

# Campo Fornecedor
janela_fornecedor = tk.Label(janela, text='Fornecedor do material:')
janela_fornecedor.grid(row=6, column=0, padx=10, pady=10)
entry_fornecedor = tk.Entry(janela, width=30)
entry_fornecedor.grid(row=6, column=1, padx=10, pady=10)

# Campo Unidade de Medida
janela_unidade_medida = tk.Label(janela, text='Unidade de medida:')
janela_unidade_medida.grid(row=7, column=0, padx=10, pady=10)
entry_unidade_medida = tk.Entry(janela, width=30)
entry_unidade_medida.grid(row=7, column=1, padx=10, pady=10)

# Campo Data de Cadastro
janela_data_cadastro = tk.Label(janela, text='Data de cadastro:')
janela_data_cadastro.grid(row=8, column=0, padx=10, pady=10)
entry_data_cadastro = tk.Entry(janela, width=30)
entry_data_cadastro.grid(row=8, column=1, padx=10, pady=10)

# Botões
btn_postgres = tk.Button(janela, width=30, text='Inserir dados', command=salvar_dados_banco)
btn_postgres.grid(row=9, column=0, padx=10, pady=10)

btn_json = tk.Button(janela, width=30, text='Arquivo JSON', command=arquivo_json)
btn_json.grid(row=9, column=1, padx=10, pady=10)

janela.mainloop()
