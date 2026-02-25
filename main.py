import pandas as pd
import sqlite3

ARQUIVO_CSV = 'data/microdados.csv'
BANCO_SQLITE = 'censo_escola.db'
NOME_TABELA = 'escolas_nordeste'
ESTADOS_NORDESTE = ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']

def migrar_dados():
    conexao = sqlite3.connect(BANCO_SQLITE) 
    print("Iniciando a migração dos dados... Isso pode levar um tempo.")
    leitor_csv = pd.read_csv(ARQUIVO_CSV, sep=';', encoding='latin1', chunksize=10000)

    primeira_insercao = True

    for chunk in leitor_csv:
        filtro_nordeste = chunk[chunk['SG_UF'].isin(ESTADOS_NORDESTE)]

        if not filtro_nordeste.empty:
            if primeira_insercao:
                filtro_nordeste.to_sql(NOME_TABELA, conexao, if_exists='replace', index=False)
                primeira_insercao = False
                print("Tabela criada e primeiras linhas inseridas...")
            else:
                filtro_nordeste.to_sql(NOME_TABELA, conexao, if_exists='append', index=False)
    
    conexao.close()
    print("Migração concluída com sucesso")

if __name__ == "__main__":
    migrar_dados()