import pandas as pd
import mysql.connector
from mysql.connector import errorcode
import re


def csv_to_dataframe(lista_csv, lista_nomes):
    dataframes = {}
    for arquivo, nome in zip(lista_csv, lista_nomes):
        df = pd.read_csv(arquivo)
        dataframes[nome] = df
    return dataframes


def paises(df_geral, nomes_df):
    lista_paises = []
    for df in nomes_df:
        df_energia = df_geral[df]
        if 'Entity' in df_energia.columns:
            todos_paises = df_energia['Entity'].unique()
            lista_paises.extend(todos_paises)
    geral = pd.Series(lista_paises).unique()
    return geral


def muda_codigo(dicio_paises, df):
    # Define uma função para mapear o código com base no país
    def obter_codigo(row):
        pais = row['Entity']
        if pais in dicio_paises:
            return dicio_paises[pais]
        else:
            return row['Code']

    # Aplica a função à coluna 'Code'
    df['Code'] = df.apply(obter_codigo, axis=1)

    return df


def todos_df(df, lista_nomes, dicio_paises):
    for nome in lista_nomes:
        df[nome] = muda_codigo(dicio_paises, df[nome])
    return df


def reordenando_colunas(df, lista_nomes):
    for nome in lista_nomes:
        df[nome] = df[nome][['Code'] +
                            [col for col in df[nome].columns if col != 'Code']]
    return df


def salva_csv(df, lista_nomes):
    for nome in lista_nomes:
        df[nome].to_csv(f'data/clean_data/clean-{nome}.csv')


def modifica_nome_coluna(df, col1, col2, col3, col4):
    df.columns = [col1, col2, col3, col4]
    return df


# posso pegar uma lista com o nome dos dataframes
# inserir como parametro de entrada de uma funçao
# colocar tudo em minusculo
# substituir espacos por _
# verificar numero de caracteres da ultima coluna do df

def altera_colunas(df, lista_nomes):
    for nome in lista_nomes:
        df_escolhido = df[nome]
        df_escolhido.columns = df_escolhido.columns.str.lower()
        coluna_4 = df_escolhido.columns[3]
        df_escolhido.rename(
            columns={coluna_4: re.sub(" ", "_", coluna_4)}, inplace=True)
    return df


def verifica_tamanho_coluna(df, lista_nome):
    for nome in lista_nome:
        df_escolhido = df[nome]
        coluna_4 = df_escolhido.columns[3]
        tamanho = len(coluna_4)
        if tamanho > 64:
            print(f"A coluna {coluna_4} do dataframe "
                  f"{nome} possui mais de 64 caracteres")
        else:
            print(f"A coluna 4 do dataframe "
                  f"{nome} está dentro do tamanho permitido")


def renomear_coluna(df, nome_df, nome_col):
    meu_df = df[nome_df]
    coluna_4 = meu_df.columns[3]
    meu_df.rename(
        columns={coluna_4: nome_col}, inplace=True)
    df[nome_df] = meu_df
    return df


# função p conectar com o mysql e inserir os dados