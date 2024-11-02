"""
Módulo destinado para:

- Ler arquivos;
- Concatenar arquivos;
- Transformar arquivos;
- Load de arquivos;

"""

import pandas as pd
import os
import glob

# Func de extract para ler e consolidar os json


def extract_data(path: str) -> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(path, '*json')) #listar tudo que está dentro da pasta data com quaquer nome (*) e json
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list, ignore_index=True)
    return df_total


# Func de transform

def total_net_sell_kpi(df: pd.DataFrame) -> pd.DataFrame:
    df["Total"] = df["Quantidade"] * df["Venda"]
    return df


# Func de Load em csv ou parquet

def load_data(df: pd.DataFrame, format_saida: list):
    
    for formato in format_saida:
        if formato == 'csv':
            df.to_csv("dados.csv", index=False)
        elif formato == 'parquet':
            df.to_parquet("dados.parquet", index=False)



def pipeline_calcular_kpi_vendas_consolidado(pasta: str, formato_de_saida: list):
    data_frame = extract_data(pasta)
    data_frame_calculado = total_net_sell_kpi(data_frame)
    load_data(data_frame_calculado, formato_de_saida)