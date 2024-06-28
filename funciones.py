import pandas as pd
from sqlalchemy import create_engine

def filtrar_por_fechas(dataframe, columna, fecha_inicio, fecha_fin):
    fecha_inicio = pd.to_datetime(fecha_inicio)
    fecha_fin = pd.to_datetime(fecha_fin)
    mask = (pd.to_datetime(dataframe[columna]) >= fecha_inicio) & (pd.to_datetime(dataframe[columna]) <= fecha_fin)
    return dataframe.loc[mask]

def generar_reporte(dataframe, filas, columnas, valores, funcion_agrupadora, fill_value=0):
    pivot_table = pd.pivot_table(
        dataframe,
        index=filas,
        columns=columnas,
        values=valores,
        aggfunc=funcion_agrupadora,
        fill_value=fill_value
    )
    return pivot_table

def escribir_df_a_sql(dataframe, nombre_tabla, engine, if_exists='replace', index=True):
    dataframe.to_sql(nombre_tabla, engine, if_exists=if_exists, index=index)
