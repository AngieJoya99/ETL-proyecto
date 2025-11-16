import pandas as pd
from pandas import DataFrame
from sqlalchemy.engine import Engine
from sqlalchemy import text
import yaml
from sqlalchemy.dialects.postgresql import insert

def loadDimCurrency(df, olap: Engine):
    df.to_sql('DimCurrency', olap, if_exists='append', index_label='nombreColumnaLlave')

def loadDimCustomer(df, olap: Engine):
    df.to_sql('DimCustomer', olap, if_exists='append', index_label='nombreColumnaLlave')

def loadDimDate(df, olap: Engine):
    df.to_sql('DimDate', olap, if_exists='append', index_label='nombreColumnaLlave')

def loadDimEmployee(df, olap: Engine):
    df.to_sql('DimEmployee', olap, if_exists='append', index_label='nombreColumnaLlave')

def loadDimGeography(df, olap: Engine):
    df.to_sql('DimGeography', olap, if_exists='append', index_label='nombreColumnaLlave')

def loadDimProduct(df, olap: Engine):
    df.to_sql('DimProduct', olap, if_exists='append', index_label='nombreColumnaLlave')

def loadDimProductCategory(df, olap: Engine):
    df.to_sql('DimProductCategory', olap, if_exists='append', index_label='nombreColumnaLlave')

def loadDimProductSubcategory(df, olap: Engine):
    df.to_sql('DimProductSubcategory', olap, if_exists='append', index_label='nombreColumnaLlave')

def loadDimPromotion(df, olap: Engine):
    df.to_sql('DimPromotion', olap, if_exists='append', index_label='nombreColumnaLlave')

def loadDimReseller(df, olap: Engine):
    df.to_sql('DimReseller', olap, if_exists='append', index_label='nombreColumnaLlave')

def loadDimSalesReason(df, olap: Engine):
    df.to_sql('DimSalesReason', olap, if_exists='append', index_label='nombreColumnaLlave')

def loadDimSalesTerritory(df, olap: Engine):
    df.to_sql('DimSalesTerritory', olap, if_exists='append', index_label='nombreColumnaLlave')

def loadFactAdditionalInternationalProductDescription(df, olap: Engine):
    df.to_sql('FactAdditionalInternationalProductDescription', olap, if_exists='append', index_label='nombreColumnaLlave')

def loadFactCurrencyRate(df, olap: Engine):
    df.to_sql('FactCurrencyRate', olap, if_exists='append', index_label='nombreColumnaLlave')

def loadFactInternetSales(df, olap: Engine):
    df.to_sql('FactInternetSales', olap, if_exists='append', index_label='nombreColumnaLlave')

def loadFactInternetSalesReason(df, olap: Engine):
    df.to_sql('FactInternetSalesReason', olap, if_exists='append', index_label='nombreColumnaLlave')

def loadFactResellerSales(df, olap: Engine):
    df.to_sql('FactResellerSales', olap, if_exists='append', index_label='nombreColumnaLlave')

def loadNewFactCurrencyRate(df, olap: Engine):
    df.to_sql('NewFactCurrencyRate', olap, if_exists='append', index_label='nombreColumnaLlave')

def load(table: DataFrame, etl_conn: Engine, tname, replace: bool = False):
    """

    :param table: table to load into the database
    :param etl_conn: sqlalchemy engine to connect to the database
    :param tname: table name to load into the database
    :param replace:  when true it deletes existing table data(rows)
    :return: void it just load the table to the database
    """
    # statement = insert(f'{table})
    # with etl_conn.connect() as conn:
    #     conn.execute(statement)
    if replace :
        with etl_conn.connect() as conn:
            conn.execute(text(f'Delete from {tname}'))
            conn.close()
        table.to_sql(f'{tname}', etl_conn, if_exists='append', index=False)
    else :
        table.to_sql(f'{tname}', etl_conn, if_exists='append', index=False)
