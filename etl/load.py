import pandas as pd
from pandas import DataFrame
from sqlalchemy.engine import Engine
from sqlalchemy import text
import yaml
from sqlalchemy.dialects.postgresql import insert

def loadDimCurrency(df, olap: Engine):
    df.to_sql('DimCurrency', olap, if_exists='replace', index=False)

def loadDimCustomer(df, olap: Engine):
    df.to_sql('DimCustomer', olap, if_exists='replace', index=False)

def loadDimDate(df, olap: Engine):
    df.to_sql('DimDate', olap, if_exists='replace', index=False)

def loadDimEmployee(df, olap: Engine):
    df.to_sql('DimEmployee', olap, if_exists='replace', index=False)

def loadDimGeography(df, olap: Engine):
    df.to_sql('DimGeography', olap, if_exists='replace', index=False)

def loadDimProduct(df, olap: Engine):
    df.to_sql('DimProduct', olap, if_exists='replace', index=False)

def loadDimProductCategory(df, olap: Engine):
    df.to_sql('DimProductCategory', olap, if_exists='replace', index=False)

def loadDimProductSubcategory(df, olap: Engine):
    df.to_sql('DimProductSubcategory', olap, if_exists='replace', index=False)

def loadDimPromotion(df, olap: Engine):
    df.to_sql('DimPromotion', olap, if_exists='replace', index=False)

def loadDimReseller(df, olap: Engine):
    df.to_sql('DimReseller', olap, if_exists='replace', index=False)

def loadDimSalesReason(df, olap: Engine):
    df.to_sql('DimSalesReason', olap, if_exists='replace', index=False)

def loadDimSalesTerritory(df, olap: Engine):
    df.to_sql('DimSalesTerritory', olap, if_exists='replace', index=False)

def loadFactCurrencyRate(df, olap: Engine):
    df.to_sql('FactCurrencyRate', olap, if_exists='replace', index=False)

def loadFactInternetSales(df, olap: Engine):
    df.to_sql('FactInternetSales', olap, if_exists='replace', index=False)

def loadFactInternetSalesReason(df, olap: Engine):
    df.to_sql('FactInternetSalesReason', olap, if_exists='replace', index=False)

def loadFactResellerSales(df, olap: Engine):
    df.to_sql('FactResellerSales', olap, if_exists='replace', index=False)

def loadNewFactCurrencyRate(df, olap: Engine):
    df.to_sql('NewFactCurrencyRate', olap, if_exists='replace', index=False)