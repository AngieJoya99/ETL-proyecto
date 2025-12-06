import pandas as pd
import datetime
from datetime import date
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
import yaml
from etl import extract, transform, load, utils_etl
import pyodbc

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)

with open('config.yml', 'r') as f:
    config = yaml.safe_load(f)
    config_oltp = config['OLTP']
    config_olap = config['OLAP']

# Construct the database URL
url_oltp = (f"mssql+pyodbc://{config_oltp['user']}:{config_oltp['password']}@{config_oltp['host']},{config_oltp['port']}/{config_oltp['dbname']}"
          f"?driver={config_oltp['drivername'].replace(' ', '+')}")
url_olap = (f"mssql+pyodbc://{config_olap['user']}:{config_olap['password']}@{config_olap['host']},{config_olap['port']}/{config_olap['dbname']}"
           f"?driver={config_olap['drivername'].replace(' ', '+')}")
# Create the SQLAlchemy Engine
oltp = create_engine(url_oltp)
olap = create_engine(url_olap)
inspector = inspect(olap)
tnames = inspector.get_table_names()
print("Tablas que ya existen =",tnames)

if not tnames:
    conn_str = (
        f"DRIVER={{{config_olap['drivername']}}};"
        f"SERVER={config_olap['host']},{config_olap['port']};"
        f"DATABASE={config_olap['dbname']};"
        f"UID={config_olap['user']};"
        f"PWD={config_olap['password']}"
    )
    
    conn = pyodbc.connect(conn_str)
    cur = conn.cursor()
    with open('sqlscripts.yml', 'r') as f:
        sql = yaml.safe_load(f)
        for key, val in sql.items():
            script = val["create"]     # extraer la cadena SQL real
            cur.execute(script)
            conn.commit()

#Extract
humanResources =  extract.extractHumanResources(oltp)
person = extract.extractPerson(oltp)
production = extract.extractProduction(oltp)
purchasing = extract.extractPurchasing(oltp)
sales = extract.extractSales(oltp)
hierarchy = extract.extractEmployeeHierarchy(oltp)
description = extract.extracProductDescription(oltp)
dealerPrices = extract.extractDealerPrices(oltp)
print("EXTRACCIÓN FINALIZADA")

#Transform dimensions
dimCurrency = transform.transformDimCurrency(sales["Currency"])
print("Transformacion dimCurrency Finalizada")
dimCustomer = transform.transformDimCustomer(person, sales)
print("Transformacion dimCustomer Finalizada")
dimDate = transform.transformDimDate()
print("Transformacion dimDate Finalizada")
dimEmployee = transform.transformDimEmployee(
    humanResources["Employee"], 
    humanResources["EmployeePayHistory"], 
    humanResources["EmployeeDepartmentHistory"], 
    humanResources["Department"], 
    sales["SalesPerson"], 
    person["Person"], 
    person["EmailAddress"], 
    person["PersonPhone"],
    hierarchy
)
print("Transformacion dimEmployee Finalizada")
dimGeography = transform.transformDimGeography(sales, person)
print("Transformacion dimGeography Finalizada")

description_translated = utils_etl.translate_missing_fast(description)
name_translated = utils_etl.translate_missing_fast_name(production["Product"].copy())
size_range_df = utils_etl.generar_size_range_tabla(production["Product"])
dimProduct = transform.transformDimProduct(
    production["Product"], 
    description, 
    name_translated, 
    production["ProductModel"], 
    production["ProductListPriceHistory"], 
    dealerPrices, 
    size_range_df
)
print("Transformacion dimProduct Finalizada")

dimProductCategory = transform.transformDimProductCategory(production["ProductCategory"])
print("Transformacion dimProductCategory Finalizada")
dimProductSubcategory = transform.transformDimProductSubcategory(production["ProductSubcategory"])
print("Transformacion dimProductSubcategory Finalizada")
dimPromotion = transform.transformDimPromotion(sales["SpecialOffer"])
print("Transformacion dimPromotion Finalizada")
dimReseller = transform.transformDimReseller(
    sales["Customer"], 
    sales["SalesOrderHeader"], 
    person["PersonPhone"], 
    person["Address"], 
    person["BusinessEntityAddress"], 
    utils_etl.extractStoreDemographics(oltp),
    dimGeography.copy(),
    person["StateProvince"]
)
print("Transformacion dimReseller Finalizada")
dimSalesReason = transform.transformDimSalesReason(sales["SalesReason"])
print("Transformacion dimSalesReason Finalizada")
dimSalesTerritory = transform.transformDimSalesTerritory(sales["SalesTerritory"])
print("Transformacion dimSalesTerritory Finalizada")
print("LA TRANSFORMACIÓN DE DIMENSIONES HA FINALIZADO")

#Transform facts
factCurrencyRate = transform.transformFactCurrencyRate(sales)
print("Transformacion factCurrencyRate Finalizada")
factInternetSales = transform.transformFactInternetSales(
    production["Product"], 
    sales["SalesOrderDetail"], 
    sales["SalesOrderHeader"],        
    sales["Customer"], 
    dimCustomer.copy(), 
    dimCurrency.copy(), 
    sales["CurrencyRate"], 
    person["StateProvince"], 
    sales["SalesTaxRate"]
)
print("Transformacion factInternetSales Finalizada")
factInternetSalesReason = transform.transformFactInternetSalesReason(sales)
print("Transformacion factInternetSalesReason Finalizada")
factResellerSales = transform.transformFactResellerSales(        
    production["Product"],
    sales["SalesOrderDetail"],
    sales["SalesOrderHeader"],
    dimCurrency,
    sales["CurrencyRate"],
    dimReseller,
    sales["Customer"], 
    sales["SalesPerson"], 
    dimEmployee, 
    sales["Store"], 
    humanResources["Employee"]
)
print("Transformacion factResellerSales Finalizada")
newFactCurrencyRate = transform.transformNewFactCurrencyRate(sales)
print("Transformacion newFactCurrencyRate Finalizada")

#Transform - Crear columnas que son llaver foráneas
dimCustomer = transform.fkDimCustomer(dimCustomer, dimGeography.copy(),person)
factCurrencyRate = transform.fkFactCurrencyRate(factCurrencyRate, dimCurrency)
newFactCurrencyRate = transform.fkNewFactCurrencyRate(newFactCurrencyRate, dimCurrency, dimDate)
print("LA TRANSFORMACIÓN DE HECHOS HA FINALIZADO")

#Load

# CARGA DE DIMENSIONES
load.load(dimDate, olap, 'DimDate', True)
print("Carga dimDate Finalizada")

load.load(dimCurrency, olap, 'DimCurrency', True)
print("Carga dimCurrency Finalizada")

load.load(dimSalesTerritory, olap, 'DimSalesTerritory', True)
print("Carga dimSalesTerritory Finalizada")

load.load(dimGeography, olap, 'DimGeography', True)
print("Carga dimGeography Finalizada")

load.load(dimSalesReason, olap, 'DimSalesReason', True)
print("Carga dimSalesReason Finalizada")

load.load(dimPromotion, olap, 'DimPromotion', True)
print("Carga dimPromotion Finalizada")

load.load(dimProductCategory, olap, 'DimProductCategory', True)
print("Carga dimProductCategory Finalizada")

load.load(dimProductSubcategory, olap, 'DimProductSubcategory', True)
print("Carga dimProductSubcategory Finalizada")

load.load(dimProduct, olap, 'DimProduct', True)
print("Carga dimProduct Finalizada")

load.load(dimCustomer, olap, 'DimCustomer', True)
print("Carga dimCustomer Finalizada")

load.load(dimEmployee, olap, 'DimEmployee', True)
print("Carga dimEmployee Finalizada")

load.load(dimReseller, olap, 'DimReseller', True)
print("Carga dimReseller Finalizada")
print("LA CARGA DE DIMENSIONES HA FINALIZADO")

# CARGA DE TABLAS DE HECHOS
load.load(factCurrencyRate, olap, 'FactCurrencyRate', True)
print("Carga factCurrencyRate Finalizada")

load.load(newFactCurrencyRate, olap, 'NewFactCurrencyRate', True)
print("Carga newFactCurrencyRate Finalizada")

load.load(factInternetSales, olap, 'FactInternetSales', True)
print("Carga factInternetSales Finalizada")

load.load(factInternetSalesReason, olap, 'FactInternetSalesReason', True)
print("Carga factInternetSalesReason Finalizada")

load.load(factResellerSales, olap, 'FactResellerSales', True)
print("Carga factResellerSales Finalizada")
print("LA CARGA DE HECHOS HA FINALIZADO")

print('El proceso ha finalizado con éxito')