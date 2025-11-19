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

#Transform dimensions
dimCurrency = transform.transformDimCurrency(sales["Currency"])
dimCustomer = transform.transformDimCustomer(person, sales)
dimDate = transform.transformDimDate()
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
dimGeography = transform.transformDimGeography(sales, person)

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


dimProductCategory = transform.transformDimProductCategory(production["ProductCategory"])
dimProductSubcategory = transform.transformDimProductSubcategory(production["ProductSubcategory"])
dimPromotion = transform.transformDimPromotion(sales["SpecialOffer"])
dimReseller = transform.transformDimReseller(
    sales["Customer"], 
    sales["SalesOrderHeader"], 
    person["PersonPhone"], 
    person["Address"], 
    person["BusinessEntityAddress"], 
    utils_etl.extractStoreDemographics(oltp),
    dimGeography,
    person["StateProvince"]
)
dimSalesReason = transform.transformDimSalesReason(sales["SalesReason"])
dimSalesTerritory = transform.transformDimSalesTerritory(sales["SalesTerritory"])

#Transform facts
factCurrencyRate = transform.transformFactCurrencyRate(sales)
factInternetSales = transform.transformFactInternetSales(
    production["Product"], 
    sales["SalesOrderDetail"], 
    sales["SalesOrderHeader"],        
    sales["Customer"], 
    dimCustomer, 
    dimCurrency, 
    sales["CurrencyRate"], 
    person["StateProvince"], 
    sales["SalesTaxRate"]
)
factInternetSalesReason = transform.transformFactInternetSalesReason(sales)
factResellerSales = transform.transformFactResellerSales(        
    production["Product"], 
    sales["SalesOrderDetail"], 
    sales["SalesOrderHeader"], 
    dimCurrency, 
    sales["CurrencyRate"], 
    dimReseller
)
newFactCurrencyRate = transform.transformNewFactCurrencyRate(sales)

#Transform - Crear columnas que son llaver foráneas
dimCustomer = transform.fkDimCustomer(dimCustomer, dimGeography,person)
factCurrencyRate = transform.fkFactCurrencyRate(factCurrencyRate, dimCurrency)
newFactCurrencyRate = transform.fkNewFactCurrencyRate(newFactCurrencyRate, dimCurrency, dimDate)

#Load
load.loadDimCurrency(dimCurrency,olap)
load.loadDimCustomer(dimCustomer, olap)
load.loadDimDate(dimDate, olap)
load.loadDimEmployee(dimEmployee, olap)
load.loadDimGeography(dimGeography, olap)
load.loadDimProduct(dimProduct, olap)
load.loadDimProductCategory(dimProductCategory, olap)
load.loadDimProductSubcategory(dimProductSubcategory, olap)
load.loadDimPromotion(dimPromotion, olap)
load.loadDimReseller(dimReseller, olap)
load.loadDimSalesReason(dimSalesReason, olap)
load.loadDimSalesTerritory(dimSalesTerritory, olap)

load.loadFactCurrencyRate(factCurrencyRate, olap)
load.loadFactInternetSales(factInternetSales, olap)
load.loadFactInternetSalesReason(factInternetSalesReason, olap)
load.loadFactResellerSales(factResellerSales, olap)
load.loadNewFactCurrencyRate(newFactCurrencyRate, olap)

print('success all tables loaded')