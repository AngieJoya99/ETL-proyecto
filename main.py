import pandas as pd
import datetime
from datetime import date
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
import yaml
from etl import extract, transform, load, utils_etl
import psycopg2

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
    conn = psycopg2.connect(dbname=config_olap['dbname'], user=config_olap['user'], password=config_olap['password'],
                            host=config_olap['host'], port=config_olap['port'])
    cur = conn.cursor()
    with open('sqlscripts.yml', 'r') as f:
        sql = yaml.safe_load(f)
        for key, val in sql.items():
            cur.execute(val)
            conn.commit()

if utils_etl.new_data(olap):

    #Extract
    humanResources =  extract.extractHumanResources(oltp)
    person = extract.extractPerson(oltp)
    production = extract.extractProduction(oltp)
    purchasing = extract.extractPurchasing(oltp)
    sales = extract.extractSales(oltp)
    hierarchy = extract.extractEmployeeHierarchy(oltp)

    #Transform - Crear Tablas
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
    dimProduct = "FALTA AÑADIR" #transform.transformDimProduct(tablas)
    dimProductCategory = transform.transformDimProductCategory(production["ProductCategory"])
    dimProductSubcategory = transform.transformDimProductSubcategory(production["ProductSubcategory"])
    dimPromotion = transform.transformDimPromotion(sales["SpecialOffer"])
    dimReseller = "FALTA AÑADIR" #transform.transformDimReseller(tablas)
    dimSalesReason = transform.transformDimSalesReason(sales["SalesReason"])
    dimSalesTerritory = transform.transformDimSalesTerritory(sales["SalesTerritory"])
    factCurrencyRate = transform.transformFactCurrencyRate(sales)
    factInternetSales = transform.transformFactInternetSales( #INCOMPLETO
        production["Product"], 
        sales["SalesOrderDetail"], 
        sales["SalesOrderHeader"]
    )
    factInternetSalesReason = transform.transformFactInternetSalesReason(sales)
    factResellerSales = "FALTA HACER" #transform.transformFactResellerSales(tablas)
    newFactCurrencyRate = transform.transformNewFactCurrencyRate(sales)
    
    #Transform - Crear columnas que son llaver foráneas
    dimCustomer = transform.fkDimCustomer(dimCustomer, dimGeography)
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

    print('success all facts loaded')
    
else:
    print('done not new data')
