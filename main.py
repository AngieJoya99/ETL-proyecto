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
        "Hierarchy"# ["Hierarchy"]
    )
    dimGeography = transform.transformDimGeography(sales, person)
    dimProduct = "FALTA AÑADIR"
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
    
    #Load
    load.load(dim_ips, olap, 'dim_ips', True)
    load.load(dim_fecha, olap, 'dim_fecha', True)
    load.load(dim_servicio, olap, 'dim_servicio', True)
    load.load(dim_persona, olap, 'dim_persona', True)
    load.load(dim_medico, olap, 'dim_medico', True)
    load.load(trans_servicio, olap, 'trans_servicio', True)
    load.load(dim_diag, olap, 'dim_diag', True)
    load.load(dim_demo, olap, 'dim_demografia', True)
    load.load(dim_drug,olap,'dim_medicamentos',True)

    print('success all facts loaded')
    
else:
    print('done not new data')

#%%
