import pandas as pd
import utils_etl
from sqlalchemy.engine import Engine
import pandas as pd
from sqlalchemy import create_engine, inspect
import yaml
import os

#Se leen así: humanResources["Shift"]
def cargaSegura(engine, schema, table):
    inspector = inspect(engine)

    # Obtener columnas
    columnas = [col["name"] for col in inspector.get_columns(table, schema=schema)]
    columnas_problematicas = []

    # Intentar cargar tabla completa
    try:
        return pd.read_sql_table(table_name=table, con=engine, schema=schema)
    except Exception:
        pass

    # Detectar columnas problemáticas
    for col in columnas:
        try:
            pd.read_sql_query(
                f'SELECT TOP 10 "{col}" FROM "{schema}"."{table}"',
                con=engine
            )
        except Exception:
            columnas_problematicas.append(col)


    # Columnas buenas
    columnas_ok = [col for col in columnas if col not in columnas_problematicas]

    # Si no hay columnas válidas
    if not columnas_ok:
        print(f"⚠ La tabla {schema}.{table} no tiene columnas convertibles. Retornando dataframe vacío.")
        return pd.DataFrame()

    # Cargar solo columnas válidas
    query = (
        f'SELECT {", ".join([f"""\"{c}\"""" for c in columnas_ok])} '
        f'FROM "{schema}"."{table}"'
    )

    df = pd.read_sql_query(query, con=engine)
    return df


def extractHumanResources(conection):
    tablas = [
        "Shift", "Department", "Employee", "EmployeeDepartmentHistory", "EmployeePayHistory"
    ]
    humanResources = {}
    for tabla in tablas:
        df = cargaSegura(conection, "HumanResources", tabla)
        humanResources[tabla] = df
        
    return humanResources

def extractPerson(conection):
    tablas = [
        "PersonPhone", "PhoneNumberType", "Address", "AddressType",
        "StateProvince", "BusinessEntity", "BusinessEntityAddress", "BusinessEntityContact",
        "ContactType", "CountryRegion", "EmailAddress", "Password", "Person"
    ]
    person = {}
    for tabla in tablas:
        df = cargaSegura(conection, "Person", tabla)
        person[tabla] = df
        
    return person

def extractProduction(conection):
    tablas = [
        "Product", "ScrapReason", "ProductCategory", "ProductCostHistory", "ProductDescription",
        "ProductDocument", "ProductInventory", "ProductListPriceHistory", "ProductModel",
        "ProductModelIllustration", "ProductModelProductDescriptionCulture", "BillOfMaterials",
        "ProductPhoto", "ProductProductPhoto", "TransactionHistory", "ProductReview",
        "TransactionHistoryArchive", "ProductSubcategory", "UnitMeasure", "WorkOrder",
        "Culture", "WorkOrderRouting", "Document", "Illustration", "Location"
    ]
    production = {}
    for tabla in tablas:
        df = cargaSegura(conection, "Production", tabla)
        production[tabla] = df
        
    return production

def extractPurchasing(conection):
    tablas = [
        "ShipMethod", "ProductVendor", "Vendor", "PurchaseOrderDetail", "PurchaseOrderHeader"
    ]
    purchasing = {}
    for tabla in tablas:
        df = cargaSegura(conection, "Purchasing", tabla)
        purchasing[tabla] = df
        
    return purchasing

def extractSales(conection):
    tablas = [
        "CountryRegionCurrency", "CreditCard", "Currency", "CurrencyRate", "Customer",
        "PersonCreditCard", "SalesOrderDetail", "SalesOrderHeader",
        "SalesOrderHeaderSalesReason", "SalesPerson",
        "SalesPersonQuotaHistory", "SalesReason", "SalesTaxRate",
        "SalesTerritory", "SalesTerritoryHistory", "ShoppingCartItem",
        "SpecialOffer", "SpecialOfferProduct", "Store"
    ]
    sales = {}
    for tabla in tablas:
        df = cargaSegura(conection, "Sales", tabla)
        sales[tabla] = df
        
    return sales



    
