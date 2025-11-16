import pandas as pd
import utils_etl
from sqlalchemy.engine import Engine

#Se leen así: humanResources["Shift"]
def extractHumanResources(conection: Engine):
    tablas = [
        "Shift", "Department", "Employee", "EmployeeDepartmentHistory", "EmployeePayHistory"
    ]
    humanResources = {}
    for tabla in tablas:
        df = utils_etl.cargaSegura(conection, "HumanResources", tabla)
        humanResources[tabla] = df
        
    return humanResources

def extractPerson(conection: Engine):
    tablas = [
        "PersonPhone", "PhoneNumberType", "Address", "AddressType",
        "StateProvince", "BusinessEntity", "BusinessEntityAddress", "BusinessEntityContact",
        "ContactType", "CountryRegion", "EmailAddress", "Password", "Person"
    ]
    person = {}
    for tabla in tablas:
        df = utils_etl.cargaSegura(conection, "Person", tabla)
        person[tabla] = df
        
    return person

def extractProduction(conection: Engine):
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
        df = utils_etl.cargaSegura(conection, "Production", tabla)
        production[tabla] = df
        
    return production

def extractPurchasing(conection: Engine):
    tablas = [
        "ShipMethod", "ProductVendor", "Vendor", "PurchaseOrderDetail", "PurchaseOrderHeader"
    ]
    purchasing = {}
    for tabla in tablas:
        df = utils_etl.cargaSegura(conection, "Purchasing", tabla)
        purchasing[tabla] = df
        
    return purchasing

def extractSales(conection: Engine):
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
        df = utils_etl.cargaSegura(conection, "Sales", tabla)
        sales[tabla] = df
        
    return sales



    
