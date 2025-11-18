import datetime
from datetime import timedelta, date, datetime
from typing import Tuple, Any

import holidays
import numpy as np
import pandas as pd
from mlxtend.frequent_patterns import apriori
from mlxtend.preprocessing import TransactionEncoder
from pandas import DataFrame
import utils_etl
import utils_etl
import utils_etl

#Las dimensiones a crear son: DimCurrency, DimCustomer, DimDate, DimEmployee, DimGeography, DimProduct,
#DimProductCategory, DimProductSubcategory, DimPromotion, DimReseller, DimSalesReason, DimSalesTerritory

#Los hechos a crear son: FactAdditionalInternationalProductDescription, FactCurrencyRate, 
#FactInternetSales, FactInternetSalesReason, FactResellerSales, NewFactCurrencyRate

#################################################

#Atributos: CurrencyKey, CurrencyAlternateKey, CurrencyName
def transformDimCurrency(currency):
    dimCurrency = pd.DataFrame(columns=[
        "CurrencyKey", "CurrencyAlternateKey", "CurrencyName"
    ])
    
    dimCurrency["CurrencyAlternateKey"] = currency["CurrencyCode"] 
    dimCurrency["CurrencyName"] = currency["Name"] 
    dimCurrency["CurrencyKey"] = range(1, len(dimCurrency) + 1)
    
    return dimCurrency

#Atributos: CustomerKey, GeographyKey, CustomerAlternateKey, Title, FirstName, MiddleName
#LastName, NameStyle, BirthDate, MaritalStatus, Suffix, Gender, EmailAddress, YearlyIncome
#TotalChildren, NumberChildrenAtHome, EnglishEducation, SpanishEducation, FrenchEducation, 
#EnglishOccupation, SpanishOccupation, FrenchOccupation, HouseOwnerFlag, NumberCarsOwned, 
# AddressLine1, AddressLine2, Phone, DateFirstPurchase, CommuteDistance
def transformDimCustomer(person, sales):
    #Tipos IN = Individual Customer
    dimCustomer = person["Person"][person["Person"]["PersonType"] == 'IN'].copy()
    dimCustomer = dimCustomer.drop(columns=[
        'PersonType', 'EmailPromotion', 'AdditionalContactInfo', 'ModifiedDate', 'rowguid'
    ])
    
    demografia = utils_etl.extraerDemografia(dimCustomer,"Demographics").drop(columns=[
        'TotalPurchaseYTD'
    ])
    demografia = demografia.rename(columns={
        'Education': 'EnglishEducation',
        'Occupation': 'EnglishOccupation',
    })
    
    #Añadir español y francés
    education_map = {
        "Bachelors": {"Spanish": "Licenciatura", "French": "Bac + 4"},
        "Graduate Degree": {"Spanish": "Estudios de postgrado", "French": "Bac + 3"},
        "High School": {"Spanish": "Educación secundaria", "French": "Bac + 2"},
        "Partial College": {"Spanish": "Estudios universitarios (en curso)", "French": "Baccalauréat"},
        "Partial High School": {"Spanish": "Educación secundaria (en curso)", "French": "Niveau bac"}
    }
    occupation_map = {
        "Clerical": {"Spanish": "Administrativo", "French": "Employé"},
        "Management": {"Spanish": "Gestión", "French": "Direction"},
        "Manual": {"Spanish": "Obrero", "French": "Ouvrier"},
        "Professional": {"Spanish": "Profesional", "French": "Cadre"},
        "Skilled Manual": {"Spanish": "Obrero especializado", "French": "Technicien"}
    }
    demografia["EnglishEducation"] = demografia["EnglishEducation"].str.strip()
    demografia["EnglishOccupation"] = demografia["EnglishOccupation"].str.strip()

    demografia["SpanishEducation"] = demografia["EnglishEducation"].map(lambda x: education_map[x]["Spanish"])
    demografia["FrenchEducation"] = demografia["EnglishEducation"].map(lambda x: education_map[x]["French"])
    
    demografia["SpanishOccupation"] = demografia["EnglishOccupation"].map(lambda x: occupation_map[x]["Spanish"])
    demografia["FrenchOccupation"] = demografia["EnglishOccupation"].map(lambda x: occupation_map[x]["French"])
    
    dimCustomer = pd.concat([dimCustomer, demografia], axis=1)
    
    businessEntityAddress = person["BusinessEntityAddress"]
    direccion = person["Address"].drop(columns=['rowguid'])
    customer = sales["Customer"].drop(columns=['rowguid'])
    phone = person["PersonPhone"].drop(columns=['ModifiedDate', 'PhoneNumberTypeID'])
    email = person["EmailAddress"].drop(columns=['EmailAddressID', 'rowguid', 'ModifiedDate'])
    
    dimCustomer = dimCustomer.merge(customer[customer['PersonID'].notna()], left_on='BusinessEntityID', right_on='PersonID', how='inner').drop(columns=['PersonID'])
    dimCustomer = dimCustomer.merge(businessEntityAddress, on='BusinessEntityID', how='left')
    dimCustomer = dimCustomer.merge(direccion, on='AddressID', how='left')
    dimCustomer = dimCustomer.merge(phone, on='BusinessEntityID', how='left')
    dimCustomer = dimCustomer.merge(email, on='BusinessEntityID', how='left')

    dimCustomer['CustomerKey'] = range(11000, 11000 + len(dimCustomer))
    dimCustomer = dimCustomer.merge(
        customer[customer['PersonID'].notna()][['PersonID', 'AccountNumber']],
        left_on='BusinessEntityID',
        right_on='PersonID',
        how='left'
    ).rename(columns={'AccountNumber_y': 'CustomerAlternateKey'})

    
    dimCustomer = dimCustomer.drop(columns=['BusinessEntityID', 'Demographics', 'CustomerID', 'StoreID', 'TerritoryID', 
       'ModifiedDate_x', 'AddressID', 'AddressTypeID', 'PersonID',
       'rowguid', 'ModifiedDate_y', 'City', 'AccountNumber_x',
       'StateProvinceID', 'ModifiedDate',       
    ])
    
    return dimCustomer

#Atributos: DateKey, FullDateAlternateKey, DayNumberOfWeek,
# EnglishDayNameOfWeek, SpanishDayNameOfWeek,
# FrenchDayNameOfWeek, DayNumberOfMonth,
# DayNumberOfYear, WeekNumberOfYear,
# EnglishMonthName, SpanishMonthName,
# FrenchMonthName, MonthNumberOfYear,
# CalendarQuarter, CalendarYear,
# CalendarSemester, FiscalQuarter,
# FiscalYear, FiscalSemester
def transformDimDate():
    fechaInicio = "2005-01-01"
    fechaFin = "2014-12-31"
    fechas = pd.date_range(start=fechaInicio, end=fechaFin, freq='D')
    weekNumberOfYear = fechas.isocalendar().week
    weekNumberOfYear = weekNumberOfYear.where(weekNumberOfYear <= 52, 1)
    
    dimDate = pd.DataFrame({
        "DateKey": fechas.strftime("%Y%m%d").astype(int),
        "FullDateAlternateKey": fechas.strftime("%Y-%m-%d"),
        "DayNumberOfWeek": fechas.weekday + 1,
        "EnglishDayNameOfWeek": fechas.day_name(),
        "SpanishDayNameOfWeek": fechas.day_name().map({
            "Monday":"Lunes","Tuesday":"Martes","Wednesday":"Miércoles",
            "Thursday":"Jueves","Friday":"Viernes","Saturday":"Sábado","Sunday":"Domingo"
        }),
        "FrenchDayNameOfWeek": fechas.day_name().map({
            "Monday":"Lundi","Tuesday":"Mardi","Wednesday":"Mercredi",
            "Thursday":"Jeudi","Friday":"Vendredi","Saturday":"Samedi","Sunday":"Dimanche"
        }),
        "DayNumberOfMonth": fechas.day,
        "DayNumberOfYear": fechas.dayofyear,
        "WeekNumberOfYear": weekNumberOfYear,
        "EnglishMonthName": fechas.month_name(),
        "SpanishMonthName": fechas.month_name().map({
            "January":"Enero","February":"Febrero","March":"Marzo","April":"Abril",
            "May":"Mayo","June":"Junio","July":"Julio","August":"Agosto",
            "September":"Septiembre","October":"Octubre","November":"Noviembre","December":"Diciembre"
        }),
        "FrenchMonthName": fechas.month_name().map({
            "January":"Janvier","February":"Février","March":"Mars","April":"Avril",
            "May":"Mai","June":"Juin","July":"Juillet","August":"Août",
            "September":"Septembre","October":"Octobre","November":"Novembre","December":"Décembre"
        }),
        "MonthNumberOfYear": fechas.month,
        "CalendarQuarter": fechas.quarter,
        "CalendarYear": fechas.year,
        "CalendarSemester": ((fechas.month - 1)//6 + 1),
        "FiscalQuarter": fechas.quarter,
        "FiscalYear": fechas.year,
        "FiscalSemester": ((fechas.month - 1)//6 + 1)
    })

    return dimDate

#Atributos: EmployeeKey, ParentEmployeeKey, EmployeeNationalIDAlternateKey, ParentEmployeeNationalIDAlternateKey
# SalesTerritoryKey, FirstName, LastName, MiddleName
# NameStyle, Title, HireDate, BirthDate
# LoginID, EmailAddress, Phone, MaritalStatus
# EmergencyContactName, EmergencyContactPhone, SalariedFlag, Gender
# PayFrequency, BaseRate, VacationHours, SickLeaveHours
# CurrentFlag, SalesPersonFlag, DepartmentName, StartDate
# EndDate, Status, EmployeePhoto
def transformDimEmployee(employee, employeePayHistory, employeeDepartmentHistory, department, salesPerson, person, emailAddress, personPhone, hierarchy):
    dimEmployee = pd.DataFrame(columns=[
        "EmployeeKey", "EmployeeNationalIDAlternateKey", "Title", "HireDate", "BirthDate", "LoginID",
        "MaritalStatus", "SalariedFlag", "Gender",
        "VacationHours", "SickLeaveHours", "CurrentFlag", "SalesPersonFlag", "Status"
    ])

    employeePayHistory = (
        employeePayHistory.sort_values("RateChangeDate")
        .groupby("BusinessEntityID")
        .tail(1)
    )
    
    dimEmployee["EmployeeKey"] = employee["BusinessEntityID"]
    dimEmployee["EmployeeNationalIDAlternateKey"] = employee["NationalIDNumber"]
    dimEmployee["Title"] = employee["JobTitle"]
    dimEmployee["HireDate"] = employee["HireDate"]
    dimEmployee["BirthDate"] = employee["BirthDate"]
    dimEmployee["LoginID"] = employee["LoginID"]
    dimEmployee["MaritalStatus"] = employee["MaritalStatus"]
    dimEmployee["SalariedFlag"] = employee["SalariedFlag"].astype(int)
    dimEmployee["Gender"] = employee["Gender"]
    dimEmployee["VacationHours"] = employee["VacationHours"]
    dimEmployee["SickLeaveHours"] = employee["SickLeaveHours"]
    dimEmployee["CurrentFlag"] = employee["CurrentFlag"].astype(int)

    dimEmployee = dimEmployee.merge(
        hierarchy[["EmployeeID", "ParentEmployeeKey", "ParentEmployeeNationalIDAlternateKey"]],
        left_on="EmployeeKey",
        right_on="EmployeeID",
        how="left"
    ).drop(columns=["EmployeeID"])

    dimEmployee = dimEmployee.merge(
        salesPerson[["BusinessEntityID", "TerritoryID"]],
        left_on="EmployeeKey",
        right_on="BusinessEntityID",
        how="left"
    ).drop(columns=["BusinessEntityID"]) \
     .rename(columns={"TerritoryID": "SalesTerritoryKey"})
    
    dimEmployee["SalesTerritoryKey"] = dimEmployee["SalesTerritoryKey"].fillna(11)

    dimEmployee = dimEmployee.merge(
        person[["BusinessEntityID", "FirstName", "LastName", "MiddleName", "NameStyle"]],
        left_on="EmployeeKey",
        right_on="BusinessEntityID",
        how="left"
    ).drop(columns=["BusinessEntityID"])

    dimEmployee["NameStyle"] = dimEmployee["NameStyle"].astype(int)

    dimEmployee = dimEmployee.merge(
        emailAddress[["BusinessEntityID", "EmailAddress"]],
        left_on="EmployeeKey",
        right_on="BusinessEntityID",
        how="left"
    ).drop(columns=["BusinessEntityID"])

    dimEmployee = dimEmployee.merge( 
        personPhone[["BusinessEntityID", "PhoneNumber"]], 
        left_on="EmployeeKey", 
        right_on="BusinessEntityID", 
        how="left" 
    ).drop(columns=["BusinessEntityID"]) \
     .rename(columns={"PhoneNumber": "Phone"})
    
    dimEmployee = dimEmployee.merge(
        employeePayHistory[["BusinessEntityID", "PayFrequency", "Rate"]],
        left_on="EmployeeKey",
        right_on="BusinessEntityID",
        how="left"
    ).drop(columns=["BusinessEntityID"]) \
     .rename(columns={"Rate": "BaseRate"})

    
    dimEmployee = dimEmployee.merge(
        employeeDepartmentHistory[["BusinessEntityID", "DepartmentID", "StartDate", "EndDate"]],
        left_on="EmployeeKey",
        right_on="BusinessEntityID",
        how="left"
    ).merge(
        department[["DepartmentID", "Name"]],
        left_on="DepartmentID",
        right_on="DepartmentID",
        how="left"
    ).rename(columns={"Name": "DepartmentName"}).drop(columns=["BusinessEntityID", "DepartmentID"])


    dimEmployee["SalesPersonFlag"] = np.where(
        (dimEmployee["DepartmentName"].str.contains("Sales", na=False)) &
        (dimEmployee["Title"] != "Vice President of Engineering"),
        1,
        0
    )

    dimEmployee["Status"] = np.where(
        dimEmployee["EndDate"].isna(),
        "Current",
        None
    )

    column_order = [
        "EmployeeKey", "ParentEmployeeKey", "EmployeeNationalIDAlternateKey", "ParentEmployeeNationalIDAlternateKey", 
        "SalesTerritoryKey", "FirstName", "LastName", "MiddleName", "NameStyle", "Title", "HireDate", "BirthDate", 
        "LoginID", "EmailAddress", "Phone", "MaritalStatus", "SalariedFlag", "Gender", "PayFrequency", "BaseRate", 
        "VacationHours", "SickLeaveHours", "CurrentFlag", "SalesPersonFlag", "DepartmentName", "StartDate", "EndDate","Status"
    ]

    dimEmployee = dimEmployee[column_order]
    dimEmployee["EmployeeKey"] = range(1, len(dimEmployee) + 1)
    lookup = dimEmployee.set_index("EmployeeNationalIDAlternateKey")["EmployeeKey"].to_dict()
    dimEmployee["ParentEmployeeKey"] = dimEmployee["ParentEmployeeNationalIDAlternateKey"].map(lookup)
    
    
    return dimEmployee

#Atributos: GeographyKey, City, StateProvinceCode, StateProvinceName
# CountryRegionCode, EnglishCountryRegionName, SpanishCountryRegionName, FrenchCountryRegionName
# PostalCode, SalesTerritoryKey, IpAddressLocator
def transformDimGeography(sales, person):
    dimGeography = sales["SalesTerritory"].drop(columns=[
        'SalesYTD', 'CostYTD', 'CostLastYear', 'rowguid', 'ModifiedDate', 'SalesLastYear',
        'Group', 'Name'
    ]).drop_duplicates()
    
    countryNameMap = {
        "US" : "United States", "CA" : "Canada", "FR" : "France", "DE" : "Germany",
        "AU" : "Australia", "GB" : "United Kingdom"
    }
    
    countryMap = {"Australia": {"Spanish": "Australia", "French": "Australie"},
    "Canada": {"Spanish": "Canada", "French": "Canada"},
    "Germany": {"Spanish": "Alemania", "French": "Allemagne"},
    "France": {"Spanish": "Francia", "French": "France"},
    "United Kingdom": {"Spanish": "Reino Unido", "French": "Royaume-Uni"},
    "United States": {"Spanish": "Estados Unidos", "French": "États-Unis"}}
    
    dimGeography["EnglishCountryRegionName"] = dimGeography["CountryRegionCode"].map(lambda x: countryNameMap[x])
    dimGeography["SpanishCountryRegionName"] = dimGeography["EnglishCountryRegionName"].map(lambda x: countryMap[x]["Spanish"])
    dimGeography["FrenchCountryRegionName"] = dimGeography["EnglishCountryRegionName"].map(lambda x: countryMap[x]["French"])
    
    province = person["StateProvince"].drop(columns=[
        'CountryRegionCode', 'IsOnlyStateProvinceFlag', 'rowguid', 'ModifiedDate'
    ]).drop_duplicates()
    
    dimGeography = dimGeography.merge(province, on='TerritoryID', how='right')
    dimGeography = dimGeography.rename(columns={'Name':'StateProvinceName'})
    
    city = person["Address"].drop(columns=[
        'AddressID', 'AddressLine1', 'AddressLine2', 'rowguid', 'ModifiedDate'
    ]).drop_duplicates()
    
    dimGeography = dimGeography.merge(city, on='StateProvinceID', how='right')
    dimGeography = dimGeography.rename(columns={'TerritoryID':'SalesTerritoryKey'})
    dimGeography["GeographyKey"] = range(1, len(dimGeography) + 1)
    dimGeography["IpAddressLocator"] = utils_etl.generate_unique_ip(dimGeography["GeographyKey"])
    dimGeography = dimGeography.drop(columns=['StateProvinceID'])
    return dimGeography

#Atributos: ProductKey, ProductAlternateKey, ProductSubcategoryKey, WeightUnitMeasureCode
# SizeUnitMeasureCode, EnglishProductName, SpanishProductName, FrenchProductName
# StandardCost, FinishedGoodsFlag, Color, SafetyStockLevel
# ReorderPoint, ListPrice, Size, SizeRange
# Weight, DaysToManufacture, ProductLine, DealerPrice
# Class, Style, ModelName, LargePhoto
# EnglishDescription, FrenchDescription, ChineseDescription, ArabicDescription
# HebrewDescription, ThaiDescription, GermanDescription, JapaneseDescription
# TurkishDescription, StartDate, EndDate, Status


""" 
Funciones para pasar los parametros:

1. Primero obtener de extract las siguientes consultas:

description = extracProductDescription(oltp)
dealerPrices = extractDealerPrices(oltp) 

2. Segundo, realizar las traducciones con las funciones de Utils:
description_translated = translate_missing_fast(description)

A la función name_translated se le pasa una copia del dataframe de productos:

df = production["Product"].copy()
name_translated = translate_missing_fast_name(df)


3. Luego, generar la tabla de size range con la función en Utils:
size_range_df = generar_size_range_tabla(production["Product"])

4. Se llama a la función transformDimProduct de la siguiente manera:
transformDimProduct(production["Product"], description_translated, name_translated, production["ProductModel"], 
                production["ProductListPriceHistory"], dealerPrices= dealerPrices, sizeRange= size_range_df).head()

"""

def transformDimProduct(product, description, name_translated, ProductModel, ProductListPriceHistory, dealerPrices, sizeRange):
    # Crear DataFrame vacío con columnas correctas
    dimProduct = pd.DataFrame(columns=[
        "ProductKey", "ProductAlternateKey", "ProductSubcategoryKey", "WeightUnitMeasureCode", 
        "SizeUnitMeasureCode", "EnglishProductName", "SpanishProductName", "FrenchProductName",
        "StandardCost", "FinishedGoodsFlag", "Color", "SafetyStockLevel", "ReorderPoint",
        "ListPrice", "Size", "Weight", "DaysToManufacture", "ProductLine", "DealerPrice",
        "Class", "Style", "EnglishDescription", "FrenchDescription", "ChineseDescription",
        "ArabicDescription", "HebrewDescription", "ThaiDescription", "GermanDescription",
        "JapaneseDescription", "TurkishDescription",  "Status",
        "ModelName"
    ])
    

    # Asignar valores directos
    dimProduct["ProductKey"] = product["ProductID"]
    dimProduct["ProductAlternateKey"] = product["ProductNumber"]
    dimProduct["ProductSubcategoryKey"] = product["ProductSubcategoryID"]
    dimProduct["WeightUnitMeasureCode"] = product["WeightUnitMeasureCode"]
    dimProduct["SizeUnitMeasureCode"] = product["SizeUnitMeasureCode"]
    dimProduct["EnglishProductName"] = product["Name"]
    dimProduct["StandardCost"] = product["StandardCost"]
    dimProduct["FinishedGoodsFlag"] = product["FinishedGoodsFlag"].fillna(0).astype(int)
    dimProduct["Color"] = product["Color"]
    dimProduct["SafetyStockLevel"] = product["SafetyStockLevel"]
    dimProduct["ReorderPoint"] = product["ReorderPoint"]
    dimProduct["ListPrice"] = product["ListPrice"]
    dimProduct["Size"] = product["Size"]
    dimProduct["Weight"] = product["Weight"]
    dimProduct["DaysToManufacture"] = product["DaysToManufacture"]
    dimProduct["ProductLine"] = product["ProductLine"]
    dimProduct["Class"] = product["Class"]
    dimProduct["Style"] = product["Style"]
    dimProduct["SizeRange"] = dimProduct["ProductKey"].map(
        sizeRange.set_index("ProductID")["SizeRange"]
    )
    
    dealerPrices_unique = dealerPrices.groupby('ProductID')['DealerPrice'].max().reset_index()

    # Asignarlo a dimProduct usando map
    dimProduct["DealerPrice"] = dimProduct["ProductKey"].map(
        dealerPrices_unique.set_index("ProductID")["DealerPrice"]
    )

    # Mapear nombres traducidos
    for lang in ["Spanish", "French"]:
        col = f"{lang}ProductName"
        dimProduct[col] = dimProduct["ProductKey"].map(
            name_translated.set_index("ProductID")[col]
        )

    # Mapear descripciones
    for lang in ["English", "French", "Chinese", "Arabic", "Hebrew", "Thai", "German", "Japanese", "Turkish"]:
        col = f"{lang}Description"
        dimProduct[col] = dimProduct["ProductKey"].map(
            description.set_index("ProductID")[col]
        )

    # Merge con ProductModel
    dimProduct = dimProduct.merge(
        product[['ProductID', 'ProductModelID']],  
        left_on='ProductKey', right_on='ProductID', how='left'
    ).merge(
        ProductModel[['ProductModelID', 'Name']], 
        on='ProductModelID', how='left'
    ).rename(columns={'Name': 'ModelName'}).drop(columns=['ProductID','ProductModelID'])
    
    # Tomar la fecha de inicio mínima por ProductID
    start_dates = ProductListPriceHistory.groupby("ProductID")["StartDate"].min()
    end_dates = ProductListPriceHistory.groupby("ProductID")["EndDate"].max()

    dimProduct["StartDate"] = dimProduct["ProductKey"].map(start_dates).fillna(np.nan)
    dimProduct["EndDate"] = dimProduct["ProductKey"].map(end_dates).fillna(np.nan)



       
    
    dimProduct["Status"] = np.where(
        dimProduct["EndDate"].isna(),
        "Current",
        None
    )
    
    column_order = [
        "ProductKey", "ProductAlternateKey", "ProductSubcategoryKey", "WeightUnitMeasureCode", 
        "SizeUnitMeasureCode", "EnglishProductName", "SpanishProductName", "FrenchProductName",
        "StandardCost", "FinishedGoodsFlag", "Color", "SafetyStockLevel", "ReorderPoint",
        "ListPrice", "Size", "SizeRange","Weight", "DaysToManufacture", "ProductLine", "DealerPrice",
        "Class", "Style", "ModelName", "EnglishDescription", "FrenchDescription", "ChineseDescription",
        "ArabicDescription", "HebrewDescription", "ThaiDescription", "GermanDescription",
        "JapaneseDescription", "TurkishDescription", "StartDate", "EndDate", "Status"
    ]

    dimProduct = dimProduct[column_order]
    dimProduct = dimProduct.drop_duplicates(subset=["ProductKey"])
    
    
    return dimProduct


#Atributos: ProductCategoryKey, ProductCategoryAlternateKey, EnglishProductCategoryName, SpanishProductCategoryName
# FrenchProductCategoryName

def transformDimProductCategory(productCategory):
    dimProductCategory = pd.DataFrame(columns=[ 
        "ProductCategoryKey", "ProductCategoryAlternateKey",
        "EnglishProductCategoryName", "SpanishProductCategoryName", "FrenchProductCategoryName"
    ])

    translationsProductCategory = {
    "Bikes": {
        "es": "Bicicletas",
        "fr": "Vélos"
    },
    "Components": {
        "es": "Componentes",
        "fr": "Composants"
    },
    "Clothing": {
        "es": "Ropa",
        "fr": "Vêtements"
    },
    "Accessories": {
        "es": "Accesorios",
        "fr": "Accessoires"
    }
}

    dimProductCategory["ProductCategoryKey"] = productCategory["ProductCategoryID"]
    dimProductCategory["ProductCategoryAlternateKey"] = productCategory["ProductCategoryID"]
    dimProductCategory["EnglishProductCategoryName"] = productCategory["Name"]
    dimProductCategory["SpanishProductCategoryName"] = dimProductCategory[
        "EnglishProductCategoryName"
    ].apply(lambda x: translationsProductCategory[x]["es"])


    dimProductCategory["FrenchProductCategoryName"] = dimProductCategory[
        "EnglishProductCategoryName"
    ].apply(lambda x: translationsProductCategory[x]["fr"])

    return dimProductCategory

#Atributos: ProductSubcategoryKey, ProductSubcategoryAlternateKey, EnglishProductSubcategoryName, SpanishProductSubcategoryName
# FrenchProductSubcategoryName, ProductCategoryKey
def transformDimProductSubcategory(ProductSubcategory):

    translationsSubcategory = {
    "Mountain Bikes": {"es": "Bicicletas de Montaña", "fr": "Vélos de Montagne"},
    "Road Bikes": {"es": "Bicicletas de Ruta", "fr": "Vélos de Route"},
    "Touring Bikes": {"es": "Bicicletas de Turismo", "fr": "Vélos de Tourisme"},
    "Handlebars": {"es": "Manillares", "fr": "Guidons"},
    "Bottom Brackets": {"es": "Ejes de Centro", "fr": "Boîtiers de Pédalier"},
    "Brakes": {"es": "Frenos", "fr": "Freins"},
    "Chains": {"es": "Cadenas", "fr": "Chaînes"},
    "Cranksets": {"es": "Juego de Bielas", "fr": "Pédaliers"},
    "Derailleurs": {"es": "Desviadores", "fr": "Dérailleurs"},
    "Forks": {"es": "Horquillas", "fr": "Fourches"},
    "Headsets": {"es": "Juegos de Dirección", "fr": "Jeux de Direction"},
    "Mountain Frames": {"es": "Marcos de Montaña", "fr": "Cadres de Montagne"},
    "Pedals": {"es": "Pedales", "fr": "Pédales"},
    "Road Frames": {"es": "Marcos de Ruta", "fr": "Cadres de Route"},
    "Saddles": {"es": "Sillines", "fr": "Selles"},
    "Touring Frames": {"es": "Marcos de Turismo", "fr": "Cadres de Tourisme"},
    "Wheels": {"es": "Ruedas", "fr": "Roues"},
    "Bib-Shorts": {"es": "Pantalones con Tirantes", "fr": "Cuissards à Bretelles"},
    "Caps": {"es": "Gorras", "fr": "Casquettes"},
    "Gloves": {"es": "Guantes", "fr": "Gants"},
    "Jerseys": {"es": "Jerseys", "fr": "Maillots"},
    "Shorts": {"es": "Pantalonetas", "fr": "Shorts"},
    "Socks": {"es": "Medias", "fr": "Chaussettes"},
    "Tights": {"es": "Mallas", "fr": "Collants"},
    "Vests": {"es": "Chalecos", "fr": "Gilets"},
    "Bike Racks": {"es": "Portabicicletas", "fr": "Porte-Vélos"},
    "Bike Stands": {"es": "Soportes para Bicicletas", "fr": "Supports de Vélo"},
    "Bottles and Cages": {"es": "Botellas y Portabotellas", "fr": "Bouteilles et Porte-Bouteilles"},
    "Cleaners": {"es": "Limpiadores", "fr": "Nettoyants"},
    "Fenders": {"es": "Guardabarros", "fr": "Garde-Boue"},
    "Helmets": {"es": "Cascos", "fr": "Casques"},
    "Hydration Packs": {"es": "Mochilas de Hidratación", "fr": "Sacs d'Hydratation"},
    "Lights": {"es": "Luces", "fr": "Lumières"},
    "Locks": {"es": "Candados", "fr": "Antivols"},
    "Panniers": {"es": "Alforjas", "fr": "Sacoches"},
    "Pumps": {"es": "Bombas de Aire", "fr": "Pompes"},
    "Tires and Tubes": {"es": "Llantas y Tubos", "fr": "Pneus et Chambres à Air"}
}

    dimProductSubcategory = pd.DataFrame(columns=[ 
        "ProductSubcategoryKey", "ProductSubcategoryAlternateKey",
        "EnglishProductSubcategoryName", "SpanishProductSubcategoryName", "FrenchProductSubcategoryName",
        "ProductCategoryKey"
    ])

    dimProductSubcategory["ProductSubcategoryKey"] = ProductSubcategory["ProductSubcategoryID"]
    dimProductSubcategory["ProductSubcategoryAlternateKey"] = ProductSubcategory["ProductSubcategoryID"]
    dimProductSubcategory["EnglishProductSubcategoryName"] = ProductSubcategory["Name"]
    dimProductSubcategory["SpanishProductSubcategoryName"] = dimProductSubcategory["EnglishProductSubcategoryName"].apply(
    lambda x: translationsSubcategory[x]["es"]
)
    dimProductSubcategory["FrenchProductSubcategoryName"] = dimProductSubcategory["EnglishProductSubcategoryName"].apply(
    lambda x: translationsSubcategory[x]["fr"]
)
    dimProductSubcategory["ProductCategoryKey"] = ProductSubcategory["ProductCategoryID"]
    
    return dimProductSubcategory

#Atributos: PromotionKey, PromotionAlternateKey, EnglishPromotionName
# SpanishPromotionName, FrenchPromotionName, DiscountPct, EnglishPromotionType
# SpanishPromotionType, FrenchPromotionType, EnglishPromotionCategory, SpanishPromotionCategory
# FrenchPromotionCategory, StartDate, EndDate, MinQty, MaxQty
def transformDimPromotion(specialOffer):
    dimPromotion = pd.DataFrame(columns=[
        "PromotionKey", "PromotionAlternateKey", "EnglishPromotionName", "SpanishPromotionName",
        "FrenchPromotionName", "DiscountPct", "EnglishPromotionType", "SpanishPromotionType",
        "FrenchPromotionType", "EnglishPromotionCategory", "SpanishPromotionCategory",
        "FrenchPromotionCategory", "StartDate", "EndDate", "MinQty", "MaxQty"
    ])

    dimPromotion["PromotionKey"] = specialOffer["SpecialOfferID"]
    dimPromotion["PromotionAlternateKey"] = specialOffer["SpecialOfferID"]
    dimPromotion["EnglishPromotionName"] = specialOffer["Description"]

    translations_es = {
        "No Discount": "Sin descuento",
        "Volume Discount 11 to 14": "Descuento por volumen (entre 11 y 14)",
        "Volume Discount 15 to 24": "Descuento por volumen (entre 15 y 24)",
        "Volume Discount 25 to 40": "Descuento por volumen (entre 25 y 40)",
        "Volume Discount 41 to 60": "Descuento por volumen (entre 41 y 60)",
        "Volume Discount over 60": "Descuento por volumen (más de 60)",
        "Mountain-100 Clearance Sale": "Liquidación de bicicleta de montaña, 100",
        "Sport Helmet Discount-2002": "Casco deportivo, descuento: 2002",
        "Road-650 Overstock": "Bicicleta de carretera: 650, oferta especial",
        "Mountain Tire Sale": "Oferta de cubierta de montaña",
        "Sport Helmet Discount-2003": "Casco deportivo, descuento: 2003",
        "LL Road Frame Sale": "Oferta de cuadro de carretera GB",
        "Touring-3000 Promotion": "Promoción ‘Touring-3000’",
        "Touring-1000 Promotion": "Promoción ‘Touring-1000’",
        "Half-Price Pedal Sale": "Venta de pedales a mitad de precio",
        "Mountain-500 Silver Clearance Sale": "Liquidación de bicicleta de montaña, 500, plateada",
        "Volume Discount": "Descuento por volumen",
        "Discontinued Product": "Descatalogado",
        "Seasonal Discount": "Descuento de temporada",
        "Excess Inventory": "Inventario excedente",
        "New Product": "Producto Nuevo",
        "Reseller": "Distribuidor",
        "Customer": "Cliente"
    }

    translations_fr = {
        "No Discount": "Aucune remise",
        "Volume Discount 11 to 14": "Remise sur quantité (de 11 à 14)",
        "Volume Discount 15 to 24": "Remise sur quantité (de 15 à 24)",
        "Volume Discount 25 to 40": "Remise sur quantité (de 25 à 40)",
        "Volume Discount 41 to 60": "Remise sur quantité (de 41 à 60)",
        "Volume Discount over 60": "Remise sur quantité (au-delà de 60)",
        "Mountain-100 Clearance Sale": "Liquidation VTT 100",
        "Sport Helmet Discount-2002": "Remise sur les casques sport - 2002",
        "Road-650 Overstock": "Déstockage Vélo de route 650",
        "Mountain Tire Sale": "Vente de pneus de VTT",
        "Sport Helmet Discount-2003": "Remise sur les casques sport - 2003",
        "LL Road Frame Sale": "Vente de cadres de vélo de route LL",
        "Touring-3000 Promotion": "Promotion “Touring-3000”",
        "Touring-1000 Promotion": "Promotion “Touring-1000”",
        "Half-Price Pedal Sale": "Pédales à moitié prix",
        "Mountain-500 Silver Clearance Sale": "Liquidation VTT 500 argent",
        "Volume Discount": "Remise sur quantité",
        "Discontinued Product": "Ce produit n'est plus commercialisé",
        "Seasonal Discount": "Remise saisonnière",
        "Excess Inventory": "Déstockage",
        "New Product": "Nouveau produit",
        "Reseller": "Revendeur",
        "Customer": "Client"
    }

    dimPromotion["SpanishPromotionName"] = dimPromotion["EnglishPromotionName"].map(translations_es)
    dimPromotion["FrenchPromotionName"] = dimPromotion["EnglishPromotionName"].map(translations_fr)
    dimPromotion["DiscountPct"] = specialOffer["DiscountPct"]
    dimPromotion["EnglishPromotionType"] = specialOffer["Type"]
    dimPromotion["SpanishPromotionType"] = dimPromotion["EnglishPromotionType"].map(translations_es)
    dimPromotion["FrenchPromotionType"] = dimPromotion["EnglishPromotionType"].map(translations_fr)
    dimPromotion["EnglishPromotionCategory"] = specialOffer["Category"]
    dimPromotion["SpanishPromotionCategory"] = dimPromotion["EnglishPromotionCategory"].map(translations_es)
    dimPromotion["FrenchPromotionCategory"] = dimPromotion["EnglishPromotionCategory"].map(translations_fr)
    dimPromotion["StartDate"] = specialOffer["StartDate"]
    dimPromotion["EndDate"] = specialOffer["EndDate"]
    dimPromotion["MinQty"] = specialOffer["MinQty"]
    dimPromotion["MaxQty"] = specialOffer["MaxQty"]

    return dimPromotion

#Atributos: ResellerKey, GeographyKey, ResellerAlternateKey, Phone
# BusinessType, ResellerName, NumberEmployees, OrderFrequency
# OrderMonth, FirstOrderYear, LastOrderYear, ProductLine
# AddressLine1, AddressLine2, AnnualSales, BankName
# MinPaymentType, MinPaymentAmount, AnnualRevenue, YearOpened
def transformDimReseller(tablas):
    dimReseller = pd.DataFrame()
    return dimReseller

#Atributos: SalesReasonKey, SalesReasonAlternateKey, SalesReasonName, SalesReasonReasonType
def  transformDimSalesReason(SalesReason):
    dimSalesReason = pd.DataFrame(columns=[
        "SalesReasonKey", "SalesReasonAlternateKey", "SalesReasonName", "SalesReasonType"
    ])

    dimSalesReason["SalesReasonKey"] = SalesReason["SalesReasonID"]
    dimSalesReason["SalesReasonAlternateKey"] = SalesReason["SalesReasonID"]
    dimSalesReason["SalesReasonName"] = SalesReason["Name"]
    dimSalesReason["SalesReasonType"] = SalesReason["ReasonType"]

    return dimSalesReason
   

#Atributos: SalesTerritoryKey, SalesTerritoryAlternateKey, SalesTerritoryRegion, SalesTerritoryCountry
# SalesTerritoryGroup, SalesTerritoryImage
def transformDimSalesTerritory(SalesTerritory):
    dimSalesTerritory = pd.DataFrame(columns=[
        "SalesTerritoryKey", "SalesTerritoryAlternateKey", "SalesTerritoryRegion", "SalesTerritoryCountry", "SalesTerritoryGroup"
    ])

    dimSalesTerritory["SalesTerritoryKey"] = SalesTerritory["TerritoryID"]
    dimSalesTerritory["SalesTerritoryRegion"] = SalesTerritory["Name"]
    dimSalesTerritory["SalesTerritoryCountry"] = SalesTerritory["CountryRegionCode"]
    dimSalesTerritory["SalesTerritoryGroup"] = SalesTerritory["Group"]

    dimSalesTerritory.loc[len(dimSalesTerritory)] = [
        11,            # SalesTerritoryKey
        0,          # SalesTerritoryAlternateKey
        "NA",       # SalesTerritoryRegion
        "NA",         # SalesTerritoryCountry
        "NA"        # SalesTerritoryGroup
    ]


    return dimSalesTerritory
#Atributos: CurrencyKey, DateKey, AverageRate, EndOfDayRate
# Date
def transformFactCurrencyRate(sales):
    factCurrencyRate = sales["CurrencyRate"].drop(columns=[
      'FromCurrencyCode', 'CurrencyRateID', 'ModifiedDate'
    ])
    
    factCurrencyRate["DateKey"] = factCurrencyRate["CurrencyRateDate"].dt.strftime("%Y%m%d")
    
    factCurrencyRate.rename(columns={
      'CurrencyRateDate' : 'Date'
    })
    
    return factCurrencyRate

#Atributos: ProductKey, OrderDateKey, DueDateKey, ShipDateKey
# CustomerKey, PromotionKey, CurrencyKey, SalesTerritoryKey
# SalesOrderNumber, SalesOrderLineNumber, RevisionNumber, OrderQuantity
# UnitPrice, ExtendedAmount, UnitPriceDiscountPct, DiscountAmount
# ProductStandardCost, TotalProductCost, SalesAmount, TaxAmt
# Freight, CarrierTrackingNumber, CustomerPONumber, OrderDate
# DueDate, ShipDate
def transformFactInternetSales(product, salesOrderDetail, salesOrderHeader, customer, dimCustomer, dimCurrency, currencyRate, stateProvince, salesTaxRate):
    salesOrderDetail = salesOrderDetail.copy()
    salesOrderDetail["SalesOrderLineNumber"] = (
        salesOrderDetail.groupby("SalesOrderID").cumcount() + 1
    )

    # Start building factInternetSales from salesOrderDetail
    factInternetSales = salesOrderDetail[["ProductID", "SalesOrderID", "SpecialOfferID", 
                                            "SalesOrderLineNumber", "OrderQty", "UnitPrice", 
                                            "UnitPriceDiscount", "LineTotal", "CarrierTrackingNumber"]].rename(
        columns={"ProductID": "ProductKey"}
    )

    # Now merge with salesOrderHeader
    factInternetSales = factInternetSales.merge(
        salesOrderHeader[["SalesOrderID", "SalesOrderNumber", "RevisionNumber", "OrderDate", 
                            "DueDate", "ShipDate", "CustomerID", "TerritoryID", 
                            "Freight", "CurrencyRateID"]],
        on="SalesOrderID",
        how="left"
    ).rename(columns={
        "SpecialOfferID": "PromotionKey", 
        "OrderQty": "OrderQuantity", 
        "UnitPriceDiscount": "UnitPriceDiscountPct", 
        "TerritoryID": "SalesTerritoryKey", 
        "LineTotal": "SalesAmount"
    }).drop(columns=["SalesOrderID"])
    
    # Rest of your code remains the same...
    factInternetSales = factInternetSales.merge(
        customer[["CustomerID", "AccountNumber"]],
        on="CustomerID",
        how="left"
    ).drop(columns=["CustomerID"]).merge(
        dimCustomer[["CustomerAlternateKey", "CustomerKey"]],
        left_on="AccountNumber",
        right_on="CustomerAlternateKey",
        how="left"
    ).drop(columns=["CustomerAlternateKey", "AccountNumber"])
    
    factInternetSales = factInternetSales.merge(
        product[["ProductID", "StandardCost"]],
        left_on="ProductKey",
        right_on="ProductID",
        how="left"
    ).rename(columns={"StandardCost": "ProductStandardCost"}) \
    .drop(columns=["ProductID"])
    
    factInternetSales = factInternetSales.merge(
        currencyRate[["CurrencyRateID", "ToCurrencyCode"]],
        on="CurrencyRateID",
        how="left"
    ).drop(columns=["CurrencyRateID"]).merge(
        dimCurrency[["CurrencyAlternateKey", "CurrencyKey"]],
        left_on="ToCurrencyCode",
        right_on="CurrencyAlternateKey",
        how="left"
    ).drop(columns=["CurrencyAlternateKey", "ToCurrencyCode"])

    factInternetSales = factInternetSales.merge(
        stateProvince[["StateProvinceID", "TerritoryID"]],
        left_on="SalesTerritoryKey",
        right_on="TerritoryID",
        how="left"
    ).drop(columns=["TerritoryID"]).merge(
        salesTaxRate[["StateProvinceID", "TaxRate"]],
        on="StateProvinceID",
        how="left"
    ).drop(columns=["StateProvinceID"])

    def transforma_date(date):
        if pd.isna(date):
            return None
        return int(date.strftime("%Y%m%d"))
    
    factInternetSales["OrderDateKey"] = factInternetSales["OrderDate"].apply(transforma_date).astype("Int64")
    factInternetSales["DueDateKey"] = factInternetSales["DueDate"].apply(transforma_date).astype("Int64")
    factInternetSales["ShipDateKey"] = factInternetSales["ShipDate"].apply(transforma_date).astype("Int64")
    
    factInternetSales["ExtendedAmount"] = factInternetSales["UnitPrice"] * factInternetSales["OrderQuantity"]
    factInternetSales["DiscountAmount"] = factInternetSales["ExtendedAmount"] * factInternetSales["UnitPriceDiscountPct"]
    factInternetSales["TotalProductCost"] = factInternetSales["ProductStandardCost"] * factInternetSales["OrderQuantity"]
    factInternetSales["TaxAmt"] = (factInternetSales["ExtendedAmount"] - factInternetSales["DiscountAmount"]) * (factInternetSales["TaxRate"] / 100)

    factInternetSales = factInternetSales.drop(columns=["TaxRate"])
    

    column_order = ["ProductKey", "OrderDateKey", "DueDateKey", "ShipDateKey", "CustomerKey", "PromotionKey", "CurrencyKey",
        "SalesTerritoryKey", "SalesOrderNumber", "SalesOrderLineNumber", "RevisionNumber", "OrderQuantity", 
        "UnitPrice", "ExtendedAmount", "UnitPriceDiscountPct", "DiscountAmount", "ProductStandardCost", "TotalProductCost",
        "SalesAmount", "TaxAmt", "Freight", "CarrierTrackingNumber", "OrderDate", "DueDate", "ShipDate"]
    
    factInternetSales = factInternetSales[column_order]
    
    return factInternetSales



#Atributos: SalesOrderNumber, SalesOrderLineNumber, SalesReasonKey
def transformFactInternetSalesReason(sales):
    factInternetSalesReason = sales["SalesOrderHeaderSalesReason"].drop(columns=['ModifiedDate'])
    factInternetSalesReason = factInternetSalesReason.rename(columns={'SalesReasonID' : 'SalesReasonKey'})

    orderDetails  = sales["SalesOrderDetail"].drop(columns=[
        'ModifiedDate', 'rowguid', 'UnitPriceDiscount', 'UnitPrice', 'CarrierTrackingNumber',
        'SpecialOfferID', 'OrderQty', 'ProductID', 'LineTotal'
    ])
    orderDetails = orderDetails.sort_values(by=['SalesOrderID', 'SalesOrderDetailID'])
    orderDetails["SalesOrderLineNumber"] =  (
        orderDetails.groupby('SalesOrderID').cumcount() + 1
    )

    factInternetSalesReason = factInternetSalesReason.merge(orderDetails, on='SalesOrderID', how='inner')
    factInternetSalesReason = factInternetSalesReason.drop(columns=['SalesOrderDetailID'])
    factInternetSalesReason["SalesOrderID"] = 'SO' + factInternetSalesReason["SalesOrderID"].astype(str)
    factInternetSalesReason = factInternetSalesReason.rename(columns={'SalesOrderID':'SalesOrderNumber'})

    return factInternetSalesReason

#Atributos: ProductKey, OrderDateKey, DueDateKey
# ShipDateKey, ResellerKey, EmployeeKey, PromotionKey
# CurrencyKey, SalesTerritoryKey, SalesOrderNumber, SalesOrderLineNumber
# RevisionNumber, OrderQuantity, UnitPrice, ExtendedAmount
# UnitPriceDiscountPct, DiscountAmount, ProductStandardCost, TotalProductCost
# SalesAmount, TaxAmt, Freight, CarrierTrackingNumber
# CustomerPONumber, OrderDate, DueDate, ShipDate
def transformFactResellerSales(tablas):
    factResellerSales = pd.DataFrame()
    return factResellerSales

#Atributos: AverageRate, CurrencyID, CurrencyDate, EndOfDayRate
# CurrencyKey, DateKey
def transformNewFactCurrencyRate(sales):
    newFactCurrencyRate = sales["CurrencyRate"].drop(columns=[
        'CurrencyRateID', 'FromCurrencyCode', 'ModifiedDate'
    ])

    newFactCurrencyRate = newFactCurrencyRate.rename(columns={
        'CurrencyRateDate' : 'CurrencyDate', 
        'ToCurrencyCode' : 'CurrencyID'
    })
    
    return newFactCurrencyRate

def fkDimCustomer(dimCustomer, dimGeography):
    llave = dimGeography[["GeographyKey", "PostalCode"]].copy()
    dimCustomer = dimCustomer.merge(llave, on='PostalCode', how='left')
    dimCustomer = dimCustomer.drop(columns=['PostalCode'])
    return dimCustomer

def fkFactCurrencyRate(factCurrencyRate, dimCurrency):
    currency = dimCurrency[["CurrencyKey", "CurrencyAlternateKey"]].copy()
    factCurrencyRate = factCurrencyRate.merge(currency, left_on='ToCurrencyCode', right_on='CurrencyAlternateKey', how='left')
    factCurrencyRate = factCurrencyRate.drop(columns=['ToCurrencyCode', 'CurrencyAlternateKey'])
    
    return factCurrencyRate

def fkNewFactCurrencyRate(newFactCurrencyRate, dimCurrency, dimDate):
    #Añadir CurrencyKey
    currency = dimCurrency[["CurrencyKey", "CurrencyAlternateKey"]].copy()
    newFactCurrencyRate = newFactCurrencyRate.merge(currency, left_on='CurrencyID', right_on='CurrencyAlternateKey', how='left')
    newFactCurrencyRate = newFactCurrencyRate.drop(columns=['CurrencyID', 'CurrencyAlternateKey'])
    
    #Añadir DateKey
    date = dimDate[["DateKey", "FullDateAlternateKey"]].copy()
    newFactCurrencyRate['CurrencyDate'] = pd.to_datetime(newFactCurrencyRate['CurrencyDate'])
    date['FullDateAlternateKey'] = pd.to_datetime(date['FullDateAlternateKey'])
    newFactCurrencyRate = newFactCurrencyRate.merge(date, left_on='CurrencyDate', right_on='FullDateAlternateKey', how='left')
    newFactCurrencyRate = newFactCurrencyRate.drop(columns=['CurrencyDate', 'FullDateAlternateKey'])
    
    return newFactCurrencyRate