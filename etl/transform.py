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
    
    dimCustomer = dimCustomer.merge(customer[customer['PersonID'].notna()], left_on='BusinessEntityID', right_on='PersonID', how='inner')
    dimCustomer = dimCustomer.merge(businessEntityAddress, on='BusinessEntityID', how='left')
    dimCustomer = dimCustomer.merge(direccion, on='AddressID', how='left')
    dimCustomer = dimCustomer.merge(phone, on='BusinessEntityID', how='left')
    dimCustomer = dimCustomer.merge(email, on='BusinessEntityID', how='left')
    
    dimCustomer['CustomerKey'] = range(11000, 11000 + len(dimCustomer))
    dimCustomer['CustomerAlternateKey'] = 'AW' + dimCustomer['CustomerKey'].astype(str).str.zfill(8)
    
    dimCustomer = dimCustomer.drop(columns=['BusinessEntityID', 'Demographics', 'CustomerID', 'PersonID', 'StoreID', 'TerritoryID',
       'AccountNumber', 'ModifiedDate_x', 'AddressID', 'AddressTypeID',
       'rowguid', 'ModifiedDate_y', 'City',
       'StateProvinceID', 'PostalCode', 'ModifiedDate'        
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
        employeeDepartmentHistory[["BusinessEntityID", "DepartmentID"]],
        left_on="EmployeeKey",
        right_on="BusinessEntityID",
        how="left"
    ).merge(
        department[["DepartmentID", "Name"]],
        on="DepartmentID",
        how="left"
    ).rename(columns={"Name": "DepartmentName"}).drop(columns=["BusinessEntityID", "DepartmentID"])


    dimEmployee["SalesPersonFlag"] = np.where(
        (dimEmployee["DepartmentName"].str.contains("Sales", na=False)) &
        (dimEmployee["Title"] != "Vice President of Engineering"),
        1,
        0
    )

    dimEmployee = dimEmployee.merge(
        employeeDepartmentHistory[["BusinessEntityID", "StartDate", "EndDate"]],
        left_on="EmployeeKey",
        right_on="BusinessEntityID",
        how="left"
    ).drop(columns=["BusinessEntityID"])

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
    dimEmployee = dimEmployee.drop_duplicates(subset=["EmployeeKey"])

    
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
def transformDimProduct(tablas):
    dimProduct = pd.DataFrame()
    return dimProduct

#Atributos: ProductCategoryKey, ProductCategoryAlternateKey, EnglishProductCategoryName, SpanishProductCategoryName
# FrenchProductCategoryName
def transformDimProductCategory(tablas):
    dimProductCategory = pd.DataFrame()
    return dimProductCategory

#Atributos: ProductSubcategoryKey, ProductSubcategoryAlternateKey, EnglishProductSubcategoryName, SpanishProductSubcategoryName
# FrenchProductSubcategoryName, ProductCategoryKey
def transformDimProductSubcategory(tablas):
    dimProductSubcategory = pd.DataFrame()
    return dimProductSubcategory

#Atributos: PromotionKey, PromotionAlternateKey, EnglishPromotionName
# SpanishPromotionName, FrenchPromotionName, DiscountPct, EnglishPromotionType
# SpanishPromotionType, FrenchPromotionType, EnglishPromotionCategory, SpanishPromotionCategory
# FrenchPromotionCategory, StartDate, EndDate, MinQty, MaxQty
def transformDimPromotion(tablas):
    dimPromotion = pd.DataFrame()
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
def  transformDimSalesTerritory(SalesTerritory):
    dimSalesTerritory = pd.DataFrame(columns=[
        "SalesTerritoryKey", "SalesTerritoryAlternateKey", "SalesTerritoryRegion", "SalesTerritoryCountry", "SalesTerritoryGroup"
    ])

    dimSalesTerritory["SalesTerritoryKey"] = SalesTerritory["TerritoryID"]
    dimSalesTerritory["SalesTerritoryRegion"] = SalesTerritory["Name"]
    dimSalesTerritory["SalesTerritoryCountry"] = SalesTerritory["CountryRegionCode"]
    dimSalesTerritory["SalesTerritoryGroup"] = SalesTerritory["Group"]

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
def transformFactInternetSales(tablas):
    factInternetSales = pd.DataFrame()
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