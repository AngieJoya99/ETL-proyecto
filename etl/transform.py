import datetime
from datetime import timedelta, date, datetime
from typing import Tuple, Any

import holidays
import numpy as np
import pandas as pd
from mlxtend.frequent_patterns import apriori
from mlxtend.preprocessing import TransactionEncoder
from pandas import DataFrame

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
def transformDimCustomer(tablas):
    dimCustomer = pd.DataFrame()
        
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
def transformDimDate(tablas):
    dimDate = pd.DataFrame()
    return dimDate

#Atributos: EmployeeKey, ParentEmployeeKey, EmployeeNationalIDAlternateKey, ParentEmployeeNationalIDAlternateKey
# SalesTerritoryKey, FirstName, LastName, MiddleName
# NameStyle, Title, HireDate, BirthDate
# LoginID, EmailAddress, Phone, MaritalStatus
# EmergencyContactName, EmergencyContactPhone, SalariedFlag, Gender
# PayFrequency, BaseRate, VacationHours, SickLeaveHours
# CurrentFlag, SalesPersonFlag, DepartmentName, StartDate
# EndDate, Status, EmployeePhoto
def transformDimEmployee(tablas):
    dimEmployee = pd.DataFrame()
    return dimEmployee

#Atributos: GeographyKey, City, StateProvinceCode, StateProvinceName
# CountryRegionCode, EnglishCountryRegionName, SpanishCountryRegionName, FrenchCountryRegionName
# PostalCode, SalesTerritoryKey, IpAddressLocator
def transformDimGeography(tablas):
    dimGeography = pd.DataFrame()
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
def transformDimSalesReason(tablas):
    dimSalesReason = pd.DataFrame()
    return dimSalesReason

#Atributos: SalesTerritoryKey, SalesTerritoryAlternateKey, SalesTerritoryRegion, SalesTerritoryCountry
# SalesTerritoryGroup, SalesTerritoryImage
def transformDimSalesTerritory(tablas):
    dimSalesTerritory = pd.DataFrame()
    return dimSalesTerritory

#Atributos: ProductKey, CultureName, ProductDescription
def transformFactAdditionalInternationalProductDescription(tablas):
    factAdditionalInternationalProductDescription = pd.DataFrame()
    return factAdditionalInternationalProductDescription

#Atributos: CurrencyKey, DateKey, AverageRate, EndOfDayRate
# Date
def transformFactCurrencyRate(tablas):
    factCurrencyRate = pd.DataFrame()
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
def transformFactInternetSalesReason(tablas):
    factInternetSalesReason = pd.DataFrame()
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
def transformNewFactCurrencyRate(tablas):
    newFactCurrencyRate = pd.DataFrame()
    return newFactCurrencyRate
