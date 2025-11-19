from sqlalchemy import text, inspect
import pandas as pd
from sqlalchemy.engine import Engine
import xml.etree.ElementTree as ET
from deep_translator import GoogleTranslator
import pandas as pd
from concurrent.futures import ThreadPoolExecutor


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

def extraerDemografia(df, xml_col):
    data = []
    
    for xml_str in df[xml_col]:
        try:
            root = ET.fromstring(xml_str)
            row = {child.tag.split('}')[1]: child.text for child in root}
            data.append(row)
        except ET.ParseError:
            # En caso de que haya XML mal formado
            data.append({})
    
    df_parsed = pd.DataFrame(data)
    
    # Columnas numéricas conocidas
    numeric_cols = [
        'TotalPurchaseYTD', 'TotalChildren', 'NumberChildrenAtHome',
        'NumberCarsOwned', 'HomeOwnerFlag'
    ]
    
    for col in numeric_cols:
        if col in df_parsed.columns:
            df_parsed[col] = pd.to_numeric(df_parsed[col], errors='coerce')
    
    # Columnas de fecha conocidas
    date_cols = ['BirthDate', 'DateFirstPurchase']
    
    for col in date_cols:
        if col in df_parsed.columns:
            df_parsed[col] = df_parsed[col].str.replace('Z','', regex=False)  # quitar la Z
            df_parsed[col] = pd.to_datetime(df_parsed[col], errors='coerce', format='%Y-%m-%d')
    
    return df_parsed

def generate_unique_ip(keys, base_ip="198.51"):
    ips = []
    for k in keys:
        # Cada "bloque" de 253 IPs usa el siguiente tercer octeto
        block = (k - 1) // 253
        last_octet = ((k - 1) % 253) + 2
        third_octet = 100 + block  # puedes ajustar 100 a cualquier valor inicial
        ip = f"{base_ip}.{third_octet}.{last_octet}"
        ips.append(ip)
    return ips
 
    
#Traducción description y name para dimProductos 

# CACHES
translation_cache = {}
translation_cache_name = {}

# ------------------------------
# TRADUCCIÓN PARA DESCRIPCIONES
# ------------------------------

def translate_with_cache(text, target):
    if pd.isna(text) or text.strip() == "":
        return None

    key = (text, target)

    if key not in translation_cache:
        translation_cache[key] = GoogleTranslator(
            source='en',
            target=target
        ).translate(text)

    return translation_cache[key]


def translate_row(row):
    eng = row['EnglishDescription']

    if pd.isna(row['GermanDescription']):
        row['GermanDescription'] = translate_with_cache(eng, 'de')

    if pd.isna(row['JapaneseDescription']):
        row['JapaneseDescription'] = translate_with_cache(eng, 'ja')

    if pd.isna(row['TurkishDescription']):
        row['TurkishDescription'] = translate_with_cache(eng, 'tr')

    return row


def translate_missing_fast(df, max_workers=5):
    rows = [row for _, row in df.iterrows()]
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        result = list(exe.map(translate_row, rows))
    return pd.DataFrame(result)


# ------------------------------
# TRADUCCIÓN PARA NOMBRES
# ------------------------------

def translate_with_cache_name(text, target):
    if pd.isna(text) or text.strip() == "":
        return None

    key = (text, target)

    if key not in translation_cache_name:
        translation_cache_name[key] = GoogleTranslator(
            source='en',
            target=target
        ).translate(text)

    return translation_cache_name[key]


def translate_row_name(row):
    eng = row["Name"]

    if pd.isna(row.get("SpanishProductName")):
        row["SpanishProductName"] = translate_with_cache_name(eng, "es")

    if pd.isna(row.get("FrenchProductName")):
        row["FrenchProductName"] = translate_with_cache_name(eng, "fr")

    return row


def translate_missing_fast_name(df, max_workers=5):
    df = df.copy()

    # Garantiza columnas
    for col in ["SpanishProductName", "FrenchProductName"]:
        if col not in df.columns:
            df[col] = None

    rows = df.to_dict(orient="records")

    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        result = list(exe.map(translate_row_name, rows))

    return pd.DataFrame(result)


#Obtener size range para dimProductos

# Función para un solo valor de Size
def size_range_calc(size):
    if pd.isna(size) or str(size).upper() == 'NA':
        return 'NA'
    elif str(size).isalpha():  # letra
        return size
    else:
        try:
            size = int(size)
            if 38 <= size <= 40:
                return '38-40 CM'
            elif 42 <= size <= 46:
                return '42-46 CM'
            elif 48 <= size <= 52:
                return '48-52 CM'
            elif 54 <= size <= 58:
                return '54-58 CM'
            elif 60 <= size <= 62:
                return '60-62 CM'
            else:
                return str(size)
        except ValueError:
            return 'NA'

# Función para generar tabla con ProductID, Size y SizeRange
def generar_size_range_tabla(product):
    df = product.copy()
    df['SizeRange'] = df['Size'].apply(size_range_calc)
    
    return df[['ProductID','Size','SizeRange']]



def extractStoreDemographics(engine):
    query = """
    WITH XMLNAMESPACES (
        'http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey' AS ss
    )
    SELECT 
        s.BusinessEntityID AS BusinessEntityID,
        s.Name AS ResellerName,
        s.SalesPersonID AS StorePersonID,

        s.Demographics.value('(ss:StoreSurvey/ss:YearOpened)[1]', 'int') AS YearOpened,
        s.Demographics.value('(ss:StoreSurvey/ss:AnnualSales)[1]', 'money') AS AnnualSales,
        s.Demographics.value('(ss:StoreSurvey/ss:AnnualRevenue)[1]', 'money') AS AnnualRevenue,
        s.Demographics.value('(ss:StoreSurvey/ss:NumberEmployees)[1]', 'int') AS NumberEmployees,
        s.Demographics.value('(ss:StoreSurvey/ss:BankName)[1]', 'nvarchar(100)') AS BankName,
        s.Demographics.value('(ss:StoreSurvey/ss:BusinessType)[1]', 'nvarchar(20)') AS BusinessType,
        s.Demographics.value('(ss:StoreSurvey/ss:Specialty)[1]', 'nvarchar(50)') AS ProductLine

    FROM Sales.Store s;
    """
    
    return pd.read_sql_query(query, con=engine)







