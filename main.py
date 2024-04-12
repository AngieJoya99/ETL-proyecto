import pandas as pd
from sqlalchemy import create_engine
import yaml

with open('config.yml', 'r') as f:
    config_co = yaml.safe_load(f)['CO_SA']
    config_etl = yaml.safe_load(f)['ETL_PRO']

# Construct the database URL
url_co = (f"{config_co['drivername']}://{config_co['user']}:{config_co['password']}@{config_co['host']}:"
          f"{config_co['port']}/{config_co['dbname']}")
url_etl = (f"{config_etl['drivername']}://{config_etl['user']}:{config_etl['password']}@{config_etl['host']}:"
           f"{config_etl['port']}/{config_etl['dbname']}")
# Create the SQLAlchemy Engine
co_sa = create_engine(url_co)
etl_conn = create_engine(url_etl)


