## Preparar bases de datos
* Montar la OTLP en SQL server con sus respectivos datos, y crear una base de datos vacía donde se alacenará la OLAP a crear

* La autenticación de SQL server debe ser por medio de usuario y contraseña, no con autenticación de windows

## Instalar entorno virtual y requerimientos
```
python -m venv my_env
pip install -r requirements.txt
pip install psycopg2
pip install pyodbc
```

## Configurar la conexión con la base de datos
* Hacer una copia del archivo config_fill.yml que se llame config.yml

* Llenar los campos con la información específica de la conexión


```
nombre_conexion:
  drivername: "ODBC Driver 17 for SQL Server"
  dbname: #nombre de la base de datos
  user: #usuario
  password: #contraseña
  host: localhost
  port: #puerto, usualmente 1433
```

## Probar la conexión

Ejecutar el notebook pruebaConexion.ipynb y verificar que corra sin errores