# DMARC Ingesta

Proyecto diseñado para el procesamiento automatizado, consulta y gestión de reportes DMARC utilizando Python y bases de datos SQL.

## 📋 Descripción de los scripts

- `process.py`: Script principal. Procesa correos electrónicos nuevos que contienen reportes DMARC, los analiza y los almacena en la base de datos.
- `consultar.py`: Herramienta para ejecutar queries personalizadas y obtener estadísticas de los dominios procesados.
- `ingestar_prueba.py`: Script auxiliar para la carga de datos de prueba y validación de esquemas.
- `migrar.py`: Gestión de migraciones de la base de datos y actualización de esquemas.

## 🚀 Requisitos

- Python 3.x
- `psycopg` (para conexión a PostgreSQL)
- `parsedmarc` (para el parseo de reportes XML)
- Archivo `.env` configurado con `DATABASE_URL`

## 🛠️ Instalación

1. Clona el repositorio:
   ```bash
   git clone [https://github.com/Yafreilis/Dmarc.git](https://github.com/Yafreilis/Dmarc.git)
