# Aplicación de Reportes

Una aplicación de escritorio para gestionar reportes usando PyQt6, con carga de archivos Excel y conexión futura a SQL Server.

## Requisitos

- Python 3.8+
- PyQt6
- pandas
- openpyxl

## Instalación

1. Clona o descarga el proyecto.
2. Instala las dependencias:
   ```
   pip install -r requirements.txt
   ```

## Ejecución

Activa el entorno virtual:
```
.\.venv\Scripts\Activate.ps1
```

Luego ejecuta:
```
python main.py
```

O directamente con la ruta completa:
```
& 'C:/1. PROYECTOS/Reportes/.venv/Scripts/python.exe' main.py
```

## Configuración de conexión SQL (opcional)

Si cambia la IP/puerto del servidor SQL, puedes sobreescribirlos sin editar código usando variables de entorno en PowerShell:

```
$env:SQL_SIG_SERVER="192.168.43.14"
$env:SQL_SIG_PORT="1433"
$env:SQL_SIG_DATABASE="SIG_PRO_CASACROSS_09-08-2021"
$env:SQL_SIG_UID="victor_compras"
$env:SQL_SIG_PWD="C0mpras2025!"

$env:SQL_CENTRO_SERVER="192.168.43.75"
$env:SQL_CENTRO_PORT="1433"
$env:SQL_CENTRO_DATABASE="CentroDistribucion"

$env:SQL_COMPRAS_SERVER="192.168.43.75"
$env:SQL_COMPRAS_PORT="1433"
$env:SQL_COMPRAS_DATABASE="ComprasInternacionales"
```

Luego ejecuta normalmente:

```
python main.py
```

## Características

- Carga de archivos Excel con columnas específicas (CodigoAlterno, CodigoOriginal, Descripcion, Marca, Cantidad, Bodega, Costo Cordobas, Factor, Precio Full Cordobas, Precio Full+IVA Cordobas, Descuento, Costo Dolares, Precio Full Dolares, Precio Full+IVA Dolares, Descuento).
- Columnas obligatorias: CodigoAlterno, Cantidad, Bodega.
- Tabla personalizable con filas alternas, selección por filas y redimensionamiento automático.
- Exportación de la tabla procesada a Excel o CSV.
- Segunda pestaña para consultar movimientos del Kardex ejecutando `spReporteMovimientoGeneralKardexWEB`, precargando el día anterior y mostrando los orígenes con nombres legibles.
- Inserción directa de los movimientos listados en `CentroDistribucion.dbo.KardexMovimientoEntreBodegas` con un botón dedicado.
- Tercera pestaña para consultar el reporte general de ventas (procedimiento `spReporteGeneralVentasWEB`), con selector de empresa, rango de fechas personalizable y precarga automática del día anterior, además de inserción directa en `CentroDistribucion.dbo.ReporteVentasWeb`.
- Archivo de log `app.log` (en la raíz del proyecto) que registra cada operación, útil para depurar errores en base de datos.
- Próximamente: Conexión a SQL Server para completar datos adicionales.