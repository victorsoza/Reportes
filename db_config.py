import os
import pyodbc
import logging

# Configuración de conexiones a bases de datos
DB_CONNECTIONS = {
    "sig_web": {
        "server": "192.168.43.14",
        "port": "1433",
        "database": "SIG_PRO_CASACROSS_09-08-2021",
        "uid": "victor_compras",
        "pwd": "C0mpras2025!",
        "trusted": False,
        "server_env": "SQL_SIG_SERVER",
        "port_env": "SQL_SIG_PORT",
        "database_env": "SQL_SIG_DATABASE",
        "uid_env": "SQL_SIG_UID",
        "pwd_env": "SQL_SIG_PWD",
    },
    "compras_internacionales": {
        "server": "192.168.42.34",
        "port": "1433",
        "database": "ComprasInternacionales",
        "trusted": True,
        "server_env": "SQL_COMPRAS_SERVER",
        "port_env": "SQL_COMPRAS_PORT",
        "database_env": "SQL_COMPRAS_DATABASE",
    },
    "centro_distribucion": {
        "server": "192.168.42.34",
        "port": "1433",
        "database": "CentroDistribucion",
        "trusted": True,
        "server_env": "SQL_CENTRO_SERVER",
        "port_env": "SQL_CENTRO_PORT",
        "database_env": "SQL_CENTRO_DATABASE",
    },
}

def build_connection_string(key: str) -> str:
    config = DB_CONNECTIONS[key]
    server = os.getenv(config["server_env"], config["server"]).strip()
    port = os.getenv(config.get("port_env", ""), config.get("port", "")).strip()
    database = os.getenv(config["database_env"], config["database"]).strip()

    if not server:
        raise ValueError(f"No se definió servidor para la conexión '{key}'.")

    if server.lower().startswith("tcp:"):
        server_target = server
    elif "\\" in server or "," in server:
        server_target = server
    elif port:
        server_target = f"tcp:{server},{port}"
    else:
        server_target = f"tcp:{server}"

    conn_parts = [
        "DRIVER={ODBC Driver 17 for SQL Server}",
        f"SERVER={server_target}",
        f"DATABASE={database}",
        "TrustServerCertificate=yes",
        "Encrypt=no",
        "Connection Timeout=5",
    ]

    if config.get("trusted", True):
        conn_parts.append("Trusted_Connection=yes")
    else:
        uid = os.getenv(config["uid_env"], config["uid"])
        pwd = os.getenv(config["pwd_env"], config["pwd"])
        conn_parts.append(f"UID={uid}")
        conn_parts.append(f"PWD={pwd}")

    return ";".join(conn_parts) + ";"

def connect_db(key: str):
    conn_str = build_connection_string(key)
    return pyodbc.connect(conn_str)
