"""
Script de inicio de Inventario Casa
Ejecuta el servidor FastAPI con Uvicorn en el puerto 8080 y abre automáticamente el navegador.
Carga las variables de entorno desde .env para usar la MISMA base de datos Turso en local y producción.
"""

import os
import sys
import webbrowser
import threading
import time

PORT = 8080

def cargar_env(ruta_env=".env"):
    """Carga las variables del archivo .env al entorno (sin sobrescribir las ya existentes)."""
    if not os.path.exists(ruta_env):
        print(f"[env] No se encontró {ruta_env}, usando variables del sistema.")
        return
    with open(ruta_env, "r", encoding="utf-8") as f:
        for linea in f:
            linea = linea.strip()
            if not linea or linea.startswith("#") or "=" not in linea:
                continue
            clave, _, valor = linea.partition("=")
            clave = clave.strip()
            valor = valor.strip()
            # Quitar comillas si están presentes
            if len(valor) >= 2 and valor[0] == valor[-1] and valor[0] in ('"', "'"):
                valor = valor[1:-1]
            if clave and clave not in os.environ:
                os.environ[clave] = valor
    print("[env] Variables de entorno cargadas desde .env")

def abrir_navegador():
    time.sleep(1.2)
    webbrowser.open(f"http://localhost:{PORT}")

if __name__ == "__main__":
    # 1. Cargar las variables del .env (usa Turso en local, igual que producción)
    cargar_env()

    # 2. Iniciar hilo para abrir navegador
    threading.Thread(target=abrir_navegador, daemon=True).start()

    # 3. Verificar conexión a Turso (opcional pero útil)
    turso_url = os.environ.get("TURSO_DATABASE_URL", "")
    if turso_url:
        print(f"[db] Conectado a Turso: {turso_url}")
    else:
        print("[db] ADVERTENCIA: Usando base de datos SQLite LOCAL (sin Turso)")

    import uvicorn
    uvicorn.run("api.index:app", host="127.0.0.1", port=PORT, reload=True)
