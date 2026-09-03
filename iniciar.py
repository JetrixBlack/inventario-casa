"""
Script de inicio de Inventario Casa
Ejecuta el servidor FastAPI con Uvicorn en el puerto 8080 y abre automáticamente el navegador.
"""

import os
import sys
import webbrowser
import threading
import time

PORT = 8080

def abrir_navegador():
    time.sleep(1.2)
    webbrowser.open(f"http://localhost:{PORT}")

if __name__ == "__main__":
    # Iniciar hilo para abrir navegador
    threading.Thread(target=abrir_navegador, daemon=True).start()

    import uvicorn
    # Iniciar servidor Uvicorn en puerto 8080
    uvicorn.run("api.index:app", host="127.0.0.1", port=PORT, reload=True)
