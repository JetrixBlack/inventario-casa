"""
Inventario Casa - Backend FastAPI Optimizado (Python Nativo)
Sistema de gestión de inventario, ventas y finanzas familiares.

Características:
  - Base de datos Turso cloud (misma en local y producción)
  - Consultas instantáneas con Row dict-like + foreign keys
  - Tasa BCV en tiempo real con fallback inteligente
"""

import os
import re
import time
import json
import sqlite3
import datetime
from contextlib import contextmanager
from typing import Optional, List, Dict, Any

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Cargar variables de entorno desde .env (local). En producción Vercel inyecta estas vars.
# ---------------------------------------------------------------------------

def _cargar_env_local(ruta_env=None):
    """Carga variables de un archivo .env si no están ya en el entorno."""
    if ruta_env is None:
        ruta_env = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"
        )
    if not os.path.exists(ruta_env):
        return
    with open(ruta_env, "r", encoding="utf-8") as f:
        for linea in f:
            linea = linea.strip()
            if not linea or linea.startswith("#") or "=" not in linea:
                continue
            clave, _, valor = linea.partition("=")
            clave = clave.strip()
            valor = valor.strip()
            if len(valor) >= 2 and valor[0] == valor[-1] and valor[0] in ('"', "'"):
                valor = valor[1:-1]
            if clave and clave not in os.environ:
                os.environ[clave] = valor

_cargar_env_local()


# ---------------------------------------------------------------------------
# Capa de Base de Datos Dual: SQLite local / Turso cloud (Vercel)
# ---------------------------------------------------------------------------

class _Row:
    """Wrapper que permite acceso por índice y por nombre de columna."""
    def __init__(self, columns, values):
        self._columns = columns
        self._values = values
    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        if isinstance(key, str):
            idx = self._columns.index(key)
            return self._values[idx]
        raise TypeError(f"Key must be int or str, got {type(key)}")
    def __contains__(self, key):
        return key in self._columns
    def keys(self):
        return self._columns
    def __iter__(self):
        return iter(self._columns)
    def __len__(self):
        return len(self._values)


class _TursoCursor:
    """Cursor-like wrapper para resultados Turso."""
    def __init__(self, columns, rows, lastrowid=None):
        self._columns = columns
        self._rows = rows
        self.lastrowid = lastrowid
        self.rowcount = len(rows)
    def fetchall(self):
        return [_Row(self._columns, r) for r in self._rows]
    def fetchone(self):
        return _Row(self._columns, self._rows[0]) if self._rows else None


def _turso_value(v):
    """Convierte un valor Python al formato de Value esperado por Turso pipeline."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return {"type": "integer", "value": "1" if v else "0"}
    if isinstance(v, int):
        return {"type": "integer", "value": str(v)}
    if isinstance(v, float):
        return {"type": "float", "value": float(v)}
    return {"type": "text", "value": str(v)}


def _turso_parse_rows(rows):
    """Convierte filas con formato Value de Turso a valores Python planos."""
    out = []
    for row in rows:
        parsed = []
        for cell in row:
            if isinstance(cell, dict):
                vt = cell.get("type")
                vv = cell.get("value")
                if vt == "integer":
                    parsed.append(int(vv))
                elif vt == "float":
                    parsed.append(float(vv))
                elif vt == "text":
                    parsed.append(vv)
                elif vt == "blob":
                    parsed.append(vv)
                elif vt == "null":
                    parsed.append(None)
                else:
                    parsed.append(vv)
            else:
                parsed.append(cell)
        out.append(parsed)
    return out


class _TursoConnection:
    """Conexi��n a Turso v��a REST API (httpx). Compatible con sqlite3 API."""
    def __init__(self, url, token):
        u = url.rstrip("/")
        if u.startswith("libsql://"):
            u = "https://" + u[len("libsql://"):]
        self._url = u
        self._token = token
    def execute(self, sql, params=()):
        args = [_turso_value(p) for p in (params or [])] if params else []
        resp = httpx.post(
            f"{self._url}/v2/pipeline",
            headers={
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
            },
            json={"requests": [{"type": "execute", "stmt": {"sql": sql, "args": args}}]},
            timeout=30.0,
        )
        resp.raise_for_status()
        data = resp.json()
        result = data["results"][0]
        if result.get("type") == "error":
            msg = result.get("error", {}).get("message", "") or result.get("message", "")
            if "UNIQUE" in msg or "constraint" in msg.lower():
                raise Exception(f"IntegrityError: {msg}")
            raise Exception(f"DB Error: {msg}")
        result_map = result.get("result") or result.get("response", {}).get("result", {})
        cols = [c["name"] for c in result_map.get("cols", [])]
        raw_rows = result_map.get("rows", [])
        rows = _turso_parse_rows(raw_rows)
        lastrowid = None
        lir = result_map.get("last_insert_rowid")
        if lir is not None:
            lastrowid = int(lir)
        return _TursoCursor(cols, rows, lastrowid=lastrowid)
    def executemany(self, sql, params_list):
        last = None
        count = 0
        for params in params_list:
            cur = self.execute(sql, params)
            last = cur
            count += cur.rowcount
        return last
    def commit(self):
        pass
    def close(self):
        pass


class _LocalCursor:
    """Cursor wrapper para SQLite local que expone _Row (igual que Turso)."""
    def __init__(self, cursor):
        self._cur = cursor
        self.lastrowid = cursor.lastrowid
        self.rowcount = cursor.rowcount
    def fetchall(self):
        rows = self._cur.fetchall()
        cols = self._cur.description
        names = [c[0] for c in cols] if cols else []
        return [_Row(names, [r[i] for i in range(len(names))]) for r in rows]
    def fetchone(self):
        row = self._cur.fetchone()
        cols = self._cur.description
        names = [c[0] for c in cols] if cols else []
        if row is None:
            return None
        return _Row(names, [row[i] for i in range(len(names))])


class _LocalConnection:
    """Envoltorio para sqlite3 local (desarrollo)."""
    def __init__(self, path):
        self._conn = sqlite3.connect(path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
    def execute(self, sql, params=()):
        return _LocalCursor(self._conn.execute(sql, params))
    def executemany(self, sql, params_list):
        return self._conn.executemany(sql, params_list)
    def commit(self):
        self._conn.commit()
    def rollback(self):
        self._conn.rollback()
    def close(self):
        self._conn.close()


TURSO_URL = os.environ.get("TURSO_DATABASE_URL", "")
TURSO_TOKEN = os.environ.get("TURSO_AUTH_TOKEN", "")


@contextmanager
def get_db():
    """Context manager para conexiones seguras. Turso en Vercel, SQLite local."""
    if TURSO_URL and TURSO_TOKEN:
        conn = _TursoConnection(TURSO_URL, TURSO_TOKEN)
    else:
        conn = _LocalConnection(DB_PATH)
    try:
        yield conn
        conn.commit()
    except Exception:
        if hasattr(conn, 'rollback'):
            conn.rollback()
        raise
    finally:
        conn.close()


def query_all(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    """Ejecuta una consulta SELECT y retorna una lista de diccionarios."""
    with get_db() as conn:
        cur = conn.execute(sql, params)
        return [dict(zip(row._columns, row._values)) for row in cur.fetchall()]


def query_one(sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
    """Ejecuta una consulta SELECT y retorna un diccionario o None."""
    with get_db() as conn:
        cur = conn.execute(sql, params)
        row = cur.fetchone()
        if row is None:
            return None
        return dict(zip(row._columns, row._values))


def execute_sql(sql: str, params: tuple = ()) -> Dict[str, Any]:
    """Ejecuta INSERT, UPDATE o DELETE y retorna metadatos."""
    with get_db() as conn:
        cur = conn.execute(sql, params)
        return {
            "last_insert_rowid": cur.lastrowid,
            "affected_rows": cur.rowcount,
        }

# ---------------------------------------------------------------------------
# Rutas y Constantes
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "inventario_casa.db")
PUBLIC_DIR = os.path.join(BASE_DIR, "public")

app = FastAPI(
    title="Inventario Casa API",
    description="Sistema de gestión de inventario, ventas y finanzas familiares",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Manejo de Base de Datos — funciones auxiliares
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Esquema e Inicialización de Base de Datos
# ---------------------------------------------------------------------------

SCHEMA_SQL = [
    """
    CREATE TABLE IF NOT EXISTS productos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL UNIQUE,
        categoria TEXT NOT NULL,
        precio_compra_actual REAL NOT NULL DEFAULT 0,
        precio_venta_actual REAL NOT NULL DEFAULT 0
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS movimientos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fecha TEXT NOT NULL,
        tipo TEXT NOT NULL CHECK(tipo IN ('Compra', 'Venta')),
        producto_id INTEGER NOT NULL REFERENCES productos(id) ON DELETE RESTRICT,
        cantidad INTEGER NOT NULL CHECK(cantidad > 0),
        precio_unitario_bs REAL NOT NULL DEFAULT 0,
        costo_unitario_bs REAL NOT NULL DEFAULT 0,
        tasa_bcv REAL NOT NULL DEFAULT 0,
        persona_proveedor TEXT NOT NULL DEFAULT '',
        estado_pago TEXT NOT NULL CHECK(estado_pago IN ('Pagado', 'Fiado')),
        notas TEXT NOT NULL DEFAULT ''
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_movimientos_fecha ON movimientos(fecha);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_movimientos_tipo ON movimientos(tipo);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_movimientos_producto ON movimientos(producto_id);
    """
]

# Datos iniciales para siembra si la BD está limpia
PRODUCTOS_INICIALES = [
    ("Cola Negra",  "Refresco", 840.0, 1300.0),
    ("Fresh",       "Refresco", 840.0, 1300.0),
    ("Uva",         "Refresco", 840.0, 1300.0),
    ("Colita",      "Refresco", 840.0, 1300.0),
    ("Polar Light", "Cerveza",  693.9175, 1100.0),
    ("Pilsen",      "Cerveza",  693.9175, 1100.0),
]


def init_database():
    """Inicializa tablas y siembra datos iniciales."""
    with get_db() as conn:
        for stmt in SCHEMA_SQL:
            conn.execute(stmt)

        # Verificar si ya hay datos (solo para Turso, en SQLite siempre funciona)
        if TURSO_URL and TURSO_TOKEN:
            try:
                cur = conn.execute("SELECT COUNT(*) FROM productos")
                row = cur.fetchone()
                prod_count = row[0] if row else 0
            except Exception:
                prod_count = 0
        else:
            cur = conn.execute("SELECT COUNT(*) FROM productos")
            prod_count = cur.fetchone()[0]

        if prod_count == 0:
            conn.executemany(
                "INSERT INTO productos (nombre, categoria, precio_compra_actual, precio_venta_actual) VALUES (?, ?, ?, ?)",
                PRODUCTOS_INICIALES,
            )
            print(f"[OK] {len(PRODUCTOS_INICIALES)} productos iniciales sembrados.")


@app.on_event("startup")
async def on_startup():
    init_database()
    print("[OK] Backend Inventario Casa iniciado exitosamente.")


# ---------------------------------------------------------------------------
# Tasa BCV en tiempo real con fallback
# ---------------------------------------------------------------------------

BCV_URL = "https://www.bcv.org.ve/"
BCV_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}


def parse_bcv_html(html: str) -> Optional[Dict[str, Any]]:
    """
    Extrae la tasa oficial del dólar y fecha valor del HTML del portal oficial del BCV.
    Maneja el formato numérico venezolano (coma como separador decimal).
    """
    idx = html.find('id="dolar"')
    if idx == -1:
        idx = html.find('id="view-dolar"')
    if idx == -1:
        idx = html.find("dolar")

    if idx == -1:
        return None

    snippet = html[idx : idx + 800]
    match = re.search(r"<strong[^>]*>\s*([0-9.,]+)\s*</strong>", snippet, re.IGNORECASE)
    if not match:
        return None

    raw_val = match.group(1).strip()
    if "," in raw_val:
        clean_val = raw_val.replace(".", "").replace(",", ".")
    else:
        clean_val = raw_val

    try:
        tasa = round(float(clean_val), 4)
    except ValueError:
        return None

    fecha_valor = None
    m_fecha = re.search(r'class="date-display-single"[^>]*content="([^"]+)"', html, re.IGNORECASE)
    if not m_fecha:
        m_fecha = re.search(r'<span class="date-display-single"[^>]*>([^<]+)</span>', html, re.IGNORECASE)
    if m_fecha:
        fecha_valor = m_fecha.group(1).strip()

    return {
        "tasa": tasa,
        "fecha_actualizacion": fecha_valor or datetime.datetime.now().isoformat(),
        "nombre": "Dólar BCV Oficial",
        "fuente": "Banco Central de Venezuela (bcv.org.ve)",
    }


async def scrape_bcv_directo() -> Optional[Dict[str, Any]]:
    """Realiza la petición directa al portal oficial del BCV (https://www.bcv.org.ve/)."""
    try:
        async with httpx.AsyncClient(verify=False, timeout=12.0, headers=BCV_HEADERS, follow_redirects=True) as client:
            resp = await client.get(BCV_URL)
            if resp.status_code == 200:
                data = parse_bcv_html(resp.text)
                if data and data["tasa"] > 0:
                    return data
    except Exception as e:
        print(f"[Aviso Scraping BCV]: No se pudo conectar directamente con bcv.org.ve: {e}")
    return None


# Cache en memoria para evitar saturar el portal del BCV en cada petición
_BCV_CACHE = {
    "data": None,
    "timestamp": 0.0,
}


@app.get("/api/bcv")
async def get_bcv(refresh: bool = False):
    """
    Consulta la tasa oficial del BCV en tiempo real.
    1. Intenta scraping directo del portal oficial bcv.org.ve
    2. Fallback secundario a ve.dolarapi.com
    3. Fallback terciario a la última tasa registrada en la BD local
    """
    ahora = time.time()
    if not refresh and _BCV_CACHE["data"] and (ahora - _BCV_CACHE["timestamp"] < 600):
        return _BCV_CACHE["data"]

    # 1. Scraping Directo Oficial
    data_oficial = await scrape_bcv_directo()
    if data_oficial:
        _BCV_CACHE["data"] = data_oficial
        _BCV_CACHE["timestamp"] = ahora
        return data_oficial

    # 2. Fallback Secundario (API Espejo)
    try:
        async with httpx.AsyncClient(timeout=6.0) as client:
            resp = await client.get("https://ve.dolarapi.com/v1/dolares/oficial")
            if resp.status_code == 200:
                data = resp.json()
                tasa = float(data.get("promedio", data.get("precio", 0)))
                res = {
                    "tasa": tasa,
                    "fecha_actualizacion": data.get("fechaActualizacion", datetime.datetime.now().isoformat()),
                    "nombre": data.get("nombre", "Dólar BCV (Espejo)"),
                    "fuente": "ve.dolarapi.com (Oficial BCV)",
                }
                _BCV_CACHE["data"] = res
                _BCV_CACHE["timestamp"] = ahora
                return res
    except Exception:
        pass

    # 3. Fallback Terciario (Última tasa histórica en SQLite)
    row = query_one("SELECT tasa_bcv FROM movimientos WHERE tasa_bcv > 0 ORDER BY id DESC LIMIT 1")
    tasa_fallback = row["tasa_bcv"] if row and row["tasa_bcv"] else 804.81

    return {
        "tasa": tasa_fallback,
        "fecha_actualizacion": datetime.datetime.now().isoformat(),
        "nombre": "Dólar BCV (Último registrado)",
        "fuente": "Historial Local",
    }


# ---------------------------------------------------------------------------
# Modelos Pydantic para la API
# ---------------------------------------------------------------------------

class ProductoCreate(BaseModel):
    nombre: str
    categoria: str
    precio_compra_actual: float
    precio_venta_actual: float


class ProductoUpdate(BaseModel):
    precio_compra_actual: float
    precio_venta_actual: float
    categoria: Optional[str] = None


class MovimientoCreate(BaseModel):
    fecha: Optional[str] = None
    tipo: str
    producto_id: int
    cantidad: int
    tasa_bcv: Optional[float] = None
    precio_unitario_bs: Optional[float] = None
    persona_proveedor: Optional[str] = ""
    estado_pago: str
    notas: Optional[str] = ""


class MovimientoUpdate(BaseModel):
    fecha: str
    tipo: str
    producto_id: int
    cantidad: int
    precio_unitario_bs: float
    costo_unitario_bs: float
    tasa_bcv: float
    persona_proveedor: str
    estado_pago: str
    notas: str


@app.get("/api/productos")
async def get_productos():
    """Retorna el catálogo completo con stock calculado en tiempo real."""
    sql = """
        SELECT
            p.id,
            p.nombre,
            p.categoria,
            p.precio_compra_actual,
            p.precio_venta_actual,
            COALESCE(SUM(CASE WHEN m.tipo = 'Compra' THEN m.cantidad ELSE 0 END), 0)
            - COALESCE(SUM(CASE WHEN m.tipo = 'Venta' THEN m.cantidad ELSE 0 END), 0) AS stock
        FROM productos p
        LEFT JOIN movimientos m ON p.id = m.producto_id
        GROUP BY p.id
        ORDER BY p.categoria, p.nombre
    """
    return query_all(sql)


@app.post("/api/productos", status_code=201)
async def create_producto(data: ProductoCreate):
    """Crea un nuevo producto en el catálogo."""
    try:
        res = execute_sql(
            "INSERT INTO productos (nombre, categoria, precio_compra_actual, precio_venta_actual) VALUES (?, ?, ?, ?)",
            (data.nombre.strip(), data.categoria.strip(), data.precio_compra_actual, data.precio_venta_actual),
        )
        return {"id": res["last_insert_rowid"], "mensaje": "Producto creado con éxito"}
    except Exception as e:
        if "UNIQUE" in str(e) or "constraint" in str(e).lower():
            raise HTTPException(400, detail="Ya existe un producto con este nombre")
        raise HTTPException(500, detail=f"Error creando producto: {str(e)}")


@app.put("/api/productos/{producto_id}")
async def update_producto(producto_id: int, data: ProductoUpdate):
    """Actualiza los precios y categoría de un producto."""
    prod = query_one("SELECT * FROM productos WHERE id = ?", (producto_id,))
    if not prod:
        raise HTTPException(404, detail="Producto no encontrado")

    cat = data.categoria.strip() if data.categoria else prod["categoria"]
    execute_sql(
        "UPDATE productos SET precio_compra_actual = ?, precio_venta_actual = ?, categoria = ? WHERE id = ?",
        (data.precio_compra_actual, data.precio_venta_actual, cat, producto_id),
    )
    return {"mensaje": "Producto actualizado correctamente", "id": producto_id}


@app.delete("/api/productos/{producto_id}")
async def delete_producto(producto_id: int):
    """Elimina un producto si no tiene movimientos registrados."""
    movs = query_one("SELECT COUNT(*) AS cnt FROM movimientos WHERE producto_id = ?", (producto_id,))
    if movs and movs["cnt"] > 0:
        raise HTTPException(400, detail=f"No se puede eliminar: tiene {movs['cnt']} movimientos registrados")

    execute_sql("DELETE FROM productos WHERE id = ?", (producto_id,))
    return {"mensaje": "Producto eliminado"}


@app.get("/api/movimientos")
async def get_movimientos():
    """Retorna el historial completo de movimientos con cálculos en Bs y USD."""
    sql = """
        SELECT
            m.id,
            m.fecha,
            m.tipo,
            m.producto_id,
            p.nombre AS producto_nombre,
            p.categoria AS producto_categoria,
            m.cantidad,
            m.precio_unitario_bs,
            m.costo_unitario_bs,
            m.tasa_bcv,
            m.persona_proveedor,
            m.estado_pago,
            m.notas,
            ROUND(m.cantidad * (CASE WHEN m.tipo = 'Compra' THEN m.costo_unitario_bs ELSE m.precio_unitario_bs END), 2) AS total_bs,
            ROUND((m.cantidad * (CASE WHEN m.tipo = 'Compra' THEN m.costo_unitario_bs ELSE m.precio_unitario_bs END)) / CASE WHEN m.tasa_bcv > 0 THEN m.tasa_bcv ELSE 1 END, 2) AS total_usd,
            ROUND(CASE WHEN m.tipo = 'Venta' THEN m.cantidad * (m.precio_unitario_bs - m.costo_unitario_bs) ELSE 0 END, 2) AS ganancia_bs
        FROM movimientos m
        JOIN productos p ON m.producto_id = p.id
        ORDER BY m.fecha DESC, m.id DESC
    """
    return query_all(sql)


@app.post("/api/movimientos", status_code=201)
async def create_movimiento(data: MovimientoCreate):
    """
    Registra un movimiento congelando los precios históricos vigentes.
    Si es compra, permite especificar el costo unitario de compra directamente.
    """
    prod = query_one("SELECT * FROM productos WHERE id = ?", (data.producto_id,))
    if not prod:
        raise HTTPException(404, detail="Producto no encontrado")

    # Obtener tasa BCV si no viene provista
    tasa = data.tasa_bcv
    if not tasa or tasa <= 0:
        bcv_info = await get_bcv()
        tasa = bcv_info["tasa"]

    fecha_reg = data.fecha or datetime.date.today().isoformat()

    if data.tipo == "Compra":
        costo_unitario = data.precio_unitario_bs if data.precio_unitario_bs is not None and data.precio_unitario_bs > 0 else prod["precio_compra_actual"]
        precio_unitario = prod["precio_venta_actual"]
    else:
        # Venta
        costo_unitario = prod["precio_compra_actual"]
        precio_unitario = data.precio_unitario_bs if data.precio_unitario_bs is not None and data.precio_unitario_bs > 0 else prod["precio_venta_actual"]

    res = execute_sql(
        """
        INSERT INTO movimientos (
            fecha, tipo, producto_id, cantidad,
            precio_unitario_bs, costo_unitario_bs,
            tasa_bcv, persona_proveedor, estado_pago, notas
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            fecha_reg,
            data.tipo,
            data.producto_id,
            data.cantidad,
            precio_unitario,
            costo_unitario,
            tasa,
            (data.persona_proveedor or "").strip(),
            data.estado_pago,
            (data.notas or "").strip(),
        ),
    )

    # Si es una Compra, actualizar el precio_compra_actual del producto
    # para que el catálogo refleje siempre el último precio pagado al proveedor
    if data.tipo == "Compra":
        execute_sql(
            "UPDATE productos SET precio_compra_actual = ? WHERE id = ?",
            (costo_unitario, data.producto_id),
        )

    return {
        "id": res["last_insert_rowid"],
        "mensaje": "Movimiento registrado correctamente",
        "precio_unitario_bs": precio_unitario,
        "costo_unitario_bs": costo_unitario,
    }


@app.put("/api/movimientos/{movimiento_id}")
async def update_movimiento(movimiento_id: int, data: MovimientoUpdate):
    """Modifica un movimiento existente."""
    mov = query_one("SELECT id FROM movimientos WHERE id = ?", (movimiento_id,))
    if not mov:
        raise HTTPException(404, detail="Movimiento no encontrado")

    execute_sql(
        """
        UPDATE movimientos SET
            fecha = ?, tipo = ?, producto_id = ?, cantidad = ?,
            precio_unitario_bs = ?, costo_unitario_bs = ?,
            tasa_bcv = ?, persona_proveedor = ?, estado_pago = ?, notas = ?
        WHERE id = ?
        """,
        (
            data.fecha, data.tipo, data.producto_id, data.cantidad,
            data.precio_unitario_bs, data.costo_unitario_bs,
            data.tasa_bcv, data.persona_proveedor.strip(), data.estado_pago, data.notas.strip(),
            movimiento_id
        ),
    )
    return {"mensaje": "Movimiento actualizado", "id": movimiento_id}


@app.delete("/api/movimientos/{movimiento_id}")
async def delete_movimiento(movimiento_id: int):
    """Elimina un movimiento registrado."""
    execute_sql("DELETE FROM movimientos WHERE id = ?", (movimiento_id,))
    return {"mensaje": "Movimiento eliminado"}


@app.put("/api/movimientos/{movimiento_id}/pagar")
async def marcar_como_pagado(movimiento_id: int):
    """Marca una venta fiada como 'Pagado'."""
    mov = query_one("SELECT * FROM movimientos WHERE id = ?", (movimiento_id,))
    if not mov:
        raise HTTPException(404, detail="Movimiento no encontrado")

    execute_sql("UPDATE movimientos SET estado_pago = 'Pagado' WHERE id = ?", (movimiento_id,))
    return {"mensaje": "Deuda saldada correctamente", "id": movimiento_id}


@app.get("/api/fiados")
async def get_fiados():
    """
    Retorna el estado de cuentas por cobrar agrupado por persona/cliente,
    con detalle de cada venta pendiente y montos totales en Bs y USD.
    """
    sql = """
        SELECT
            m.id,
            m.fecha,
            m.persona_proveedor AS cliente,
            p.nombre AS producto_nombre,
            p.categoria AS producto_categoria,
            m.cantidad,
            m.precio_unitario_bs,
            m.tasa_bcv,
            m.notas,
            ROUND(m.cantidad * m.precio_unitario_bs, 2) AS total_bs,
            ROUND((m.cantidad * m.precio_unitario_bs) / CASE WHEN m.tasa_bcv > 0 THEN m.tasa_bcv ELSE 1 END, 2) AS total_usd
        FROM movimientos m
        JOIN productos p ON m.producto_id = p.id
        WHERE m.tipo = 'Venta' AND m.estado_pago = 'Fiado'
        ORDER BY m.persona_proveedor, m.fecha DESC
    """
    rows = query_all(sql)

    # Agrupar por persona
    clientes: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        nom = r["cliente"] or "Sin nombre"
        if nom not in clientes:
            clientes[nom] = {
                "cliente": nom,
                "total_deuda_bs": 0.0,
                "total_deuda_usd": 0.0,
                "cantidad_items": 0,
                "movimientos": [],
            }
        clientes[nom]["total_deuda_bs"] += r["total_bs"]
        clientes[nom]["total_deuda_usd"] += r["total_usd"]
        clientes[nom]["cantidad_items"] += r["cantidad"]
        clientes[nom]["movimientos"].append(r)

    lista_clientes = sorted(clientes.values(), key=lambda x: x["total_deuda_bs"], reverse=True)
    total_general_bs = sum(c["total_deuda_bs"] for c in lista_clientes)
    total_general_usd = sum(c["total_deuda_usd"] for c in lista_clientes)

    return {
        "total_general_bs": round(total_general_bs, 2),
        "total_general_usd": round(total_general_usd, 2),
        "total_deudores": len(lista_clientes),
        "clientes": lista_clientes,
    }


@app.get("/api/dashboard")
async def get_dashboard():
    """
    Retorna todas las métricas del sistema:
    - Tasa BCV
    - Stock por producto y por categoría (Refrescos, Cervezas)
    - Resumen financiero por categoría (Invertido, Fiado, Vendido Pagado, Vendido General, Ganancia General, Valor Stock)
    - Resumen General Consolidado (Ganancia Neta General, Total General Valor Inventario)
    """
    # 1. Tasa BCV actual
    bcv_res = await get_bcv()
    tasa_bcv = bcv_res["tasa"]

    # 2. Stock y valorización por producto
    sql_stock = """
        SELECT
            p.id,
            p.nombre,
            p.categoria,
            p.precio_compra_actual,
            p.precio_venta_actual,
            COALESCE(SUM(CASE WHEN m.tipo = 'Compra' THEN m.cantidad ELSE 0 END), 0)
            - COALESCE(SUM(CASE WHEN m.tipo = 'Venta' THEN m.cantidad ELSE 0 END), 0) AS stock
        FROM productos p
        LEFT JOIN movimientos m ON p.id = m.producto_id
        GROUP BY p.id
        ORDER BY p.categoria, p.nombre
    """
    stock_rows = query_all(sql_stock)

    stock_por_categoria: Dict[str, Dict[str, Any]] = {}
    for row in stock_rows:
        cat = row["categoria"]
        if cat not in stock_por_categoria:
            stock_por_categoria[cat] = {
                "categoria": cat,
                "unidades_totales": 0,
                "valor_stock_bs": 0.0,
                "valor_stock_usd": 0.0,
                "productos": [],
            }
        unidades = row["stock"]
        valor_bs = round(unidades * row["precio_venta_actual"], 2)
        valor_usd = round(valor_bs / tasa_bcv, 2) if tasa_bcv > 0 else 0.0

        item = {
            "id": row["id"],
            "nombre": row["nombre"],
            "stock": unidades,
            "precio_venta_bs": row["precio_venta_actual"],
            "precio_venta_usd": round(row["precio_venta_actual"] / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
            "precio_compra_bs": row["precio_compra_actual"],
            "precio_compra_usd": round(row["precio_compra_actual"] / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
            "valor_stock_bs": valor_bs,
            "valor_stock_usd": valor_usd,
        }
        stock_por_categoria[cat]["productos"].append(item)
        stock_por_categoria[cat]["unidades_totales"] += unidades
        stock_por_categoria[cat]["valor_stock_bs"] += valor_bs
        stock_por_categoria[cat]["valor_stock_usd"] += valor_usd

    # 3. Métricas financieras por categoría
    sql_fin_cat = """
        SELECT
            p.categoria,
            ROUND(SUM(CASE WHEN m.tipo = 'Compra' THEN m.cantidad * m.costo_unitario_bs ELSE 0 END), 2) AS total_invertido_bs,
            ROUND(SUM(CASE WHEN m.tipo = 'Venta' AND m.estado_pago = 'Fiado' THEN m.cantidad * m.precio_unitario_bs ELSE 0 END), 2) AS total_fiado_bs,
            ROUND(SUM(CASE WHEN m.tipo = 'Venta' AND m.estado_pago = 'Pagado' THEN m.cantidad * m.precio_unitario_bs ELSE 0 END), 2) AS total_vendido_pagado_bs,
            ROUND(SUM(CASE WHEN m.tipo = 'Venta' THEN m.cantidad * m.precio_unitario_bs ELSE 0 END), 2) AS total_vendido_general_bs,
            ROUND(SUM(CASE WHEN m.tipo = 'Venta' THEN m.cantidad * m.costo_unitario_bs ELSE 0 END), 2) AS costo_ventas_bs,
            ROUND(SUM(CASE WHEN m.tipo = 'Venta' THEN m.cantidad * (m.precio_unitario_bs - m.costo_unitario_bs) ELSE 0 END), 2) AS ganancia_general_bs
        FROM productos p
        LEFT JOIN movimientos m ON p.id = m.producto_id
        GROUP BY p.categoria
    """
    fin_cat_rows = query_all(sql_fin_cat)

    resumen_financiero_cat: Dict[str, Dict[str, Any]] = {}
    for r in fin_cat_rows:
        cat = r["categoria"]
        inv_bs = r["total_invertido_bs"] or 0.0
        fiado_bs = r["total_fiado_bs"] or 0.0
        pagado_bs = r["total_vendido_pagado_bs"] or 0.0
        vendido_bs = r["total_vendido_general_bs"] or 0.0
        costo_bs = r["costo_ventas_bs"] or 0.0
        gan_bs = r["ganancia_general_bs"] or 0.0
        stock_val_bs = stock_por_categoria.get(cat, {}).get("valor_stock_bs", 0.0)

        resumen_financiero_cat[cat] = {
            "categoria": cat,
            "total_invertido_bs": inv_bs,
            "total_invertido_usd": round(inv_bs / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
            "total_fiado_bs": fiado_bs,
            "total_fiado_usd": round(fiado_bs / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
            "total_vendido_pagado_bs": pagado_bs,
            "total_vendido_pagado_usd": round(pagado_bs / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
            "total_vendido_general_bs": vendido_bs,
            "total_vendido_general_usd": round(vendido_bs / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
            "costo_ventas_bs": costo_bs,
            "costo_ventas_usd": round(costo_bs / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
            "ganancia_general_bs": gan_bs,
            "ganancia_general_usd": round(gan_bs / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
            "valor_stock_actual_bs": stock_val_bs,
            "valor_stock_actual_usd": round(stock_val_bs / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
        }

    # 4. Totales Globales
    total_inventario_bs = sum(c["valor_stock_bs"] for c in stock_por_categoria.values())
    total_ganancia_bs = sum(c["ganancia_general_bs"] for c in resumen_financiero_cat.values())
    total_fiado_global_bs = sum(c["total_fiado_bs"] for c in resumen_financiero_cat.values())
    total_invertido_global_bs = sum(c["total_invertido_bs"] for c in resumen_financiero_cat.values())
    total_vendido_global_bs = sum(c["total_vendido_general_bs"] for c in resumen_financiero_cat.values())

    return {
        "tasa_bcv": tasa_bcv,
        "resumen_general": {
            "ganancia_neta_bs": round(total_ganancia_bs, 2),
            "ganancia_neta_usd": round(total_ganancia_bs / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
            "valor_inventario_bs": round(total_inventario_bs, 2),
            "valor_inventario_usd": round(total_inventario_bs / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
            "total_fiado_bs": round(total_fiado_global_bs, 2),
            "total_fiado_usd": round(total_fiado_global_bs / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
            "total_invertido_bs": round(total_invertido_global_bs, 2),
            "total_invertido_usd": round(total_invertido_global_bs / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
            "total_vendido_bs": round(total_vendido_global_bs, 2),
            "total_vendido_usd": round(total_vendido_global_bs / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
        },
        "stock_por_categoria": stock_por_categoria,
        "resumen_por_categoria": resumen_financiero_cat,
    }


@app.get("/api/finanzas")
async def get_finanzas():
    """Retorna el control financiero mensual agrupado por mes y por categoría."""
    bcv_res = await get_bcv()
    tasa_bcv = bcv_res["tasa"]

    sql = """
        SELECT
            STRFTIME('%Y-%m', m.fecha) AS mes,
            p.categoria,
            ROUND(SUM(CASE WHEN m.tipo = 'Compra' THEN m.cantidad * m.costo_unitario_bs ELSE 0 END), 2) AS total_comprado_bs,
            ROUND(SUM(CASE WHEN m.tipo = 'Venta' AND m.estado_pago = 'Pagado' THEN m.cantidad * m.precio_unitario_bs ELSE 0 END), 2) AS total_vendido_pagado_bs,
            ROUND(SUM(CASE WHEN m.tipo = 'Venta' AND m.estado_pago = 'Fiado' THEN m.cantidad * m.precio_unitario_bs ELSE 0 END), 2) AS total_fiado_bs,
            ROUND(SUM(CASE WHEN m.tipo = 'Venta' THEN m.cantidad * m.precio_unitario_bs ELSE 0 END), 2) AS total_vendido_general_bs,
            ROUND(SUM(CASE WHEN m.tipo = 'Venta' THEN m.cantidad * m.costo_unitario_bs ELSE 0 END), 2) AS costo_ventas_bs,
            ROUND(SUM(CASE WHEN m.tipo = 'Venta' THEN m.cantidad * (m.precio_unitario_bs - m.costo_unitario_bs) ELSE 0 END), 2) AS ganancia_bs,
            ROUND(SUM(CASE WHEN m.tipo = 'Compra' THEN m.cantidad * m.costo_unitario_bs ELSE 0 END) - SUM(CASE WHEN m.tipo = 'Venta' THEN m.cantidad * m.precio_unitario_bs ELSE 0 END), 2) AS inversion_acumulada_bs
        FROM movimientos m
        JOIN productos p ON m.producto_id = p.id
        GROUP BY mes, p.categoria
        ORDER BY mes DESC, p.categoria
    """
    rows = query_all(sql)

    resultado = []
    for r in rows:
        item = dict(r)
        item["total_comprado_usd"] = round((item["total_comprado_bs"] or 0) / tasa_bcv, 2) if tasa_bcv > 0 else 0.0
        item["total_vendido_pagado_usd"] = round((item["total_vendido_pagado_bs"] or 0) / tasa_bcv, 2) if tasa_bcv > 0 else 0.0
        item["total_fiado_usd"] = round((item["total_fiado_bs"] or 0) / tasa_bcv, 2) if tasa_bcv > 0 else 0.0
        item["total_vendido_general_usd"] = round((item["total_vendido_general_bs"] or 0) / tasa_bcv, 2) if tasa_bcv > 0 else 0.0
        item["ganancia_usd"] = round((item["ganancia_bs"] or 0) / tasa_bcv, 2) if tasa_bcv > 0 else 0.0
        resultado.append(item)

    return resultado


@app.get("/api/banco")
async def get_banco():
    """
    Dashboard de Flujo de Caja y Conciliación Bancaria:
    - Saldo bancario esperado (entradas pagadas - salidas pagadas)
    - Top productos por unidades vendidas (porcentaje)
    - Proyección mensual de ventas, ganancia y saldo
    """
    bcv_res = await get_bcv()
    tasa_bcv = bcv_res["tasa"]

    # 1. Flujo de caja: entradas (ventas pagadas) - salidas (compras pagadas), por MES y CUENTA
    sql_flujo = """
        SELECT
            STRFTIME('%Y-%m', m.fecha) AS mes,
            p.categoria,
            ROUND(SUM(CASE WHEN m.tipo = 'Venta' AND m.estado_pago = 'Pagado'
                           THEN m.cantidad * m.precio_unitario_bs ELSE 0 END), 2) AS entradas_bs,
            ROUND(SUM(CASE WHEN m.tipo = 'Compra' AND m.estado_pago = 'Pagado'
                           THEN m.cantidad * m.costo_unitario_bs ELSE 0 END), 2) AS salidas_bs,
            ROUND(SUM(CASE WHEN m.tipo = 'Venta' AND m.estado_pago = 'Fiado'
                           THEN m.cantidad * m.precio_unitario_bs ELSE 0 END), 2) AS fiado_bs,
            ROUND(SUM(CASE WHEN m.tipo = 'Venta'
                           THEN m.cantidad * m.precio_unitario_bs ELSE 0 END), 2) AS ventas_totales_bs,
            ROUND(SUM(CASE WHEN m.tipo = 'Venta'
                           THEN m.cantidad * m.costo_unitario_bs ELSE 0 END), 2) AS costo_ventas_bs
        FROM movimientos m
        JOIN productos p ON m.producto_id = p.id
        GROUP BY mes, p.categoria
    """
    flujo_rows = query_all(sql_flujo)

    # Totales generales
    entradas = 0.0
    salidas = 0.0
    fiado = 0.0
    ventas_totales = 0.0
    costo_ventas = 0.0

    # Agrupar por mes, y dentro de cada mes por cuenta
    conciliacion_mensual = {}  # mes -> {cuentas: [...], entradas, salidas, saldo}
    for r in flujo_rows:
        mes = r["mes"]
        cat = r["categoria"]
        e = r["entradas_bs"] or 0.0
        s = r["salidas_bs"] or 0.0
        f = r["fiado_bs"] or 0.0
        vt = r["ventas_totales_bs"] or 0.0
        cv = r["costo_ventas_bs"] or 0.0
        g = round(vt - cv, 2)
        saldo = round(e - s, 2)
        entradas += e
        salidas += s
        fiado += f
        ventas_totales += vt
        costo_ventas += cv

        if mes not in conciliacion_mensual:
            conciliacion_mensual[mes] = {
                "mes": mes,
                "entradas_bs": 0.0,
                "salidas_bs": 0.0,
                "saldo_bs": 0.0,
                "cuentas": [],
            }
        conciliacion_mensual[mes]["entradas_bs"] += e
        conciliacion_mensual[mes]["salidas_bs"] += s
        conciliacion_mensual[mes]["saldo_bs"] += saldo
        conciliacion_mensual[mes]["cuentas"].append({
            "cuenta": cat,
            "entradas_bs": e,
            "entradas_usd": round(e / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
            "salidas_bs": s,
            "salidas_usd": round(s / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
            "fiado_bs": f,
            "fiado_usd": round(f / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
            "saldo_bs": saldo,
            "saldo_usd": round(saldo / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
            "ganancia_bs": g,
            "ganancia_usd": round(g / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
        })

    # Convertir a lista ordenada (mes más reciente primero) y con el saldo USD
    conciliacion_mensual_list = sorted(
        conciliacion_mensual.values(),
        key=lambda x: x["mes"],
        reverse=True,
    )
    for m in conciliacion_mensual_list:
        m["entradas_usd"] = round(m["entradas_bs"] / tasa_bcv, 2) if tasa_bcv > 0 else 0.0
        m["salidas_usd"] = round(m["salidas_bs"] / tasa_bcv, 2) if tasa_bcv > 0 else 0.0
        m["saldo_usd"] = round(m["saldo_bs"] / tasa_bcv, 2) if tasa_bcv > 0 else 0.0

    # También exponer "cuentas" agregado global (total por categoría sin mes)
    cuentas_global = {}
    for m in conciliacion_mensual_list:
        for cu in m["cuentas"]:
            if cu["cuenta"] not in cuentas_global:
                cuentas_global[cu["cuenta"]] = {
                    "cuenta": cu["cuenta"],
                    "entradas_bs": 0.0, "salidas_bs": 0.0, "fiado_bs": 0.0, "saldo_bs": 0.0, "ganancia_bs": 0.0,
                }
            cuentas_global[cu["cuenta"]]["entradas_bs"] += cu["entradas_bs"]
            cuentas_global[cu["cuenta"]]["salidas_bs"] += cu["salidas_bs"]
            cuentas_global[cu["cuenta"]]["fiado_bs"] += cu["fiado_bs"]
            cuentas_global[cu["cuenta"]]["saldo_bs"] += cu["saldo_bs"]
    for cu in cuentas_global.values():
        cu["entradas_usd"] = round(cu["entradas_bs"] / tasa_bcv, 2) if tasa_bcv > 0 else 0.0
        cu["salidas_usd"] = round(cu["salidas_bs"] / tasa_bcv, 2) if tasa_bcv > 0 else 0.0
        cu["fiado_usd"] = round(cu["fiado_bs"] / tasa_bcv, 2) if tasa_bcv > 0 else 0.0
        cu["saldo_usd"] = round(cu["saldo_bs"] / tasa_bcv, 2) if tasa_bcv > 0 else 0.0
        cu["ganancia_usd"] = round(cu["ganancia_bs"] / tasa_bcv, 2) if tasa_bcv > 0 else 0.0
    cuentas = list(cuentas_global.values())

    saldo_banco = round(entradas - salidas, 2)
    ganancia_total = round(ventas_totales - costo_ventas, 2)
    margen_neto = round((ganancia_total / ventas_totales) * 100, 1) if ventas_totales > 0 else 0.0
    tasa_cobro = round((entradas / (entradas + fiado)) * 100, 1) if (entradas + fiado) > 0 else 0.0
    roi = round((ganancia_total / salidas) * 100, 1) if salidas > 0 else 0.0

    # 2. Top productos vendidos por porcentaje (unidades)
    sql_top = """
        SELECT
            p.nombre,
            p.categoria,
            SUM(CASE WHEN m.tipo = 'Venta' THEN m.cantidad ELSE 0 END) AS unidades_vendidas,
            ROUND(SUM(CASE WHEN m.tipo = 'Venta' THEN m.cantidad * m.precio_unitario_bs ELSE 0 END), 2) AS ingresos_bs
        FROM productos p
        LEFT JOIN movimientos m ON p.id = m.producto_id
        GROUP BY p.id, p.nombre, p.categoria
        HAVING unidades_vendidas > 0
        ORDER BY unidades_vendidas DESC
    """
    top_rows = query_all(sql_top)
    total_unidades = sum(r["unidades_vendidas"] or 0 for r in top_rows) or 1

    top_productos = []
    for r in top_rows:
        unidades = r["unidades_vendidas"] or 0
        ingresos = r["ingresos_bs"] or 0.0
        top_productos.append({
            "nombre": r["nombre"],
            "categoria": r["categoria"],
            "unidades_vendidas": unidades,
            "ingresos_bs": ingresos,
            "ingresos_usd": round(ingresos / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
            "porcentaje": round((unidades / total_unidades) * 100, 1),
            "porcentaje_ingresos": round((ingresos / (sum(x["ingresos_bs"] for x in top_rows) or 1)) * 100, 1),
        })

    # 3. Proyección mensual (ventas/fiados por mes + tendencia)
    sql_mensual = """
        SELECT
            STRFTIME('%Y-%m', m.fecha) AS mes,
            ROUND(SUM(CASE WHEN m.tipo = 'Venta' AND m.estado_pago = 'Pagado'
                           THEN m.cantidad * m.precio_unitario_bs ELSE 0 END), 2) AS ventas_pagadas_bs,
            ROUND(SUM(CASE WHEN m.tipo = 'Venta' AND m.estado_pago = 'Fiado'
                           THEN m.cantidad * m.precio_unitario_bs ELSE 0 END), 2) AS ventas_fiado_bs,
            ROUND(SUM(CASE WHEN m.tipo = 'Compra' AND m.estado_pago = 'Pagado'
                           THEN m.cantidad * m.costo_unitario_bs ELSE 0 END), 2) AS compras_bs,
            ROUND(SUM(CASE WHEN m.tipo = 'Venta'
                           THEN m.cantidad * (m.precio_unitario_bs - m.costo_unitario_bs) ELSE 0 END), 2) AS ganancia_bs
        FROM movimientos m
        GROUP BY mes
        ORDER BY mes
    """
    mensual_rows = query_all(sql_mensual)

    # Construir proyección: usar el promedio de ventas pagadas de los meses con datos
    ventas_mensuales = [r["ventas_pagadas_bs"] or 0 for r in mensual_rows if (r["ventas_pagadas_bs"] or 0) > 0]
    promedio_ventas = round(sum(ventas_mensuales) / len(ventas_mensuales), 2) if ventas_mensuales else 0.0
    promedio_ganancia = round(sum(r["ganancia_bs"] or 0 for r in mensual_rows) / max(len(mensual_rows), 1), 2) if mensual_rows else 0.0

    # Proyección a 3 meses
    hoy = datetime.date.today()
    proyeccion = []
    saldo_proy = saldo_banco
    for i in range(1, 4):
        mes_proy = (hoy.replace(day=28) + datetime.timedelta(days=30 * i)).strftime("%Y-%m")
        proy_saldo = round(saldo_proy + promedio_ventas, 2)
        proyeccion.append({
            "mes": mes_proy,
            "ventas_proyectadas_bs": promedio_ventas,
            "ganancia_proyectada_bs": promedio_ganancia,
            "saldo_proyectado_bs": proy_saldo,
            "saldo_proyectado_usd": round(proy_saldo / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
        })
        saldo_proy = proy_saldo

    # Serie mensual para el gráfico de línea proyectado (históricos + proyección)
    serie_mensual = []
    for r in mensual_rows:
        serie_mensual.append({
            "mes": r["mes"],
            "ventas_bs": r["ventas_pagadas_bs"] or 0.0,
            "compras_bs": r["compras_bs"] or 0.0,
            "ganancia_bs": r["ganancia_bs"] or 0.0,
            "proyeccion": False,
        })
    # Agregar proyecciones a la serie
    for p in proyeccion:
        serie_mensual.append({
            "mes": p["mes"],
            "ventas_bs": p["ventas_proyectadas_bs"],
            "compras_bs": None,
            "ganancia_bs": p["ganancia_proyectada_bs"],
            "proyeccion": True,
        })

    # Composición de ventas por categoría
    comp = {}
    for tp in top_productos:
        cat = tp["categoria"]
        if cat not in comp:
            comp[cat] = {"categoria": cat, "ventas_bs": 0.0, "cantidad": 0}
        comp[cat]["ventas_bs"] += tp["ingresos_bs"]
        comp[cat]["cantidad"] += tp["unidades_vendidas"]
    for c in comp.values():
        c["porcentaje"] = round((c["ventas_bs"] / (ventas_totales or 1)) * 100, 1)
    composicion_categorias = list(comp.values())

    return {
        "tasa_bcv": tasa_bcv,
        "cuentas": cuentas,
        "conciliacion_mensual": conciliacion_mensual_list,
        "flujo_caja": {
            "entradas_bs": entradas,
            "entradas_usd": round(entradas / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
            "salidas_bs": salidas,
            "salidas_usd": round(salidas / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
            "fiado_bs": fiado,
            "fiado_usd": round(fiado / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
            "saldo_banco_bs": saldo_banco,
            "saldo_banco_usd": round(saldo_banco / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
            "ventas_totales_bs": ventas_totales,
            "ganancia_total_bs": ganancia_total,
        },
        "ratios": {
            "margen_neto": margen_neto,
            "efectividad_cobro": tasa_cobro,
            "roi": roi,
            "ganancia_neta_bs": ganancia_total,
            "ganancia_neta_usd": round(ganancia_total / tasa_bcv, 2) if tasa_bcv > 0 else 0.0,
        },
        "top_productos": top_productos,
        "composicion_categorias": composicion_categorias,
        "proyeccion_mensual": proyeccion,
        "serie_mensual": serie_mensual,
        "promedio_ventas_mensual_bs": promedio_ventas,
        "promedio_ganancia_mensual_bs": promedio_ganancia,
    }



# ---------------------------------------------------------------------------
# Frontend SPA Estático
# ---------------------------------------------------------------------------

if os.path.exists(PUBLIC_DIR):
    app.mount("/public", StaticFiles(directory=PUBLIC_DIR), name="public")

    @app.get("/")
    async def serve_spa():
        index_file = os.path.join(PUBLIC_DIR, "index.html")
        if os.path.exists(index_file):
            return FileResponse(index_file)
        return {"mensaje": "Inventario Casa API Activa"}
