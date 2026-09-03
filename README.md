# Inventario Casa ⚡

Sistema web ligero, rápido y optimizado de gestión de inventario, ventas, cuentas por cobrar (fiados) y finanzas familiares.

**Stack:** Python + FastAPI + SQLite 3 (local) / Turso (cloud) + OpenPyXL + Frontend Vanilla CSS/JS

---

## 🚀 Inicio Rápido

### Opción 1: Con doble clic en Windows
Haz doble clic en **`iniciar.bat`** → se abre `http://localhost:8080`

### Opción 2: Desde la terminal
```bash
.\venv\Scripts\python.exe iniciar.py
```

---

## 📦 Estructura del Proyecto

```
inventario-casa/
├── api/
│   └── index.py            # Backend FastAPI (SQLite local + Turso cloud)
├── public/
│   └── index.html          # Frontend SPA (Vanilla CSS/JS, Light & Dark Mode)
├── iniciar.py              # Script de inicio con apertura automática del navegador
├── iniciar.bat             # Acceso directo para Windows
├── inventario_casa.db      # Base de datos local SQLite3 (desarrollo)
├── requirements.txt        # Dependencias de Python
├── vercel.json             # Configuración de deploy en Vercel
├── .env.example            # Plantilla de variables de entorno (Turso)
└── README.md
```

---

## 🔧 Configuración

### Variables de Entorno
Copia `.env.example` como `.env` y completa tus credenciales de Turso:

```bash
TURSO_DATABASE_URL=libsql://tu-db.turso.io
TURSO_AUTH_TOKEN=tu-token-aqui
```

> Si no se configura Turso, el sistema usa SQLite local automáticamente.

### Deploy en Vercel
1. Conecta el repo de GitHub en [vercel.com/new](https://vercel.com/new)
2. Configura las variables de entorno en el dashboard de Vercel:
   - `TURSO_DATABASE_URL`
   - `TURSO_AUTH_TOKEN`
3. Deploy automático en cada push a `master`

---

## 💡 Características

1. **Gestión de Inventario:**
   - Catálogo de productos con precios de compra y venta
   - Control de stock físico en tiempo real (Compras - Ventas)
   - Edición ágil de precios

2. **Multimoneda (Bs/USD):**
   - Tasa BCV oficial en tiempo real
   - Cálculos automáticos en Bolívares y Dólares

3. **Cuentas por Cobrar (Fiados):**
   - Panel de deudores agrupados por cliente
   - Saldar deudas con un solo clic

4. **Dashboard & Finanzas:**
   - Métricas resumen de inventario y ventas
   - Panel financiero con ganancias y movimiento

5. **Sincronización Excel:**
   - Importar datos desde archivo Excel del escritorio
   - Exportar a `.xlsx` con fórmulas y estilos profesionales

6. **UI Moderna:**
   - Light Mode por defecto (`#fafafa`)
   - Dark Mode (`#09090b`)
   - Tipografía Outfit, acento dorado ámbar

---

## 🛠️ Tech Stack

| Capa | Tecnología |
|------|-----------|
| Backend | Python + FastAPI + Uvicorn |
| Base de datos | SQLite 3 (local) / Turso (cloud) |
| Frontend | HTML + CSS + JavaScript (vanilla) |
| Excel | OpenPyXL |
| Deploy | Vercel |
| HTTP Client | httpx |

---

## 📝 Licencia

Proyecto personal — Guillermo Torres
