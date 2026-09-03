# Inventario Casa ⚡

Sistema web ligero, rápido y optimizado de gestión de inventario, ventas, cuentas por cobrar (fiados) y finanzas familiares.

**Stack:** Python Nativo + SQLite 3 (`sqlite3`) + FastAPI + OpenPyXL + Frontend Vanilla CSS/JS (Light Mode First).

---

## 🚀 Inicio Rápido

### Opción 1: Con doble clic en Windows
Simplemente haz doble clic en el archivo **`iniciar.bat`**. Se abrirá automáticamente el navegador en `http://localhost:8000`.

### Opción 2: Desde la terminal
```bash
# Activar entorno e iniciar
.\venv\Scripts\python.exe iniciar.py
```

---

## 📦 Estructura del Proyecto

```
inventario-casa/
├── api/
│   └── index.py            # Backend FastAPI en Python nativo con SQLite3 y OpenPyXL
├── public/
│   └── index.html          # Frontend SPA (Vanilla CSS/JS, Light & Dark Mode)
├── iniciar.py              # Script de inicio con apertura automática del navegador
├── iniciar.bat             # Acceso directo para Windows
├── inventario_casa.db      # Base de datos local ultrarrápida SQLite3
├── requirements.txt        # Dependencias esenciales de Python
└── README.md
```

---

## 💡 Características Principales

1. **Gestión Fiel al Excel:**
   - Métricas y cálculos idénticos a las 4 hojas del Excel de escritorio: `Dashboard`, `Movimientos`, `Catálogo`, `Finanzas`.
   - Soporte multimoneda en **Bolívares (Bs)** y **Dólares (USD)** calculados con la tasa BCV oficial en tiempo real.
2. **Control de Cuentas por Cobrar (Fiados):**
   - Panel de deudores agrupados por cliente con saldo adeudado y botón para saldar deudas con un solo clic.
3. **Catálogo & Precios:**
   - Control de stock físico en tiempo real (Compras - Ventas).
   - Edición ágil de precios de costo y venta.
4. **Sincronización Bidireccional:**
   - **Sincronizar Excel:** Importa en cualquier momento los datos desde `Inventario_Casa_Torres_Actualizado(2).xlsx` en el escritorio.
   - **Exportar Excel:** Genera y descarga un archivo `.xlsx` idéntico con fórmulas nativas y estilos profesionales.
5. **Diseño UI Light Mode First:**
   - Fondo claro `#fafafa` por defecto, con switch a modo oscuro `#09090b`.
   - Tipografía Outfit y acento dorado ámbar.
