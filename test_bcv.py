"""
Script de prueba para validación y extracción robusta de la tasa oficial del BCV.
"""

import re
import datetime
import httpx

BCV_URL = "https://www.bcv.org.ve/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

def parse_bcv_html(html: str):
    # 1. Buscar bloque del dólar
    idx = html.find('id="dolar"')
    if idx == -1:
        idx = html.find('id="view-dolar"')
    if idx == -1:
        idx = html.find("dolar")

    if idx == -1:
        return None

    snippet = html[idx : idx + 800]
    
    # Extraer el valor numérico en el tag strong
    match = re.search(r"<strong[^>]*>\s*([0-9.,]+)\s*</strong>", snippet, re.IGNORECASE)
    if not match:
        return None

    raw_val = match.group(1).strip()
    # En Venezuela el separador de miles es punto y el decimal es coma
    # Ejemplo: '804,81090000' -> '804.8109'
    if "," in raw_val:
        clean_val = raw_val.replace(".", "").replace(",", ".")
    else:
        clean_val = raw_val

    tasa = round(float(clean_val), 4)

    # Extraer fecha valor
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

def test_scrape():
    with httpx.Client(verify=False, timeout=12.0, headers=HEADERS, follow_redirects=True) as client:
        r = client.get(BCV_URL)
        if r.status_code == 200:
            res = parse_bcv_html(r.text)
            print("Resultado scraping BCV:", res)
            return res
        else:
            print("Error HTTP status:", r.status_code)
            return None

if __name__ == "__main__":
    test_scrape()
