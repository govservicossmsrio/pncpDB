import os
import time
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from google.auth import default
import gspread
import psycopg2
from psycopg2.extras import execute_batch
from typing import Dict, Optional, List, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# =====================================================
# CONFIGURAÇÃO
# =====================================================

CONFIG = {
    "COCKROACH_CONNECTION_STRING": os.getenv(
        "COCKROACH_CONNECTION_STRING",
        "postgresql://sgc_admin:<password>@scary-quetzal-18026.j77.aws-us-east-1.cockroachlabs.cloud:26257/defaultdb?sslmode=require"
    ),
    "SPREADSHEET_ID": os.getenv("SPREADSHEET_ID", "18P9l9_g-QE-DWsfRCokY18M5RLZe7mV-CWY1bfw6hlA"),
    "SHEET_NAME": "idlista_sms",
    "OUTROS_SHEET": "idlista_outros",
    "PNCP_BASE_URL": "https://dadosabertos.compras.gov.br",
    "ENDPOINTS": {
        "CONTRATACOES": "modulo-contratacoes/1.1_consultarContratacoes_PNCP_14133_Id",
        "ITENS": "modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id",
        "RESULTADOS": "modulo-contratacoes/3.1_consultarResultadoItensContratacoes_PNCP_14133_Id"
    },
    "BATCH_SIZE": 50,
    "SUCCESS_DELAY_SECONDS": 2,
    "API_INTERVAL_SECONDS": 1,
    "RETRY_DELAY_SECONDS": 5,
    "MAX_RETRIES_PER_ITEM": 3
}

STATUS_FINALIZADOS = {"homologado", "fracassado", "deserto", "anulado/revogado/cancelado"}

db_lock = Lock()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =====================================================
# SCHEMAS
# =====================================================

COMPRAS_SCHEMA = {
    "idcompra": "STRING",
    "numerocompra": "STRING",
    "anocomprapncp": "INTEGER",
    "codigomodalidade": "INTEGER",
    "modalidadenome": "STRING",
    "srp": "BOOLEAN",
    "unidadeorgaocodigounidade": "STRING",
    "unidadeorgaonomeunidade": "STRING",
    "unidadeorgaomunicipionome": "STRING",
    "unidadeorgaoufsigla": "STRING",
    "processo": "STRING",
    "objetocompra": "STRING",
    "valortotalestimado": "FLOAT",
    "valortotalhomologado": "FLOAT",
    "existeresultado": "BOOLEAN",
    "dataaberturapropostapncp": "TIMESTAMP",
    "contratacaoexcluida": "BOOLEAN",
    "itenstotal": "INTEGER",
    "itensresultados": "INTEGER",
    "itenshomologados": "INTEGER",
    "itensfracassados": "INTEGER",
    "itensdesertos": "INTEGER",
    "itensoutros": "INTEGER"
}

ITENS_SCHEMA = {
    "idcompraitem": "STRING",
    "idcompra": "STRING",
    "numeroitemcompra": "INTEGER",
    "numerogrupo": "INTEGER",
    "materialouserviconome": "STRING",
    "tipobeneficionome": "STRING",
    "coditemcatalogo": "STRING",
    "descricaoresumida": "STRING",
    "descricaodetalhada": "STRING",
    "quantidade": "FLOAT",
    "unidademedida": "STRING",
    "valorunitarioestimado": "FLOAT",
    "valortotal": "FLOAT",
    "temresultado": "BOOLEAN",
    "situacaocompraitemnome": "STRING",
    "cnpjfornecedor": "STRING",
    "nomefornecedor": "STRING"
}

RESULTADOS_SCHEMA = {
    "idcompraitem": "STRING",
    "idcompra": "STRING",
    "nifornecedor": "STRING",
    "tipopessoa": "STRING",
    "nomerazaosocialfornecedor": "STRING",
    "naturezajuridicanome": "STRING",
    "portefornecedornome": "STRING",
    "quantidadehomologada": "FLOAT",
    "valorunitariohomologado": "FLOAT",
    "valortotalhomologado": "FLOAT",
    "percentualdesconto": "FLOAT",
    "dataresultadopncp": "TIMESTAMP",
    "aplicacaobeneficiomeepp": "BOOLEAN"
}

# =====================================================
# CONEXÃO
# =====================================================

def get_db_connection():
    try:
        return psycopg2.connect(CONFIG["COCKROACH_CONNECTION_STRING"])
    except Exception as e:
        logger.error(f"Erro conexão DB: {e}")
        raise

# =====================================================
# INICIALIZAÇÃO
# =====================================================

def init_control_tables():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS precos_catalogo_processados (
                idcompra STRING PRIMARY KEY,
                sucesso BOOLEAN,
                data_processamento TIMESTAMP DEFAULT current_timestamp()
            )
        """)
        for table in ['compras', 'itens_compra', 'resultados_itens']:
            cursor.execute(f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = '{table}' AND column_name = 'origem'
            """)
            if not cursor.fetchone():
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN origem STRING DEFAULT 'SMS'")
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Erro inicialização: {e}")
        raise

# =====================================================
# GOOGLE SHEETS – AUXILIARES
# =====================================================

def get_sheets_client():
    credentials, _ = default(scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return gspread.authorize(credentials)

def write_sms_status(idcompra: str, status: str, dt: str):
    try:
        gc = get_sheets_client()
        sheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])
        values = sheet.get_all_values()
        for i in range(1, len(values)):
            if values[i][0].strip() == idcompra:
                sheet.update_cell(i+1, 4, status)
                sheet.update_cell(i+1, 5, dt)
                return
    except Exception as e:
        logger.error(f"Erro escrita SMS sheets: {e}")

def write_outros_status(idcompra: str, modalidade: str, lic: str, status: str, dt: str):
    try:
        gc = get_sheets_client()
        sheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["OUTROS_SHEET"])
        values = sheet.get_all_values()
        last = len(values)
        found = False
        for i in range(1, last):
            if values[i][0].strip() == idcompra:
                sheet.update_cell(i+1, 4, status)
                sheet.update_cell(i+1, 5, dt)
                found = True
                break
        if not found:
            sheet.append_row([idcompra, lic, modalidade, status, dt])
    except Exception as e:
        logger.error(f"Erro escrita OUTROS sheets: {e}")

# =====================================================
# IDCOMPRA – FILTROS E OBTENÇÃO
# =====================================================

def get_ids_from_sheets():
    try:
        gc = get_sheets_client()
        sheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])
        rows = sheet.get_all_values()
        return [row[0].strip() for row in rows[1:] if row and row[0].strip()]
    except:
        return []

def check_if_compra_finalizada(idcompra, conn):
    cur = conn.cursor()
    cur.execute("SELECT situacaocompraitemnome FROM itens_compra WHERE idcompra=%s", (idcompra,))
    rows = cur.fetchall()
    cur.close()
    if not rows:
        return False
    for (s,) in rows:
        if not s or s.lower().strip() not in STATUS_FINALIZADOS:
            return False
    return True

def get_filtered_sheets_ids():
    ids = get_ids_from_sheets()
    conn = get_db_connection()
    out = []
    for idc in ids:
        if not check_if_compra_finalizada(idc, conn):
            out.append((idc, "SMS"))
    conn.close()
    return out

def get_new_precos_catalogo_ids():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT pc.idcompra
            FROM precos_catalogo pc
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except:
        return []
    ids = [r[0] for r in rows]
    filtered = []
    for x in ids:
        if len(x) >= 9 and x[-9] == "9":
            filtered.append((x, "outras fontes"))
    return filtered

def get_idcompraitems_from_precos_catalogo(idc):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT idcompraitem FROM precos_catalogo WHERE idcompra=%s", (idc,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {r[0] for r in rows}
    except:
        return set()

def mark_catalogo_processed(idc, sucesso):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO precos_catalogo_processados (idcompra, sucesso, data_processamento)
        VALUES (%s, %s, %s)
        ON CONFLICT (idcompra) DO UPDATE SET sucesso=%s, data_processamento=%s
    """, (idc, sucesso, datetime.utcnow(), sucesso, datetime.utcnow()))
    conn.commit()
    cur.close()
    conn.close()

# =====================================================
# API
# =====================================================

def get_pncp_data(endpoint_key, id_param):
    try:
        url = f"{CONFIG['PNCP_BASE_URL']}/{CONFIG['ENDPOINTS'][endpoint_key]}"
        params = {'tipo': 'idCompra', 'codigo': id_param}
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        if not r.content:
            return None
        return r.json()
    except:
        return None

# =====================================================
# TRANSFORMAÇÃO
# =====================================================

def convert_column_type(s, t):
    try:
        if t == "STRING":
            r = s.astype(str).replace(['nan', 'None', '<NA>', ''], None)
            if hasattr(r, 'str'):
                r = r.str.replace(r'\.0$', '', regex=True)
            return r
        if t == "INTEGER":
            return pd.to_numeric(s, errors='coerce').astype('Int64')
        if t == "FLOAT":
            return pd.to_numeric(s, errors='coerce')
        if t == "BOOLEAN":
            return s.astype(bool)
        if t == "TIMESTAMP":
            return pd.to_datetime(s, errors='coerce', utc=True)
        return s
    except:
        return s

def normalize_column_names(df):
    df.columns = df.columns.str.replace('.', '', regex=False).str.lower()
    return df

def map_and_clean_dataframe(df, schema):
    if df.empty:
        return pd.DataFrame(columns=list(schema.keys()) + ['data_extracao', 'origem'])
    df = normalize_column_names(df)
    r = pd.DataFrame()
    for col, dtype in schema.items():
        if col in df.columns:
            r[col] = convert_column_type(df[col], dtype)
        else:
            r[col] = None
    r['data_extracao'] = datetime.utcnow()
    return r

# =====================================================
# LOAD
# =====================================================

def load_data_to_cockroach(df, table, schema, origem):
    if df.empty:
        return True
    try:
        df['origem'] = origem
        conn = get_db_connection()
        cur = conn.cursor()
        cols = list(schema.keys()) + ['data_extracao', 'origem']
        ph = ','.join(['%s'] * len(cols))
        cols_str = ','.join(cols)
        conflict = "idcompraitem" if table != "compras" else "idcompra"
        set_clause = ','.join([f"{c}=EXCLUDED.{c}" for c in cols if c != conflict])
        q = f"INSERT INTO {table} ({cols_str}) VALUES ({ph}) ON CONFLICT ({conflict}) DO UPDATE SET {set_clause}"
        data = [tuple(row) for row in df[cols].replace({np.nan: None}).values]
        execute_batch(cur, q, data, page_size=1000)
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Erro load {table}: {e}")
        return False

# =====================================================
# PROCESSAMENTO
# =====================================================

def process_api(endpoint_key, pncp_id, schema, table, origem, filter_items, retry=True):
    data = get_pncp_data(endpoint_key, pncp_id)
    if (not data or not data.get("resultado")) and retry:
        time.sleep(CONFIG["RETRY_DELAY_SECONDS"])
        data = get_pncp_data(endpoint_key, pncp_id)
    if not data or not data.get("resultado"):
        return False, f"erro ao puxar os dados da API {endpoint_key}"
    df = pd.json_normalize(data["resultado"])
    df = map_and_clean_dataframe(df, schema)
    if filter_items and 'idcompraitem' in df.columns:
        df = df[df['idcompraitem'].isin(filter_items)]
    if df.empty:
        return True, None
    ok = load_data_to_cockroach(df, table, schema, origem)
    if not ok:
        return False, f"erro ao inserir na tabela {table}"
    return True, None

def process_single_id(pncp_id, origem):
    filter_items = get_idcompraitems_from_precos_catalogo(pncp_id) if origem == "outras fontes" else None
    errors = []
    results = {}

    ok, err = process_api("CONTRATACOES", pncp_id, COMPRAS_SCHEMA, "compras", origem, filter_items)
    if not ok:
        errors.append(err)
    results["CONTRATACOES"] = ok

    time.sleep(1)

    ok, err = process_api("ITENS", pncp_id, ITENS_SCHEMA, "itens_compra", origem, filter_items)
    if not ok:
        errors.append(err)
    results["ITENS"] = ok

    time.sleep(1)

    ok, err = process_api("RESULTADOS", pncp_id, RESULTADOS_SCHEMA, "resultados_itens", origem, filter_items)
    if not ok:
        errors.append(err)
    results["RESULTADOS"] = ok

    status = "sucesso" if not errors else " // ".join(errors)
    dt = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    if origem == "SMS":
        write_sms_status(pncp_id, status, dt)
    else:
        write_outros_status(pncp_id, None, None, status, dt)

    if origem == "outras fontes":
        sucesso_total = results["CONTRATACOES"] and results["ITENS"]
        mark_catalogo_processed(pncp_id, sucesso_total)

    return True

# =====================================================
# MAIN
# =====================================================

def main():
    init_control_tables()

    sms_ids = get_filtered_sheets_ids()
    outros_ids = get_new_precos_catalogo_ids()

    all_ids = sms_ids + outros_ids
    seen = set()
    unique = []
    for idc, o in all_ids:
        if idc not in seen:
            seen.add(idc)
            unique.append((idc, o))

    for idc, o in unique:
        process_single_id(idc, o)
        time.sleep(2)

if __name__ == "__main__":
    main()
