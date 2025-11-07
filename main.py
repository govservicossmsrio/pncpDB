import os
import requests
import logging
import time
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from google.api_core import exceptions
import gspread
import google.auth

# --- CONFIGURAÇÃO DE LOG ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# --- CONFIGURAÇÃO CENTRALIZADA ---
CONFIG = {
    "GCP_PROJECT_ID": "pncpdb", 
    "BIGQUERY_DATASET": "pncp_data",
    "SPREADSHEET_ID": "18P9l9_g-QE-DWsfRCokY18M5RLZe7mV-CWY1bfw6hlA",
    "SHEET_NAME": "idLista",
    "PNCP_BASE_URL": "https://dadosabertos.compras.gov.br",
    "ENDPOINTS": {
        "CONTRATACOES": "modulo-contratacoes/1.1_consultarContratacoes_PNCP_14133_Id",
        "ITENS": "modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id",
        "RESULTADOS": "modulo-contratacoes/3.1_consultarResultadoItensContratacoes_PNCP_14133_Id"
    },
    "BATCH_SIZE": 3,
    "SUCCESS_DELAY_SECONDS": 2,
    "MAX_RETRIES_PER_ITEM": 3,
    "RETRY_DELAYS_SECONDS": {3: 5, 6: 10, 9: 60, 12: 300, 15: 600, 18: "CANCEL"}
}

def get_pncp_data(endpoint_key, id_param):
    url = f"{CONFIG['PNCP_BASE_URL']}/{CONFIG['ENDPOINTS'][endpoint_key]}"
    params = {'tipo': 'idCompra', 'codigo': id_param}
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        if not response.content:
            logging.warning(f"Resposta vazia da API para {url} com params {params}.")
            return None
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"Erro HTTP para ID {id_param}. Status: {http_err.response.status_code}. Resposta: {http_err.response.text}")
        raise
    except Exception as e:
        logging.error(f"Erro inesperado na chamada da API para ID {id_param}: {e}")
        raise

def load_data_to_bigquery(df, table_name):
    if df.empty: return
    table_id = f"{CONFIG['GCP_PROJECT_ID']}.{CONFIG['BIGQUERY_DATASET']}.{table_name}"
    try:
        df.to_gbq(destination_table=table_id, project_id=CONFIG["GCP_PROJECT_ID"], if_exists='append')
    except Exception as e:
        logging.error(f"Falha ao carregar dados na tabela '{table_name}': {e}")
        raise

def process_single_id(pncp_id):
    try:
        contratacoes_data = get_pncp_data("CONTRATACOES", pncp_id)
        if not contratacoes_data or not contratacoes_data.get('data'):
            logging.warning(f"Nenhum dado de contratação encontrado para ID {pncp_id}. Pulando.")
            return

        contratacoes_list = contratacoes_data.get('data', [])
        itens_data = get_pncp_data("ITENS", pncp_id)
        resultados_data = get_pncp_data("RESULTADOS", pncp_id)

        if contratacoes_list:
            df = pd.json_normalize(contratacoes_list)
            logging.info(f"Processando {len(df)} registro(s) para a tabela 'contratacoes'.")
            load_data_to_bigquery(df, 'contratacoes')

        if itens_data and itens_data.get('data'):
            df = pd.json_normalize(itens_data.get('data', []))
            logging.info(f"Processando {len(df)} registro(s) para a tabela 'itens_compra'.")
            load_data_to_bigquery(df, 'itens_compra')

        if resultados_data and resultados_data.get('data'):
            df = pd.json_normalize(resultados_data.get('data', []))
            logging.info(f"Processando {len(df)} registro(s) para a tabela 'resultados_itens'.")
            load_data_to_bigquery(df, 'resultados_itens')

    except Exception as e:
        logging.warning(f"Falha ao processar completamente o ID {pncp_id}.")
        raise e

def main():
    logging.info("--- INICIANDO PIPELINE DE ETL DO PNCP ---")
    
    try:
        # Pega as credenciais padrão do ambiente (configuradas pelo GitHub Actions).
        creds, project_id = google.auth.default()

        # Cria uma versão das credenciais com os escopos necessários para Sheets e Drive.
        scoped_creds = creds.with_scopes([
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ])
        
        # Autoriza o gspread usando as credenciais com escopo.
        gc = gspread.authorize(scoped_creds)
        
        # O cliente do BigQuery usará as credenciais padrão, que já têm o escopo cloud-platform.
        bq_client = bigquery.Client(credentials=creds, project=project_id or CONFIG["GCP_PROJECT_ID"])

        logging.info("Lendo IDs da planilha Google Sheets...")
        sheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])
        all_rows = sheet.get_all_values()
        ids_from_sheet = [row[0] for row in all_rows[1:] if row and row[0].strip()]
        logging.info(f"Encontrados {len(ids_from_sheet)} IDs na planilha.")

        query = f"SELECT DISTINCT CAST(idCompra AS STRING) as idCompra FROM `{CONFIG['GCP_PROJECT_ID']}.{CONFIG['BIGQUERY_DATASET']}.contratacoes`"
        existing_ids = set()
        try:
            df_existing = bq_client.query(query).to_dataframe()
            if not df_existing.empty:
                existing_ids = set(df_existing['idCompra'])
        except exceptions.NotFound:
            logging.warning("Tabela 'contratacoes' não encontrada. Assumindo primeira execução.")
        
        ids_to_process = [id_val for id_val in ids_from_sheet if id_val not in existing_ids]
        logging.info(f"Após filtragem, {len(ids_to_process)} IDs a serem processados.")
        
    except Exception as e:
        logging.critical(f"Falha na configuração inicial. Abortando. Erro: {e}")
        return

    retry_counts = {item: 0 for item in ids_to_process}
    while ids_to_process:
        batch = ids_to_process[:CONFIG["BATCH_SIZE"]]
        if not batch: break
        
        logging.info(f"--- Processando lote de {len(batch)} IDs: {batch} ---")
        
        for item in list(batch):
            if item not in ids_to_process: continue

            retry_counts.setdefault(item, 0)
            retry_counts[item] += 1
            try:
                logging.info(f"Processando ID: {item} (Tentativa {retry_counts[item]})")
                process_single_id(item)
                ids_to_process.remove(item)
                time.sleep(CONFIG["SUCCESS_DELAY_SECONDS"])
            except Exception:
                logging.warning(f"Falha na tentativa {retry_counts[item]} para o ID: {item}.")
                if retry_counts[item] >= CONFIG["MAX_RETRIES_PER_ITEM"]:
                    logging.error(f"ID {item} atingiu o máximo de falhas e será descartado.")
                    ids_to_process.remove(item)
    
    logging.info("--- PIPELINE DE ETL DO PNCP CONCLUÍDO ---")

if __name__ == "__main__":
    main()
