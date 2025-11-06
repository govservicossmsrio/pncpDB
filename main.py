import os
import json
import requests
import logging
import time
from datetime import datetime
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
from google.api_core import exceptions
import gspread

# --- CONFIGURAÇÃO DE LOG ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# --- CONFIGURAÇÃO CENTRALIZADA ---
CONFIG = {
    "GCP_SECRET_NAME": "GCP_SA_KEY",
    "GCP_PROJECT_ID": "pncpDB",
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

def get_gcp_credentials():
    gcp_sa_key_json = os.getenv(CONFIG["GCP_SECRET_NAME"])
    if not gcp_sa_key_json:
        raise ValueError(f"Secret {CONFIG['GCP_SECRET_NAME']} não encontrada nas variáveis de ambiente.")
    
    info = json.loads(gcp_sa_key_json)
    return service_account.Credentials.from_service_account_info(info)

def get_google_sheets_client(credentials):
    return gspread.authorize(credentials.with_scopes([
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]))

def get_pncp_data(endpoint_key, id_param, param_type="codigo"):
    url = f"{CONFIG['PNCP_BASE_URL']}/{CONFIG['ENDPOINTS'][endpoint_key]}"
    params = {'tipo': 'idCompra', param_type: id_param}
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        if not response.content:
            logging.warning(f"Resposta vazia da API para {url} com params {params}.")
            return []
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"Erro HTTP para ID {id_param} no endpoint {endpoint_key}. Status: {http_err.response.status_code}. Resposta: {http_err.response.text}")
        raise
    except ValueError:
        logging.error(f"Falha ao decodificar JSON da API para ID {id_param}. Resposta recebida: {response.text[:200]}...")
        raise
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Erro de rede/conexão para ID {id_param}: {req_err}")
        raise

def load_data_to_bigquery(df, table_name, credentials):
    if df.empty:
        logging.info(f"DataFrame para a tabela '{table_name}' está vazio. Nenhum dado para carregar.")
        return

    table_id = f"{CONFIG['GCP_PROJECT_ID']}.{CONFIG['BIGQUERY_DATASET']}.{table_name}"
    
    try:
        df.to_gbq(
            destination_table=table_id,
            project_id=CONFIG["GCP_PROJECT_ID"],
            if_exists='append',
            credentials=credentials
        )
        logging.info(f"{len(df)} registros carregados com sucesso na tabela '{table_name}'.")
    except Exception as e:
        logging.error(f"Falha ao carregar dados na tabela '{table_name}' do BigQuery. Erro: {e}")
        raise

def process_single_id(pncp_id, credentials):
    try:
        contratacoes_data = get_pncp_data("CONTRATACOES", pncp_id)
        if not contratacoes_data:
            logging.warning(f"Nenhum dado de contratação encontrado para o ID {pncp_id}. Pulando.")
            return

        itens_data = get_pncp_data("ITENS", pncp_id)
        resultados_data = get_pncp_data("RESULTADOS", pncp_id)

        contratacoes_df = pd.DataFrame(contratacoes_data)
        load_data_to_bigquery(contratacoes_df, 'contratacoes', credentials)
        
        if itens_data:
            itens_df = pd.DataFrame(itens_data)
            load_data_to_bigquery(itens_df, 'itens', credentials)

        if resultados_data:
            resultados_df = pd.DataFrame(resultados_data)
            load_data_to_bigquery(resultados_df, 'resultados', credentials)

    except Exception as e:
        logging.warning(f"Falha ao processar completamente o ID: {pncp_id}. Detalhes: {e}")
        raise e

def main():
    logging.info("--- INICIANDO PIPELINE DE ETL DO PNCP ---")
    consecutive_failures = 0
    
    try:
        credentials = get_gcp_credentials()
        gs_client = get_google_sheets_client(credentials)
        
        # **CORREÇÃO 1: IGNORAR CABEÇALHO**
        logging.info("Lendo IDs da planilha Google Sheets...")
        sheet = gs_client.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])
        all_rows = sheet.get_all_values()
        # Pula a primeira linha (cabeçalho) usando slice [1:]
        ids_from_sheet = [row[0] for row in all_rows[1:] if row and row[0].strip()]
        logging.info(f"Encontrados {len(ids_from_sheet)} IDs na planilha.")

        # **CORREÇÃO 2: CONSULTA ROBUSTA AO BIGQUERY**
        bq_client = bigquery.Client(credentials=credentials, project=CONFIG["GCP_PROJECT_ID"])
        query = f"SELECT DISTINCT CAST(idCompra AS STRING) as idCompra FROM `{CONFIG['GCP_PROJECT_ID']}.{CONFIG['BIGQUERY_DATASET']}.contratacoes`"
        existing_ids = set()
        try:
            query_job = bq_client.query(query)
            df_existing = query_job.to_dataframe()
            if not df_existing.empty:
                existing_ids = set(df_existing['idCompra'])
        except exceptions.NotFound:
            logging.warning("Tabela 'contratacoes' não encontrada. Assumindo que nenhum ID existe (primeira execução).")
        
        ids_to_process = [id_val for id_val in ids_from_sheet if id_val not in existing_ids]
        logging.info(f"Após filtragem, {len(ids_to_process)} IDs a serem processados.")
        
    except Exception as e:
        logging.critical(f"Falha na configuração inicial. Abortando. Erro: {e}")
        return

    retry_counts = {item: 0 for item in ids_to_process}
    while ids_to_process:
        batch = ids_to_process[:CONFIG["BATCH_SIZE"]]
        logging.info(f"--- Processando lote de {len(batch)} IDs: {batch} ---")
        
        batch_failed = True
        for item in batch:
            retry_counts[item] += 1
            logging.info(f"Processando ID: {item} (Tentativa {retry_counts[item]})")
            
            try:
                process_single_id(item, credentials)
                ids_to_process.remove(item)
                batch_failed = False
                consecutive_failures = 0
                time.sleep(CONFIG["SUCCESS_DELAY_SECONDS"])
            except Exception as final_error:
                logging.warning(f"Falha na tentativa {retry_counts[item]} para o ID: {item}.")
                if retry_counts[item] >= CONFIG["MAX_RETRIES_PER_ITEM"]:
                    logging.error(f"ID {item} atingiu o máximo de {CONFIG['MAX_RETRIES_PER_ITEM']} falhas. Erro final: {final_error}. Desistindo.")
                    ids_to_process.remove(item)

        if batch_failed:
            consecutive_failures += 1
            logging.warning(f"Lote inteiro falhou. Contagem de falhas consecutivas: {consecutive_failures}")
            
            delay = CONFIG["RETRY_DELAYS_SECONDS"].get(consecutive_failures)
            if delay == "CANCEL":
                logging.critical("Número máximo de falhas consecutivas atingido. Abortando o pipeline.")
                break
            if delay:
                logging.info(f"Aguardando {delay}s antes da próxima tentativa...")
                time.sleep(delay)

    logging.info("--- PIPELINE DE ETL DO PNCP CONCLUÍDO COM SUCESSO ---")

if __name__ == "__main__":
    main()
