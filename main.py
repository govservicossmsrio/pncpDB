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
import numpy as np

# --- CONFIGURAÇÃO DE LOG ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# --- CONFIGURAÇÃO CENTRALIZADA ---
CONFIG = {
    "GCP_PROJECT_ID": "inbound-fulcrum-476517-s6",
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

# Mapeamento de campos e seus tipos (conforme schema do BigQuery)
COMPRAS_SCHEMA = {
    'idCompra': 'STRING',
    'numeroCompra': 'STRING',
    'anoCompraPncp': 'INTEGER',
    'codigoModalidade': 'INTEGER',
    'modalidadeNome': 'STRING',
    'srp': 'BOOLEAN',
    'unidadeOrgaoCodigoUnidade': 'INTEGER',
    'unidadeOrgaoNomeUnidade': 'STRING',
    'unidadeOrgaoMunicipioNome': 'STRING',
    'unidadeOrgaoUfSigla': 'STRING',
    'orgaoEntidadeEsferaId': 'INTEGER',
    'processo': 'STRING',
    'objetoCompra': 'STRING',
    'valorTotalEstimado': 'FLOAT',
    'valorTotalHomologado': 'FLOAT',
    'existeResultado': 'BOOLEAN',
    'dataAberturaPropostaPncp': 'TIMESTAMP',
    'contratacaoExcluida': 'BOOLEAN',
    'dataExtracao': 'TIMESTAMP',
    'itensTotal': 'INTEGER',
    'itensResultados': 'INTEGER',
    'itensHomologados': 'INTEGER',
    'itensFracassados': 'INTEGER',
    'itensDesertos': 'INTEGER',
    'itensOutros': 'INTEGER'
}

ITENS_SCHEMA = {
    'idCompraItem': 'STRING',
    'idCompra': 'STRING',
    'numeroItemCompra': 'INTEGER',
    'numeroGrupo': 'INTEGER',
    'materialOuServicoNome': 'STRING',
    'tipoBeneficioNome': 'STRING',
    'codItemCatalogo': 'INTEGER',
    'descricaoResumida': 'STRING',
    'descricaodetalhada': 'STRING',
    'quantidade': 'FLOAT',
    'unidadeMedida': 'STRING',
    'valorUnitarioEstimado': 'FLOAT',
    'valorTotal': 'FLOAT',
    'temResultado': 'BOOLEAN',
    'situacaoCompraItemNome': 'STRING',
    'cnpjFornecedor': 'STRING',
    'nomeFornecedor': 'STRING',
    'data_extracao': 'TIMESTAMP'
}

RESULTADOS_SCHEMA = {
    'idCompraItem': 'STRING',
    'idCompra': 'STRING',
    'niFornecedor': 'STRING',
    'tipoPessoa': 'STRING',
    'nomeRazaoSocialFornecedor': 'STRING',
    'naturezaJuridicaNome': 'STRING',
    'porteFornecedorNome': 'STRING',
    'quantidadeHomologada': 'FLOAT',
    'valorUnitarioHomologado': 'FLOAT',
    'valorTotalHomologado': 'FLOAT',
    'percentualDesconto': 'FLOAT',
    'dataResultadoPncp': 'TIMESTAMP',
    'aplicacaoBeneficioMeepp': 'BOOLEAN',
    'moedaEstrangeiraId': 'STRING',
    'dataCotacaoMoedaEstrangeira': 'TIMESTAMP',
    'valorNominalMoedaEstrangeira': 'FLOAT',
    'paisOrigemProdutoServicoId': 'INTEGER',
    'data_extracao': 'TIMESTAMP'
}

BQ_CLIENT = None

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

def convert_column_type(series, target_type):
    """Converte uma coluna pandas para o tipo especificado no schema do BigQuery."""
    try:
        if target_type == 'STRING':
            return series.astype(str).replace(['nan', 'None', '<NA>'], None)
        elif target_type == 'INTEGER':
            return pd.to_numeric(series, errors='coerce').fillna(0).astype('Int64')
        elif target_type == 'FLOAT':
            return pd.to_numeric(series, errors='coerce')
        elif target_type == 'BOOLEAN':
            return series.astype(bool)
        elif target_type == 'TIMESTAMP':
            return pd.to_datetime(series, errors='coerce')
        else:
            return series
    except Exception as e:
        logging.warning(f"Erro ao converter coluna para tipo {target_type}: {e}. Retornando como string.")
        return series.astype(str)

def map_and_clean_dataframe(df, schema):
    """Mapeia campos e converte tipos baseado no schema da tabela."""
    if df.empty:
        return df
    
    # Seleciona apenas campos que existem no schema E no DataFrame
    available_fields = [field for field in schema.keys() if field in df.columns]
    df_mapped = df[available_fields].copy()
    
    # Converte cada coluna para o tipo correto
    for col, target_type in schema.items():
        if col in df_mapped.columns:
            df_mapped[col] = convert_column_type(df_mapped[col], target_type)
    
    # Adiciona timestamp de extração
    if 'data_extracao' in schema and 'data_extracao' not in df_mapped.columns:
        df_mapped['data_extracao'] = datetime.utcnow()
    if 'dataExtracao' in schema and 'dataExtracao' not in df_mapped.columns:
        df_mapped['dataExtracao'] = datetime.utcnow()
    
    return df_mapped

def load_data_to_bigquery(df, table_name, schema):
    """Carrega dados usando o cliente nativo do BigQuery."""
    if df.empty: 
        logging.info(f"DataFrame vazio para '{table_name}', nada a carregar.")
        return
    
    table_id = f"{CONFIG['GCP_PROJECT_ID']}.{CONFIG['BIGQUERY_DATASET']}.{table_name}"
    
    try:
        df_clean = map_and_clean_dataframe(df, schema)
        
        logging.info(f"Carregando {len(df_clean)} registro(s) com {len(df_clean.columns)} campos na tabela '{table_name}'...")
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=False
        )
        
        job = BQ_CLIENT.load_table_from_dataframe(
            df_clean,
            table_id,
            job_config=job_config
        )
        
        job.result()
        
        logging.info(f"✓ {len(df_clean)} registro(s) carregado(s) com sucesso na tabela '{table_name}'.")
    except Exception as e:
        logging.error(f"Falha ao carregar dados na tabela '{table_name}': {e}")
        if hasattr(e, 'errors'):
            logging.error(f"Erros detalhados do BigQuery: {e.errors}")
        raise

def process_single_id(pncp_id):
    try:
        contratacoes_data = get_pncp_data("CONTRATACOES", pncp_id)
        if not contratacoes_data or not contratacoes_data.get('resultado'):
            logging.warning(f"Nenhum dado de contratação encontrado para ID {pncp_id}. Pulando.")
            return

        contratacoes_list = contratacoes_data.get('resultado', [])
        itens_data = get_pncp_data("ITENS", pncp_id)
        resultados_data = get_pncp_data("RESULTADOS", pncp_id)

        if contratacoes_list:
            df = pd.json_normalize(contratacoes_list)
            logging.info(f"ID {pncp_id}: {len(df)} registro(s) de contratação encontrado(s).")
            load_data_to_bigquery(df, 'compras', COMPRAS_SCHEMA)

        if itens_data and itens_data.get('resultado'):
            df = pd.json_normalize(itens_data.get('resultado', []))
            logging.info(f"ID {pncp_id}: {len(df)} item(ns) de compra encontrado(s).")
            load_data_to_bigquery(df, 'itens_compra', ITENS_SCHEMA)

        if resultados_data and resultados_data.get('resultado'):
            df = pd.json_normalize(resultados_data.get('resultado', []))
            logging.info(f"ID {pncp_id}: {len(df)} resultado(s) de item encontrado(s).")
            load_data_to_bigquery(df, 'resultados_itens', RESULTADOS_SCHEMA)

    except Exception as e:
        logging.warning(f"Falha ao processar completamente o ID {pncp_id}.")
        raise e

def main():
    global BQ_CLIENT
    
    logging.info("--- INICIANDO PIPELINE DE ETL DO PNCP ---")
    
    try:
        creds, project_id = google.auth.default()
        scoped_creds = creds.with_scopes([
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ])
        
        gc = gspread.authorize(scoped_creds)
        BQ_CLIENT = bigquery.Client(credentials=creds, project=project_id or CONFIG["GCP_PROJECT_ID"])

        logging.info("Lendo IDs da planilha Google Sheets...")
        sheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])
        all_rows = sheet.get_all_values()
        ids_from_sheet = [row[0] for row in all_rows[1:] if row and row[0].strip()]
        logging.info(f"Encontrados {len(ids_from_sheet)} IDs na planilha.")

        query = f"SELECT DISTINCT CAST(idCompra AS STRING) as idCompra FROM `{CONFIG['GCP_PROJECT_ID']}.{CONFIG['BIGQUERY_DATASET']}.compras`"
        existing_ids = set()
        try:
            df_existing = BQ_CLIENT.query(query).to_dataframe()
            if not df_existing.empty:
                existing_ids = set(df_existing['idCompra'])
        except exceptions.NotFound:
            logging.warning("Tabela 'compras' não encontrada ou vazia. Assumindo primeira execução.")
        
        ids_to_process = [id_val for id_val in ids_from_sheet if id_val not in existing_ids]
        logging.info(f"Após filtragem, {len(ids_to_process)} IDs a serem processados.")
        
    except Exception as e:
        logging.critical(f"Falha na configuração inicial. Abortando. Erro: {e}")
        return

    retry_counts = {item: 0 for item in ids_to_process}
    consecutive_failures = 0
    
    while ids_to_process:
        batch = ids_to_process[:CONFIG["BATCH_SIZE"]]
        if not batch: break
        
        logging.info(f"--- Processando lote de {len(batch)} IDs: {batch} ---")
        batch_success = False
        
        for item in list(batch):
            if item not in ids_to_process: continue

            retry_counts[item] += 1
            try:
                logging.info(f"Processando ID: {item} (Tentativa {retry_counts[item]})")
                process_single_id(item)
                ids_to_process.remove(item)
                batch_success = True
                consecutive_failures = 0
                time.sleep(CONFIG["SUCCESS_DELAY_SECONDS"])
            except Exception:
                logging.warning(f"Falha na tentativa {retry_counts[item]} para o ID: {item}.")
                if retry_counts[item] >= CONFIG["MAX_RETRIES_PER_ITEM"]:
                    logging.error(f"ID {item} atingiu o máximo de falhas e será descartado.")
                    ids_to_process.remove(item)
        
        if not batch_success:
            consecutive_failures += 1
            logging.warning(f"Lote inteiro falhou. Contagem consecutiva: {consecutive_failures}")
            delay = CONFIG["RETRY_DELAYS_SECONDS"].get(consecutive_failures)
            if delay == "CANCEL":
                logging.critical("Máximo de falhas consecutivas atingido. Abortando.")
                break
            if delay:
                logging.info(f"Aguardando {delay}s antes da próxima tentativa...")
                time.sleep(delay)
    
    logging.info("--- PIPELINE DE ETL DO PNCP CONCLUÍDO ---")

if __name__ == "__main__":
    main()
