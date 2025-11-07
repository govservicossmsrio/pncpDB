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
import pandas_gbq

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

# Mapeamento de campos da API para o schema da tabela 'compras'
COMPRAS_FIELD_MAPPING = {
    'idCompra': 'idCompra',
    'numeroCompra': 'numeroCompra',
    'anoCompraPncp': 'anoCompraPncp',
    'codigoModalidade': 'codigoModalidade',
    'modalidadeNome': 'modalidadeNome',
    'srp': 'srp',
    'unidadeOrgaoCodigoUnidade': 'unidadeOrgaoCodigoUnidade',
    'unidadeOrgaoNomeUnidade': 'unidadeOrgaoNomeUnidade',
    'unidadeOrgaoMunicipioNome': 'unidadeOrgaoMunicipioNome',
    'unidadeOrgaoUfSigla': 'unidadeOrgaoUfSigla',
    'orgaoEntidadeEsferaId': 'orgaoEntidadeEsferaId',
    'processo': 'processo',
    'objetoCompra': 'objetoCompra',
    'valorTotalEstimado': 'valorTotalEstimado',
    'valorTotalHomologado': 'valorTotalHomologado',
    'existeResultado': 'existeResultado',
    'dataAberturaPropostaPncp': 'dataAberturaPropostaPncp',
    'contratacaoExcluida': 'contratacaoExcluida'
}

# Mapeamento para 'itens_compra'
ITENS_FIELD_MAPPING = {
    'idCompraItem': 'idCompraItem',
    'idCompra': 'idCompra',
    'numeroItemCompra': 'numeroItemCompra',
    'numeroGrupo': 'numeroGrupo',
    'materialOuServicoNome': 'materialOuServicoNome',
    'tipoBeneficioNome': 'tipoBeneficioNome',
    'codItemCatalogo': 'codItemCatalogo',
    'descricaoResumida': 'descricaoResumida',
    'descricaodetalhada': 'descricaodetalhada',
    'quantidade': 'quantidade',
    'unidadeMedida': 'unidadeMedida',
    'valorUnitarioEstimado': 'valorUnitarioEstimado',
    'valorTotal': 'valorTotal',
    'temResultado': 'temResultado',
    'situacaoCompraItemNome': 'situacaoCompraItemNome',
    'cnpjFornecedor': 'cnpjFornecedor',
    'nomeFornecedor': 'nomeFornecedor'
}

# Mapeamento para 'resultados_itens'
RESULTADOS_FIELD_MAPPING = {
    'idCompraItem': 'idCompraItem',
    'idCompra': 'idCompra',
    'niFornecedor': 'niFornecedor',
    'tipoPessoa': 'tipoPessoa',
    'nomeRazaoSocialFornecedor': 'nomeRazaoSocialFornecedor',
    'naturezaJuridicaNome': 'naturezaJuridicaNome',
    'porteFornecedorNome': 'porteFornecedorNome',
    'quantidadeHomologada': 'quantidadeHomologada',
    'valorUnitarioHomologado': 'valorUnitarioHomologado',
    'valorTotalHomologado': 'valorTotalHomologado',
    'percentualDesconto': 'percentualDesconto',
    'dataResultadoPncp': 'dataResultadoPncp',
    'aplicacaoBeneficioMeepp': 'aplicacaoBeneficioMeepp',
    'moedaEstrangeiraId': 'moedaEstrangeiraId',
    'dataCotacaoMoedaEstrangeira': 'dataCotacaoMoedaEstrangeira',
    'valorNominalMoedaEstrangeira': 'valorNominalMoedaEstrangeira',
    'paisOrigemProdutoServicoId': 'paisOrigemProdutoServicoId'
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

def map_and_clean_dataframe(df, field_mapping):
    """
    Mapeia e limpa o DataFrame para corresponder ao schema da tabela do BigQuery.
    Mantém apenas os campos definidos no mapeamento.
    """
    if df.empty:
        return df
    
    # Seleciona apenas os campos que existem no mapeamento E no DataFrame
    available_fields = {api_field: bq_field for api_field, bq_field in field_mapping.items() if api_field in df.columns}
    
    # Cria um novo DataFrame apenas com os campos mapeados
    df_mapped = df[list(available_fields.keys())].copy()
    
    # Renomeia as colunas conforme o mapeamento (se necessário)
    df_mapped = df_mapped.rename(columns=available_fields)
    
    # Adiciona timestamp de extração se não existir
    if 'data_extracao' in field_mapping.values() and 'data_extracao' not in df_mapped.columns:
        df_mapped['data_extracao'] = datetime.utcnow()
    
    # Converte colunas de data para string (para evitar problemas com timezone)
    date_columns = [col for col in df_mapped.columns if 'data' in col.lower() or 'date' in col.lower()]
    for col in date_columns:
        if col in df_mapped.columns:
            df_mapped[col] = df_mapped[col].astype(str)
    
    return df_mapped

def load_data_to_bigquery(df, table_name, field_mapping):
    if df.empty: 
        logging.info(f"DataFrame vazio para '{table_name}', nada a carregar.")
        return
    
    table_id = f"{CONFIG['GCP_PROJECT_ID']}.{CONFIG['BIGQUERY_DATASET']}.{table_name}"
    
    try:
        # Mapeia e limpa o DataFrame
        df_clean = map_and_clean_dataframe(df, field_mapping)
        
        logging.info(f"Carregando {len(df_clean)} registro(s) com {len(df_clean.columns)} campos na tabela '{table_name}'...")
        
        pandas_gbq.to_gbq(
            df_clean,
            destination_table=table_id,
            project_id=CONFIG["GCP_PROJECT_ID"],
            if_exists='append',
            progress_bar=False
        )
        
        logging.info(f"{len(df_clean)} registro(s) carregado(s) com sucesso na tabela '{table_name}'.")
    except Exception as e:
        logging.error(f"Falha ao carregar dados na tabela '{table_name}': {e}")
        logging.error(f"Colunas no DataFrame limpo: {list(df_clean.columns) if 'df_clean' in locals() else 'N/A'}")
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
            load_data_to_bigquery(df, 'compras', COMPRAS_FIELD_MAPPING)

        if itens_data and itens_data.get('resultado'):
            df = pd.json_normalize(itens_data.get('resultado', []))
            logging.info(f"ID {pncp_id}: {len(df)} item(ns) de compra encontrado(s).")
            load_data_to_bigquery(df, 'itens_compra', ITENS_FIELD_MAPPING)

        if resultados_data and resultados_data.get('resultado'):
            df = pd.json_normalize(resultados_data.get('resultado', []))
            logging.info(f"ID {pncp_id}: {len(df)} resultado(s) de item encontrado(s).")
            load_data_to_bigquery(df, 'resultados_itens', RESULTADOS_FIELD_MAPPING)

    except Exception as e:
        logging.warning(f"Falha ao processar completamente o ID {pncp_id}.")
        raise e

def main():
    logging.info("--- INICIANDO PIPELINE DE ETL DO PNCP ---")
    
    try:
        creds, project_id = google.auth.default()
        scoped_creds = creds.with_scopes([
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ])
        
        gc = gspread.authorize(scoped_creds)
        bq_client = bigquery.Client(credentials=creds, project=project_id or CONFIG["GCP_PROJECT_ID"])

        logging.info("Lendo IDs da planilha Google Sheets...")
        sheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])
        all_rows = sheet.get_all_values()
        ids_from_sheet = [row[0] for row in all_rows[1:] if row and row[0].strip()]
        logging.info(f"Encontrados {len(ids_from_sheet)} IDs na planilha.")

        query = f"SELECT DISTINCT CAST(idCompra AS STRING) as idCompra FROM `{CONFIG['GCP_PROJECT_ID']}.{CONFIG['BIGQUERY_DATASET']}.compras`"
        existing_ids = set()
        try:
            df_existing = bq_client.query(query).to_dataframe()
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
