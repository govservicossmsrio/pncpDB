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
from typing import Dict, Optional, List

# =====================================================
# CONFIGURAÇÃO
# =====================================================

CONFIG = {
    "COCKROACH_CONNECTION_STRING": os.getenv(
        "COCKROACH_CONNECTION_STRING",
        "postgresql://sgc_admin:<password>@scary-quetzal-18026.j77.aws-us-east-1.cockroachlabs.cloud:26257/defaultdb?sslmode=require"
    ),
    "SPREADSHEET_ID": os.getenv("SPREADSHEET_ID", "18P9l9_g-QE-DWsfRCokY18M5RLZe7mV-CWY1bfw6hlA"),
    "SHEET_NAME": "idLista",
    "PNCP_BASE_URL": "https://dadosabertos.compras.gov.br",
    "ENDPOINTS": {
        "CONTRATACOES": "modulo-contratacoes/1.1_consultarContratacoes_PNCP_14133_Id",
        "ITENS": "modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id",
        "RESULTADOS": "modulo-contratacoes/3.1_consultarResultadoItensContratacoes_PNCP_14133_Id"
    },
    "BATCH_SIZE": 50,
    "SUCCESS_DELAY_SECONDS": 2,
    "MAX_RETRIES_PER_ITEM": 3,
    "RETRY_DELAYS_SECONDS": {3: 5, 6: 10, 9: 60, 12: 300, 15: 600, 18: "CANCEL"}
}

# =====================================================
# LOGGING
# =====================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =====================================================
# SCHEMAS
# =====================================================

COMPRAS_SCHEMA = {
    "idCompra": "STRING",
    "numeroCompra": "STRING",
    "anoCompraPncp": "INTEGER",
    "codigoModalidade": "INTEGER",
    "modalidadeNome": "STRING",
    "srp": "BOOLEAN",
    "unidadeOrgaoCodigoUnidade": "STRING",
    "unidadeOrgaoNomeUnidade": "STRING",
    "unidadeOrgaoMunicipioNome": "STRING",
    "unidadeOrgaoUfSigla": "STRING",
    "processo": "STRING",
    "objetoCompra": "STRING",
    "valorTotalEstimado": "FLOAT",
    "valorTotalHomologado": "FLOAT",
    "existeResultado": "BOOLEAN",
    "dataAberturaPropostaPncp": "TIMESTAMP",
    "contratacaoExcluida": "BOOLEAN",
    "itensTotal": "INTEGER",
    "itensResultados": "INTEGER",
    "itensHomologados": "INTEGER",
    "itensFracassados": "INTEGER",
    "itensDesertos": "INTEGER",
    "itensOutros": "INTEGER",
}

ITENS_SCHEMA = {
    "idCompraItem": "STRING",
    "idCompra": "STRING",
    "numeroItemCompra": "INTEGER",
    "numeroGrupo": "INTEGER",
    "materialOuServicoNome": "STRING",
    "tipoBeneficioNome": "STRING",
    "codItemCatalogo": "STRING",
    "descricaoResumida": "STRING",
    "descricaodetalhada": "STRING",
    "quantidade": "FLOAT",
    "unidadeMedida": "STRING",
    "valorUnitarioEstimado": "FLOAT",
    "valorTotal": "FLOAT",
    "temResultado": "BOOLEAN",
    "situacaoCompraItemNome": "STRING",
    "cnpjFornecedor": "STRING",
    "nomeFornecedor": "STRING",
}

RESULTADOS_SCHEMA = {
    "idCompraItem": "STRING",
    "idCompra": "STRING",
    "niFornecedor": "STRING",
    "tipoPessoa": "STRING",
    "nomeRazaoSocialFornecedor": "STRING",
    "naturezaJuridicaNome": "STRING",
    "porteFornecedorNome": "STRING",
    "quantidadeHomologada": "FLOAT",
    "valorUnitarioHomologado": "FLOAT",
    "valorTotalHomologado": "FLOAT",
    "percentualDesconto": "FLOAT",
    "dataResultadoPncp": "TIMESTAMP",
    "aplicacaoBeneficioMeepp": "BOOLEAN",
}

# =====================================================
# CONEXÃO COM COCKROACHDB
# =====================================================

def get_db_connection():
    """Cria conexão com CockroachDB"""
    try:
        conn = psycopg2.connect(CONFIG["COCKROACH_CONNECTION_STRING"])
        return conn
    except Exception as e:
        logger.error(f"Erro ao conectar com CockroachDB: {e}")
        raise

# =====================================================
# FUNÇÕES DE EXTRAÇÃO
# =====================================================

def get_pncp_data(endpoint_key: str, id_param: str) -> Optional[Dict]:
    """Extrai dados da API PNCP usando query parameters"""
    try:
        url = f"{CONFIG['PNCP_BASE_URL']}/{CONFIG['ENDPOINTS'][endpoint_key]}"
        params = {'tipo': 'idCompra', 'codigo': id_param}
        
        logger.info(f"Chamando: {url} com params: {params}")
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        if not response.content:
            logger.warning(f"Resposta vazia para {endpoint_key} - ID {id_param}")
            return None
        
        return response.json()
        
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"Erro HTTP para ID {id_param}. Status: {http_err.response.status_code}")
        logger.error(f"Resposta: {http_err.response.text}")
        return None
    except Exception as e:
        logger.error(f"Erro ao acessar {endpoint_key}: {e}")
        return None

# =====================================================
# FUNÇÕES DE TRANSFORMAÇÃO
# =====================================================

def convert_column_type(series: pd.Series, target_type: str) -> pd.Series:
    """Converte tipos de dados"""
    try:
        if target_type == "STRING":
            return series.astype(str).replace(['nan', 'None', '<NA>', ''], None)
        elif target_type == "INTEGER":
            return pd.to_numeric(series, errors='coerce').astype('Int64')
        elif target_type == "FLOAT":
            return pd.to_numeric(series, errors='coerce')
        elif target_type == "BOOLEAN":
            return series.astype(bool)
        elif target_type == "TIMESTAMP":
            return pd.to_datetime(series, errors='coerce', utc=True)
        else:
            return series
    except Exception as e:
        logger.warning(f"Erro ao converter coluna: {e}")
        return series

def map_and_clean_dataframe(df: pd.DataFrame, schema: Dict[str, str]) -> pd.DataFrame:
    """Mapeia e limpa DataFrame conforme schema"""
    if df.empty:
        return pd.DataFrame(columns=list(schema.keys()) + ['data_extracao'])
    
    result_df = pd.DataFrame()
    
    # Seleciona apenas campos que existem no schema E no DataFrame
    for col, dtype in schema.items():
        if col in df.columns:
            result_df[col] = convert_column_type(df[col], dtype)
        else:
            result_df[col] = None
    
    result_df['data_extracao'] = datetime.utcnow()
    
    return result_df

# =====================================================
# FUNÇÕES DE CARGA
# =====================================================

def load_data_to_cockroach(df: pd.DataFrame, table_name: str, schema: Dict[str, str]) -> bool:
    """Carrega dados no CockroachDB"""
    if df.empty:
        logger.warning(f"DataFrame vazio para tabela {table_name}")
        return False
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        columns = list(schema.keys()) + ['data_extracao']
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        # Determina a coluna de conflito
        conflict_column = "idCompraItem" if table_name != "compras" else "idCompra"
        
        insert_query = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_column})
            DO UPDATE SET data_extracao = EXCLUDED.data_extracao
        """
        
        data_tuples = [tuple(row) for row in df[columns].replace({np.nan: None}).values]
        
        execute_batch(cursor, insert_query, data_tuples, page_size=1000)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"✓ {len(df)} registros inseridos/atualizados em {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao inserir em {table_name}: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False

# =====================================================
# PROCESSAMENTO DE IDS
# =====================================================

def process_single_id(pncp_id: str) -> bool:
    """Processa um único ID do PNCP"""
    try:
        logger.info(f"Processando ID: {pncp_id}")
        
        # 1. Extração de compras
        contratacoes_data = get_pncp_data("CONTRATACOES", pncp_id)
        
        if not contratacoes_data or not contratacoes_data.get('resultado'):
            logger.warning(f"Sem dados de contratação para ID {pncp_id}")
            return False
        
        # Normaliza dados de compras
        compras_df = pd.json_normalize(contratacoes_data.get('resultado', []))
        compras_df = map_and_clean_dataframe(compras_df, COMPRAS_SCHEMA)
        
        if not load_data_to_cockroach(compras_df, "compras", COMPRAS_SCHEMA):
            return False
        
        # 2. Extração de itens
        itens_data = get_pncp_data("ITENS", pncp_id)
        
        if itens_data and itens_data.get('resultado'):
            itens_df = pd.json_normalize(itens_data.get('resultado', []))
            itens_df = map_and_clean_dataframe(itens_df, ITENS_SCHEMA)
            load_data_to_cockroach(itens_df, "itens_compra", ITENS_SCHEMA)
        
        # 3. Extração de resultados
        resultados_data = get_pncp_data("RESULTADOS", pncp_id)
        
        if resultados_data and resultados_data.get('resultado'):
            resultados_df = pd.json_normalize(resultados_data.get('resultado', []))
            resultados_df = map_and_clean_dataframe(resultados_df, RESULTADOS_SCHEMA)
            load_data_to_cockroach(resultados_df, "resultados_itens", RESULTADOS_SCHEMA)
        
        return True
        
    except Exception as e:
        logger.error(f"Erro ao processar ID {pncp_id}: {e}")
        return False

# =====================================================
# FUNÇÃO PRINCIPAL
# =====================================================

def main():
    """Orquestração principal do ETL"""
    logger.info("Iniciando ETL Pipeline PNCP → CockroachDB")
    
    try:
        # 1. Ler IDs do Google Sheets
        credentials, _ = default(scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
        gc = gspread.authorize(credentials)
        sheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])
        all_rows = sheet.get_all_values()
        ids_list = [row[0] for row in all_rows[1:] if row and row[0].strip()]
        
        logger.info(f"{len(ids_list)} IDs encontrados no Google Sheet")
        
        # 2. Verificar IDs já processados
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT idCompra FROM compras")
        processed_ids = set(row[0] for row in cursor.fetchall())
        cursor.close()
        conn.close()
        
        pending_ids = [id for id in ids_list if id not in processed_ids]
        logger.info(f"{len(pending_ids)} IDs pendentes de processamento")
        
        # 3. Processar IDs em batches
        retry_counts = {item: 0 for item in pending_ids}
        consecutive_failures = 0
        
        while pending_ids:
            batch = pending_ids[:CONFIG["BATCH_SIZE"]]
            if not batch:
                break
            
            logger.info(f"--- Processando lote de {len(batch)} IDs ---")
            batch_success = False
            
            for item in list(batch):
                if item not in pending_ids:
                    continue
                
                retry_counts[item] += 1
                
                try:
                    logger.info(f"ID: {item} (Tentativa {retry_counts[item]})")
                    success = process_single_id(item)
                    
                    if success:
                        pending_ids.remove(item)
                        batch_success = True
                        consecutive_failures = 0
                        time.sleep(CONFIG["SUCCESS_DELAY_SECONDS"])
                    else:
                        if retry_counts[item] >= CONFIG["MAX_RETRIES_PER_ITEM"]:
                            logger.error(f"ID {item} atingiu máximo de tentativas")
                            pending_ids.remove(item)
                            
                except Exception as e:
                    logger.warning(f"Falha na tentativa {retry_counts[item]} para {item}: {e}")
                    if retry_counts[item] >= CONFIG["MAX_RETRIES_PER_ITEM"]:
                        logger.error(f"ID {item} descartado após {CONFIG['MAX_RETRIES_PER_ITEM']} falhas")
                        pending_ids.remove(item)
            
            if not batch_success:
                consecutive_failures += 1
                logger.warning(f"Lote inteiro falhou. Falhas consecutivas: {consecutive_failures}")
                
                delay = CONFIG["RETRY_DELAYS_SECONDS"].get(consecutive_failures)
                if delay == "CANCEL":
                    logger.critical("Máximo de falhas consecutivas atingido. Abortando.")
                    break
                if delay:
                    logger.info(f"Aguardando {delay}s antes da próxima tentativa...")
                    time.sleep(delay)
        
        logger.info("--- ETL Pipeline concluído ---")
        
    except Exception as e:
        logger.error(f"Erro no pipeline principal: {e}")
        raise

if __name__ == "__main__":
    main()
