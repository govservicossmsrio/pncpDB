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
        "postgresql://sgc_admin:<password>@scary-quetzal-18026.j77.aws-us-east-1.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full"
    ),
    "SPREADSHEET_ID": os.getenv("SPREADSHEET_ID", "your-spreadsheet-id"),
    "SHEET_NAME": "Sheet1",
    "PNCP_BASE_URL": "https://pncp.gov.br/api/consulta/v1",
    "ENDPOINTS": {
        "compras": "contratacoes/publicacao/{id_compra}",
        "itens": "contratacoes/{id_compra}/itens",
        "resultados": "contratacoes/{id_compra}/itens/{numero_item}/resultado-item"
    },
    "BATCH_SIZE": 50,
    "SUCCESS_DELAY_SECONDS": 2,
    "MAX_RETRIES_PER_ITEM": 3,
    "RETRY_DELAYS_SECONDS": [5, 10, 30, "CANCEL"]
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
    "anoCompra": "INTEGER",
    "codigoModalidadeCompra": "INTEGER",
    "modalidadeCompra": "STRING",
    "srp": "BOOLEAN",
    "codigoUnidadeCompradora": "STRING",
    "nomeUnidadeCompradora": "STRING",
    "municipioUnidadeCompradora": "STRING",
    "ufUnidadeCompradora": "STRING",
    "processoCompra": "STRING",
    "objetoCompra": "STRING",
    "valorTotalEstimado": "FLOAT",
    "valorTotalHomologado": "FLOAT",
    "possuiResultados": "BOOLEAN",
    "dataAbertura": "TIMESTAMP",
    "dataExclusao": "TIMESTAMP",
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
    "numeroItem": "INTEGER",
    "numeroGrupo": "INTEGER",
    "materialOuServico": "STRING",
    "tipoBeneficio": "STRING",
    "codigoItemCatalogo": "STRING",
    "descricao": "STRING",
    "descricaoDetalhada": "STRING",
    "quantidade": "FLOAT",
    "unidadeMedida": "STRING",
    "valorUnitarioEstimado": "FLOAT",
    "valorTotal": "FLOAT",
    "situacaoResultado": "STRING",
    "situacaoCompraItem": "STRING",
    "cnpjFornecedor": "STRING",
    "nomeFornecedor": "STRING",
}

RESULTADOS_SCHEMA = {
    "idCompraItem": "STRING",
    "idCompra": "STRING",
    "niFornecedor": "STRING",
    "tipoPessoa": "STRING",
    "nomeFornecedor": "STRING",
    "naturezaJuridica": "STRING",
    "porte": "STRING",
    "quantidadeHomologada": "FLOAT",
    "valorUnitarioHomologado": "FLOAT",
    "valorTotalHomologado": "FLOAT",
    "percentualDesconto": "FLOAT",
    "dataResultado": "TIMESTAMP",
    "meepp": "BOOLEAN",
    "moedaEstrangeira": "BOOLEAN",
    "codigoMoeda": "STRING",
    "valorMoedaEstrangeira": "FLOAT",
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

def get_pncp_data(endpoint_key: str, **params) -> Optional[Dict]:
    """Extrai dados da API PNCP"""
    try:
        endpoint_template = CONFIG["ENDPOINTS"][endpoint_key]
        endpoint = endpoint_template.format(**params)
        url = f"{CONFIG['PNCP_BASE_URL']}/{endpoint}"
        
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao acessar {endpoint_key}: {e}")
        return None

# =====================================================
# FUNÇÕES DE TRANSFORMAÇÃO
# =====================================================

def convert_column_type(series: pd.Series, target_type: str) -> pd.Series:
    """Converte tipos de dados"""
    try:
        if target_type == "STRING":
            return series.astype(str).replace(['nan', 'None', ''], None)
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
        
        insert_query = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT ({"idCompraItem" if table_name != "compras" else "idCompra"})
            DO UPDATE SET data_extracao = EXCLUDED.data_extracao
        """
        
        data_tuples = [tuple(row) for row in df[columns].replace({np.nan: None}).values]
        
        execute_batch(cursor, insert_query, data_tuples, page_size=1000)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"{len(df)} registros inseridos/atualizados em {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao inserir em {table_name}: {e}")
        if conn:
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
        compras_data = get_pncp_data("compras", id_compra=pncp_id)
        if not compras_data:
            return False
        
        compras_df = pd.DataFrame([compras_data])
        compras_df = map_and_clean_dataframe(compras_df, COMPRAS_SCHEMA)
        
        if not load_data_to_cockroach(compras_df, "compras", COMPRAS_SCHEMA):
            return False
        
        # 2. Extração de itens
        itens_data = get_pncp_data("itens", id_compra=pncp_id)
        if not itens_data:
            logger.warning(f"Sem itens para ID {pncp_id}")
            return True
        
        itens_df = pd.DataFrame(itens_data)
        itens_df['idCompra'] = pncp_id
        itens_df['idCompraItem'] = itens_df['idCompra'] + '_' + itens_df['numeroItem'].astype(str).str.zfill(5)
        
        itens_df = map_and_clean_dataframe(itens_df, ITENS_SCHEMA)
        
        if not load_data_to_cockroach(itens_df, "itens_compra", ITENS_SCHEMA):
            return False
        
        # 3. Extração de resultados
        resultados_list = []
        for _, item in itens_df.iterrows():
            resultado_data = get_pncp_data(
                "resultados",
                id_compra=pncp_id,
                numero_item=int(item['numeroItem'])
            )
            
            if resultado_data:
                resultado_data['idCompraItem'] = item['idCompraItem']
                resultado_data['idCompra'] = pncp_id
                resultados_list.append(resultado_data)
        
        if resultados_list:
            resultados_df = pd.DataFrame(resultados_list)
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
        ids_list = [row[0] for row in sheet.get_all_values()[1:] if row]
        
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
        for i in range(0, len(pending_ids), CONFIG["BATCH_SIZE"]):
            batch = pending_ids[i:i + CONFIG["BATCH_SIZE"]]
            logger.info(f"Processando batch {i//CONFIG['BATCH_SIZE'] + 1}")
            
            for pncp_id in batch:
                success = process_single_id(pncp_id)
                if success:
                    time.sleep(CONFIG["SUCCESS_DELAY_SECONDS"])
        
        logger.info("ETL Pipeline concluído com sucesso")
        
    except Exception as e:
        logger.error(f"Erro no pipeline principal: {e}")
        raise

if __name__ == "__main__":
    main()
