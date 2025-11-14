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
    "SHEET_NAME": "idLista",
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
    "MAX_RETRIES_PER_ITEM": 3,
    "RETRY_DELAYS_SECONDS": {3: 5, 6: 10, 9: 60, 12: 300, 15: 600, 18: "CANCEL"}
}

# Status finalizados (case insensitive)
STATUS_FINALIZADOS = {"homologado", "fracassado", "deserto", "anulado/revogado/cancelado"}

# Lock para operações thread-safe
db_lock = Lock()

# =====================================================
# LOGGING
# =====================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =====================================================
# SCHEMAS (TODOS EM MINÚSCULAS)
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
    "itensoutros": "INTEGER",
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
    "nomefornecedor": "STRING",
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
    "aplicacaobeneficiomeepp": "BOOLEAN",
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
# INICIALIZAÇÃO DO BANCO
# =====================================================

def init_control_tables():
    """Cria tabelas de controle se não existirem"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Tabela de controle para IDs de precos_catalogo já processados
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS precos_catalogo_processados (
                idcompra STRING PRIMARY KEY,
                data_processamento TIMESTAMP DEFAULT current_timestamp()
            )
        """)
        
        # Adicionar coluna 'origem' nas tabelas principais se não existir
        for table in ['compras', 'itens_compra', 'resultados_itens']:
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table}' AND column_name = 'origem'
            """)
            
            if not cursor.fetchone():
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN origem STRING DEFAULT 'SMS'")
                logger.info(f"Coluna 'origem' adicionada à tabela {table}")
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Tabelas de controle inicializadas")
        
    except Exception as e:
        logger.error(f"Erro ao inicializar tabelas de controle: {e}")
        raise

# =====================================================
# FUNÇÕES DE CONSTRUÇÃO DA LISTA DE IDS
# =====================================================

def get_ids_from_sheets() -> List[str]:
    """Extrai IDs do Google Sheets"""
    try:
        credentials, _ = default(scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
        gc = gspread.authorize(credentials)
        sheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])
        all_rows = sheet.get_all_values()
        ids_list = [row[0].strip() for row in all_rows[1:] if row and row[0].strip()]
        
        logger.info(f"{len(ids_list)} IDs encontrados no Google Sheets")
        return ids_list
        
    except Exception as e:
        logger.error(f"Erro ao ler Google Sheets: {e}")
        return []

def check_if_compra_finalizada(idcompra: str, conn) -> bool:
    """
    Verifica se TODOS os itens de uma compra estão finalizados
    Retorna True se finalizada (não deve buscar), False se pendente (deve buscar)
    """
    cursor = conn.cursor()
    
    # Buscar todos os idcompraitem e suas situações
    cursor.execute("""
        SELECT situacaocompraitemnome 
        FROM itens_compra 
        WHERE idcompra = %s
    """, (idcompra,))
    
    rows = cursor.fetchall()
    cursor.close()
    
    # Se não há itens no banco, deve buscar
    if not rows:
        return False
    
    # Verificar se TODOS estão finalizados
    for (situacao,) in rows:
        if situacao is None:
            return False
        if situacao.lower().strip() not in STATUS_FINALIZADOS:
            return False
    
    # Todos os itens estão finalizados
    return True

def get_filtered_sheets_ids() -> List[Tuple[str, str]]:
    """
    Retorna lista de (idcompra, origem) do Google Sheets
    Filtra apenas IDs que têm pelo menos 1 item não finalizado
    """
    ids_sheets = get_ids_from_sheets()
    if not ids_sheets:
        return []
    
    conn = get_db_connection()
    filtered_ids = []
    
    for idcompra in ids_sheets:
        if not check_if_compra_finalizada(idcompra, conn):
            filtered_ids.append((idcompra, "SMS"))
            logger.debug(f"ID {idcompra} incluído (tem itens pendentes)")
        else:
            logger.debug(f"ID {idcompra} pulado (todos itens finalizados)")
    
    conn.close()
    
    logger.info(f"{len(filtered_ids)} IDs do Sheets após filtro de finalização")
    return filtered_ids

def get_new_precos_catalogo_ids() -> List[Tuple[str, str]]:
    """
    Retorna lista de (idcompra, origem) de precos_catalogo
    Apenas IDs que NUNCA foram processados
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Buscar IDs distintos de precos_catalogo que não estão processados
        cursor.execute("""
            SELECT DISTINCT pc.idcompra
            FROM precos_catalogo pc
            LEFT JOIN precos_catalogo_processados pcp 
                ON pc.idcompra = pcp.idcompra
            WHERE pcp.idcompra IS NULL
        """)
        
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        ids_list = [(row[0], "outras fontes") for row in rows]
        logger.info(f"{len(ids_list)} IDs novos encontrados em precos_catalogo")
        
        return ids_list
        
    except Exception as e:
        logger.error(f"Erro ao buscar IDs de precos_catalogo: {e}")
        return []

def get_idcompraitems_from_precos_catalogo(idcompra: str) -> Set[str]:
    """Retorna set de idcompraitem presentes em precos_catalogo para um idcompra"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT DISTINCT idcompraitem 
            FROM precos_catalogo 
            WHERE idcompra = %s
        """, (idcompra,))
        
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return {row[0] for row in rows}
        
    except Exception as e:
        logger.error(f"Erro ao buscar idcompraitems: {e}")
        return set()

def build_search_list() -> List[Tuple[str, str]]:
    """
    Constrói lista final de IDs para buscar
    Retorna lista de tuplas (idcompra, origem)
    """
    logger.info("Construindo lista de IDs para buscar...")
    
    # 1. IDs do Google Sheets (filtrados por finalização)
    sheets_ids = get_filtered_sheets_ids()
    
    # 2. IDs de precos_catalogo (não processados)
    catalogo_ids = get_new_precos_catalogo_ids()
    
    # 3. Combinar listas
    all_ids = sheets_ids + catalogo_ids
    
    # 4. Remover duplicatas mantendo a primeira ocorrência (prioriza SMS)
    seen = set()
    unique_ids = []
    for idcompra, origem in all_ids:
        if idcompra not in seen:
            seen.add(idcompra)
            unique_ids.append((idcompra, origem))
    
    logger.info(f"Lista final: {len(unique_ids)} IDs únicos para buscar")
    logger.info(f"  - SMS: {sum(1 for _, o in unique_ids if o == 'SMS')}")
    logger.info(f"  - Outras fontes: {sum(1 for _, o in unique_ids if o == 'outras fontes')}")
    
    return unique_ids

def mark_catalogo_as_processed(idcompra: str):
    """Marca um ID de precos_catalogo como processado"""
    try:
        with db_lock:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO precos_catalogo_processados (idcompra, data_processamento)
                VALUES (%s, %s)
                ON CONFLICT (idcompra) DO NOTHING
            """, (idcompra, datetime.utcnow()))
            
            conn.commit()
            cursor.close()
            conn.close()
        
    except Exception as e:
        logger.error(f"Erro ao marcar ID {idcompra} como processado: {e}")

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
            result = series.astype(str).replace(['nan', 'None', '<NA>', ''], None)
            if result is not None and hasattr(result, 'str'):
                result = result.str.replace(r'\.0$', '', regex=True)
            return result
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

def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nomes de colunas removendo pontos e convertendo para minúsculas"""
    df.columns = df.columns.str.replace('.', '', regex=False).str.lower()
    return df

def map_and_clean_dataframe(df: pd.DataFrame, schema: Dict[str, str]) -> pd.DataFrame:
    """Mapeia e limpa DataFrame conforme schema"""
    if df.empty:
        return pd.DataFrame(columns=list(schema.keys()) + ['data_extracao', 'origem'])
    
    df = normalize_column_names(df)
    result_df = pd.DataFrame()
    
    for col, dtype in schema.items():
        if col in df.columns:
            result_df[col] = convert_column_type(df[col], dtype)
        else:
            result_df[col] = None
            logger.debug(f"Coluna {col} não encontrada no DataFrame")
    
    result_df['data_extracao'] = datetime.utcnow()
    
    return result_df

# =====================================================
# FUNÇÕES DE CARGA
# =====================================================

def load_data_to_cockroach(df: pd.DataFrame, table_name: str, schema: Dict[str, str], origem: str) -> bool:
    """Carrega dados no CockroachDB com origem e UPSERT inteligente"""
    if df.empty:
        logger.warning(f"DataFrame vazio para tabela {table_name}")
        return False
    
    try:
        # Adicionar coluna origem
        df['origem'] = origem
        
        with db_lock:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            columns = list(schema.keys()) + ['data_extracao', 'origem']
            placeholders = ', '.join(['%s'] * len(columns))
            columns_str = ', '.join(columns)
            
            conflict_column = "idcompraitem" if table_name != "compras" else "idcompra"
            
            # SET clauses que só atualizam se NULL ou diferente
            set_clauses = ', '.join([
                f'{col} = CASE WHEN {table_name}.{col} IS NULL OR {table_name}.{col} != EXCLUDED.{col} THEN EXCLUDED.{col} ELSE {table_name}.{col} END'
                for col in columns if col != conflict_column
            ])
            
            insert_query = f"""
                INSERT INTO {table_name} ({columns_str})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_column})
                DO UPDATE SET {set_clauses}
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
            conn.close()
        return False

# =====================================================
# PROCESSAMENTO PARALELO COM RETRY
# =====================================================

def call_api_with_retry(endpoint_key: str, pncp_id: str, schema: Dict[str, str], 
                        table_name: str, origem: str, filter_items: Optional[Set[str]] = None) -> Tuple[str, bool, Optional[str]]:
    """
    Chama API com retry automático em caso de falha
    Retorna: (endpoint_key, sucesso, mensagem_erro)
    """
    try:
        # Tentativa 1
        data = get_pncp_data(endpoint_key, pncp_id)
        
        if not data or not data.get('resultado'):
            logger.warning(f"Tentativa 1 falhou para {endpoint_key} - ID {pncp_id}. Aguardando {CONFIG['RETRY_DELAY_SECONDS']}s...")
            time.sleep(CONFIG['RETRY_DELAY_SECONDS'])
            
            # Tentativa 2 (retry)
            data = get_pncp_data(endpoint_key, pncp_id)
            
            if not data or not data.get('resultado'):
                error_msg = f"Falha após retry: {endpoint_key} - ID {pncp_id}"
                logger.error(error_msg)
                return (endpoint_key, False, error_msg)
        
        # Sucesso na obtenção dos dados
        df = pd.json_normalize(data.get('resultado', []))
        df = map_and_clean_dataframe(df, schema)
        
        # Aplicar filtro se necessário
        if filter_items is not None and not df.empty and 'idcompraitem' in df.columns:
            df = df[df['idcompraitem'].isin(filter_items)]
            logger.info(f"{endpoint_key} - Itens após filtro: {len(df)}")
        
        if not df.empty:
            success = load_data_to_cockroach(df, table_name, schema, origem)
            if success:
                logger.info(f"✓ {endpoint_key} processado com sucesso para ID {pncp_id}")
                return (endpoint_key, True, None)
            else:
                error_msg = f"Falha ao salvar dados: {endpoint_key} - ID {pncp_id}"
                return (endpoint_key, False, error_msg)
        else:
            logger.info(f"Sem dados para {endpoint_key} - ID {pncp_id}")
            return (endpoint_key, True, None)
        
    except Exception as e:
        error_msg = f"Exceção em {endpoint_key} - ID {pncp_id}: {str(e)}"
        logger.error(error_msg)
        return (endpoint_key, False, error_msg)

def call_api_without_retry(endpoint_key: str, pncp_id: str, schema: Dict[str, str], 
                           table_name: str, origem: str, filter_items: Optional[Set[str]] = None) -> Tuple[str, bool, Optional[str]]:
    """
    Chama API SEM retry (para segunda passagem)
    Retorna: (endpoint_key, sucesso, mensagem_erro)
    """
    try:
        data = get_pncp_data(endpoint_key, pncp_id)
        
        if not data or not data.get('resultado'):
            error_msg = f"Falha na segunda passagem: {endpoint_key} - ID {pncp_id}"
            logger.error(error_msg)
            return (endpoint_key, False, error_msg)
        
        # Sucesso na obtenção dos dados
        df = pd.json_normalize(data.get('resultado', []))
        df = map_and_clean_dataframe(df, schema)
        
        # Aplicar filtro se necessário
        if filter_items is not None and not df.empty and 'idcompraitem' in df.columns:
            df = df[df['idcompraitem'].isin(filter_items)]
        
        if not df.empty:
            success = load_data_to_cockroach(df, table_name, schema, origem)
            if success:
                logger.info(f"✓ {endpoint_key} processado com sucesso na 2ª passagem - ID {pncp_id}")
                return (endpoint_key, True, None)
            else:
                error_msg = f"Falha ao salvar dados na 2ª passagem: {endpoint_key} - ID {pncp_id}"
                return (endpoint_key, False, error_msg)
        else:
            return (endpoint_key, True, None)
        
    except Exception as e:
        error_msg = f"Exceção na 2ª passagem - {endpoint_key} - ID {pncp_id}: {str(e)}"
        logger.error(error_msg)
        return (endpoint_key, False, error_msg)

def process_single_id(pncp_id: str, origem: str, apis_to_process: Optional[List[str]] = None, 
                      allow_retry: bool = True) -> Tuple[bool, List[str]]:
    """
    Processa um único ID do PNCP com chamadas paralelas
    Retorna: (sucesso_total, lista_de_apis_falhadas)
    """
    try:
        logger.info(f"Processando ID: {pncp_id} (origem: {origem})")
        
        # Determinar quais APIs processar
        if apis_to_process is None:
            apis_to_process = ["CONTRATACOES", "ITENS", "RESULTADOS"]
        
        # Determinar filtro de itens
        filter_items = None
        if origem == "outras fontes":
            filter_items = get_idcompraitems_from_precos_catalogo(pncp_id)
            if filter_items:
                logger.info(f"Filtrando {len(filter_items)} idcompraitems de precos_catalogo")
        
        # Mapeamento de APIs para schemas e tabelas
        api_config = {
            "CONTRATACOES": (COMPRAS_SCHEMA, "compras", None),
            "ITENS": (ITENS_SCHEMA, "itens_compra", filter_items),
            "RESULTADOS": (RESULTADOS_SCHEMA, "resultados_itens", filter_items)
        }
        
        failed_apis = []
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            
            # Disparar chamadas com intervalo de 1s
            for i, api_key in enumerate(apis_to_process):
                if i > 0:
                    time.sleep(CONFIG['API_INTERVAL_SECONDS'])
                
                schema, table, filter_set = api_config[api_key]
                future = executor.submit(
                    call_api_with_retry if allow_retry else call_api_without_retry,
                    api_key, pncp_id, schema, table, origem, filter_set
                )
                futures.append(future)
            
            # Aguardar todas as respostas
            for future in as_completed(futures):
                api_key, success, error_msg = future.result()
                if not success:
                    failed_apis.append(api_key)
                    if error_msg:
                        logger.error(error_msg)
        
        # Marcar como processado se origem = "outras fontes" e sucesso total
        if origem == "outras fontes" and len(failed_apis) == 0:
            mark_catalogo_as_processed(pncp_id)
        
        success_total = len(failed_apis) == 0
        return (success_total, failed_apis)
        
    except Exception as e:
        logger.error(f"Erro ao processar ID {pncp_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return (False, apis_to_process if apis_to_process else ["CONTRATACOES", "ITENS", "RESULTADOS"])

# =====================================================
# FUNÇÃO PRINCIPAL
# =====================================================

def main():
    """Orquestração principal do ETL"""
    logger.info("Iniciando ETL Pipeline PNCP → CockroachDB")
    
    try:
        # 0. Inicializar tabelas de controle
        init_control_tables()
        
        # 1. Construir lista de IDs para buscar
        search_list = build_search_list()
        
        if not search_list:
            logger.info("Nenhum ID para processar")
            return
        
        # Dicionário para rastrear falhas
        failed_ids = {}
        
        # 2. PRIMEIRA PASSAGEM
        logger.info("=== INICIANDO PRIMEIRA PASSAGEM ===")
        
        for idcompra, origem in search_list:
            try:
                success, failed_apis = process_single_id(idcompra, origem)
                
                if not success:
                    failed_ids[idcompra] = (origem, failed_apis)
                    apis_str = ', '.join(failed_apis)
                    logger.warning(f"ID {idcompra} teve falhas em: {apis_str}")
                else:
                    logger.info(f"✓ ID {idcompra} processado com sucesso total")
                
                time.sleep(CONFIG['SUCCESS_DELAY_SECONDS'])
                
            except Exception as e:
                logger.error(f"Erro ao processar ID {idcompra}: {e}")
                failed_ids[idcompra] = (origem, ["CONTRATACOES", "ITENS", "RESULTADOS"])
        
        # 3. SEGUNDA PASSAGEM
        if failed_ids:
            logger.info("\n=== INICIANDO SEGUNDA PASSAGEM ===")
            logger.info(f"Reprocessando {len(failed_ids)} IDs com falhas")
            
            final_errors = []
            
            for idcompra, (origem, failed_apis) in failed_ids.items():
                try:
                    apis_str = ', '.join(failed_apis)
                    logger.info(f"Reprocessando ID {idcompra} - APIs: {apis_str}")
                    
                    success, still_failed = process_single_id(
                        idcompra, 
                        origem, 
                        apis_to_process=failed_apis,
                        allow_retry=False
                    )
                    
                    if not success:
                        error_entry = {
                            'idcompra': idcompra,
                            'origem': origem,
                            'apis_falhadas': still_failed
                        }
                        final_errors.append(error_entry)
                        apis_str2 = ', '.join(still_failed)
                        logger.error(f"ERRO FINAL - ID: {idcompra}, APIs falhadas: {apis_str2}, origem: {origem}")
                    else:
                        logger.info(f"✓ ID {idcompra} recuperado com sucesso na 2ª passagem")
                    
                    time.sleep(CONFIG['SUCCESS_DELAY_SECONDS'])
                    
                except Exception as e:
                    logger.error(f"Erro na 2ª passagem para ID {idcompra}: {e}")
                    final_errors.append({
                        'idcompra': idcompra,
                        'origem': origem,
                        'apis_falhadas': failed_apis,
                        'erro': str(e)
                    })
            
            # Log final de erros
            if final_errors:
                logger.error("\n=== RESUMO DE ERROS FINAIS ===")
                for error in final_errors:
                    apis_str = ', '.join(error['apis_falhadas'])
                    logger.error(msg)
        
        # 4. Estatísticas finais
        total_ids = len(search_list)
        total_success = total_ids - len(failed_ids)
        partial_success = len(failed_ids) - len(final_errors) if failed_ids else 0
        total_failed = len(final_errors) if failed_ids else 0
        
        logger.info("\n=== ETL Pipeline concluído ===")
        logger.info(f"Total de IDs: {total_ids}")
        logger.info(f"Sucesso total (1ª passagem): {total_success}")
        logger.info(f"Recuperados (2ª passagem): {partial_success}")
        logger.info(f"Falhas finais: {total_failed}")
        
    except Exception as e:
        logger.error(f"Erro no pipeline principal: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise

if __name__ == "__main__":
    main()
