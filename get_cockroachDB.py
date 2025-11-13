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

# Status finalizados (case insensitive)
STATUS_FINALIZADOS = {"homologado", "fracassado", "deserto", "anulado/revogado/cancelado"}

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
# PROCESSAMENTO DE IDS
# =====================================================

def process_single_id(pncp_id: str, origem: str) -> bool:
    """Processa um único ID do PNCP com filtro de itens se origem = 'outras fontes'"""
    try:
        logger.info(f"Processando ID: {pncp_id} (origem: {origem})")
        
        # Determinar quais idcompraitems filtrar (se aplicável)
        filter_items = None
        if origem == "outras fontes":
            filter_items = get_idcompraitems_from_precos_catalogo(pncp_id)
            logger.info(f"Filtrando {len(filter_items)} idcompraitems de precos_catalogo")
        
        # 1. Extração de contratações (compras) - SEMPRE COMPLETO
        contratacoes_data = get_pncp_data("CONTRATACOES", pncp_id)
        
        if not contratacoes_data or not contratacoes_data.get('resultado'):
            logger.warning(f"Sem dados de contratação para ID {pncp_id}")
            return False
        
        compras_df = pd.json_normalize(contratacoes_data.get('resultado', []))
        compras_df = map_and_clean_dataframe(compras_df, COMPRAS_SCHEMA)
        
        if not load_data_to_cockroach(compras_df, "compras", COMPRAS_SCHEMA, origem):
            return False
        
        # 2. Extração de itens - FILTRADO SE origem = "outras fontes"
        itens_data = get_pncp_data("ITENS", pncp_id)
        
        if itens_data and itens_data.get('resultado'):
            itens_df = pd.json_normalize(itens_data.get('resultado', []))
            itens_df = map_and_clean_dataframe(itens_df, ITENS_SCHEMA)
            
            # Aplicar filtro se necessário
            if filter_items is not None and not itens_df.empty:
                itens_df = itens_df[itens_df['idcompraitem'].isin(filter_items)]
                logger.info(f"Itens após filtro: {len(itens_df)}")
            
            if not itens_df.empty:
                load_data_to_cockroach(itens_df, "itens_compra", ITENS_SCHEMA, origem)
        else:
            logger.info(f"Sem itens para ID {pncp_id}")
        
        # 3. Extração de resultados - FILTRADO SE origem = "outras fontes"
        resultados_data = get_pncp_data("RESULTADOS", pncp_id)
        
        if resultados_data and resultados_data.get('resultado'):
            resultados_df = pd.json_normalize(resultados_data.get('resultado', []))
            resultados_df = map_and_clean_dataframe(resultados_df, RESULTADOS_SCHEMA)
            
            # Aplicar filtro se necessário
            if filter_items is not None and not resultados_df.empty:
                resultados_df = resultados_df[resultados_df['idcompraitem'].isin(filter_items)]
                logger.info(f"Resultados após filtro: {len(resultados_df)}")
            
            if not resultados_df.empty:
                load_data_to_cockroach(resultados_df, "resultados_itens", RESULTADOS_SCHEMA, origem)
        else:
            logger.info(f"Sem resultados para ID {pncp_id}")
        
        # Marcar como processado se origem = "outras fontes"
        if origem == "outras fontes":
            mark_catalogo_as_processed(pncp_id)
        
        return True
        
    except Exception as e:
        logger.error(f"Erro ao processar ID {pncp_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

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
        
        # 2. Processar IDs em batches
        retry_counts = {item[0]: 0 for item in search_list}
        consecutive_failures = 0
        
        while search_list:
            batch = search_list[:CONFIG["BATCH_SIZE"]]
            if not batch:
                break
            
            logger.info(f"--- Processando lote de {len(batch)} IDs ---")
            batch_success = False
            
            for idcompra, origem in list(batch):
                if (idcompra, origem) not in search_list:
                    continue
                
                retry_counts[idcompra] += 1
                
                try:
                    logger.info(f"ID: {idcompra} | Origem: {origem} (Tentativa {retry_counts[idcompra]})")
                    success = process_single_id(idcompra, origem)
                    
                    if success:
                        search_list.remove((idcompra, origem))
                        batch_success = True
                        consecutive_failures = 0
                        time.sleep(CONFIG["SUCCESS_DELAY_SECONDS"])
                    else:
                        if retry_counts[idcompra] >= CONFIG["MAX_RETRIES_PER_ITEM"]:
                            logger.error(f"ID {idcompra} atingiu máximo de tentativas")
                            search_list.remove((idcompra, origem))
                            
                except Exception as e:
                    logger.warning(f"Falha na tentativa {retry_counts[idcompra]} para {idcompra}: {e}")
                    if retry_counts[idcompra] >= CONFIG["MAX_RETRIES_PER_ITEM"]:
                        logger.error(f"ID {idcompra} descartado após {CONFIG['MAX_RETRIES_PER_ITEM']} falhas")
                        search_list.remove((idcompra, origem))
            
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
        
        total_ids = len([item for item in build_search_list()])
        processed = total_ids - len(search_list)
        
        logger.info("--- ETL Pipeline concluído ---")
        logger.info(f"Total processado: {processed}/{total_ids}")
        
    except Exception as e:
        logger.error(f"Erro no pipeline principal: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise

if __name__ == "__main__":
    main()
