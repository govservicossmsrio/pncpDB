import os
import time
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from io import StringIO
import psycopg2
from psycopg2.extras import execute_batch
from typing import Dict, List, Optional, Tuple

# =====================================================
# CONFIGURAÇÃO
# =====================================================

CONFIG = {
    "COCKROACH_CONNECTION_STRING": os.getenv(
        "COCKROACH_CONNECTION_STRING",
        "postgresql://sgc_admin:<password>@scary-quetzal-18026.j77.aws-us-east-1.cockroachlabs.cloud:26257/defaultdb?sslmode=require"
    ),
    "PNCP_BASE_URL": "https://dadosabertos.compras.gov.br",
    "ENDPOINTS": {
        "MATERIAL": "modulo-pesquisa-preco/1.1_consultarMaterial_CSV",
        "SERVICO": "modulo-pesquisa-preco/3.1_consultarServico_CSV"
    },
    "PAGE_SIZE": 200,
    "BATCH_SIZE": 10,
    "SUCCESS_DELAY_SECONDS": 2,
    "MAX_RETRIES_PER_ITEM": 3,
    "RETRY_DELAY_SECONDS": 5,
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
# SCHEMA PRECOS_CATALOGO (MINÚSCULAS)
# =====================================================

PRECOS_SCHEMA = {
    "idcompra": "STRING",
    "iditemcompra": "STRING",
    "forma": "STRING",
    "modalidade": "STRING",
    "criteriojulgamento": "STRING",
    "numeroitemcompra": "INTEGER",
    "descricaoitem": "STRING",
    "codigoitemcatalogo": "STRING",
    "nomeunidademedida": "STRING",
    "siglaunidademedida": "STRING",
    "nomeunidadefornecimento": "STRING",
    "siglaunidadefornecimento": "STRING",
    "capacidadeunidadefornecimento": "FLOAT",
    "quantidade": "FLOAT",
    "precounitario": "FLOAT",
    "percentualmaiordesconto": "FLOAT",
    "nifornecedor": "STRING",
    "nomefornecedor": "STRING",
    "marca": "STRING",
    "codigouasg": "STRING",
    "nomeuasg": "STRING",
    "codigomunicipio": "STRING",
    "municipio": "STRING",
    "estado": "STRING",
    "codigoorgao": "STRING",
    "nomeorgao": "STRING",
    "poder": "STRING",
    "esfera": "STRING",
    "datacompra": "DATE",
    "datahoraatualizacaocompra": "TIMESTAMP",
    "datahoraatualizacaoitem": "TIMESTAMP",
    "dataresultado": "DATE",
    "datahoraatualizacaouasg": "TIMESTAMP",
    "codigoclasse": "STRING",
    "nomeclasse": "STRING",
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
# FUNÇÕES DE BUSCA DE CÓDIGOS
# =====================================================

def get_pending_codes() -> List[Tuple[str, str]]:
    """
    Retorna lista de (coditemcatalogo, tipo) priorizando:
    1. Códigos nunca buscados
    2. Códigos mais antigos (data_extracao ASC)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
        WITH codigos_itens AS (
            SELECT DISTINCT 
                coditemcatalogo,
                LOWER(materialouserviconome) as tipo_lower
            FROM itens_compra
            WHERE coditemcatalogo IS NOT NULL 
              AND coditemcatalogo != ''
              AND materialouserviconome IS NOT NULL
        ),
        codigos_processados AS (
            SELECT DISTINCT 
                codigoitemcatalogo,
                MAX(data_extracao) as ultima_extracao
            FROM precos_catalogo
            GROUP BY codigoitemcatalogo
        )
        SELECT 
            ci.coditemcatalogo,
            CASE 
                WHEN ci.tipo_lower LIKE '%material%' THEN 'MATERIAL'
                WHEN ci.tipo_lower LIKE '%servi%' THEN 'SERVICO'
                ELSE 'MATERIAL'
            END as tipo
        FROM codigos_itens ci
        LEFT JOIN codigos_processados cp ON ci.coditemcatalogo = cp.codigoitemcatalogo
        ORDER BY 
            CASE WHEN cp.codigoitemcatalogo IS NULL THEN 0 ELSE 1 END,
            cp.ultima_extracao ASC NULLS FIRST,
            ci.coditemcatalogo
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        logger.info(f"Total de códigos para processar: {len(results)}")
        return results
        
    except Exception as e:
        logger.error(f"Erro ao buscar códigos pendentes: {e}")
        return []

# =====================================================
# FUNÇÕES DE EXTRAÇÃO DA API
# =====================================================

def fetch_all_pages(codigo: str, tipo: str) -> Optional[pd.DataFrame]:
    """
    Busca todas as páginas de um código na API correspondente
    Retorna DataFrame consolidado ou None
    """
    try:
        endpoint = CONFIG["ENDPOINTS"][tipo]
        url = f"{CONFIG['PNCP_BASE_URL']}/{endpoint}"
        
        all_data = []
        pagina = 1
        
        while True:
            params = {
                'pagina': pagina,
                'codigoItemCatalogo': codigo
            }
            
            # Adicionar tamanhoPagina apenas para Material
            if tipo == "MATERIAL":
                params['tamanhoPagina'] = CONFIG["PAGE_SIZE"]
            
            logger.info(f"Buscando código {codigo} ({tipo}) - Página {pagina}")
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            # Decodificar com UTF-8 para caracteres especiais
            content = response.content.decode('utf-8-sig')
            
            if not content.strip():
                logger.warning(f"Resposta vazia para código {codigo} na página {pagina}")
                break
            
            # Remover última linha de metadados (totalRegistros: ...)
            lines = content.strip().split('\n')
            if lines and 'totalRegistros:' in lines[-1]:
                lines = lines[:-1]
            
            if len(lines) <= 1:  # Apenas header ou vazio
                logger.info(f"Sem mais dados na página {pagina}")
                break
            
            clean_csv = '\n'.join(lines)
            
            # Parse CSV
            df_page = pd.read_csv(StringIO(clean_csv), sep='\t', encoding='utf-8')
            
            if df_page.empty:
                logger.info(f"DataFrame vazio na página {pagina}")
                break
            
            all_data.append(df_page)
            
            # Verificar se há próxima página (inferir do tamanho)
            if len(df_page) < CONFIG["PAGE_SIZE"]:
                logger.info(f"Última página alcançada (registros: {len(df_page)})")
                break
            
            pagina += 1
            time.sleep(1)  # Rate limiting entre páginas
        
        if not all_data:
            logger.warning(f"Nenhum dado encontrado para código {codigo}")
            return None
        
        # Consolidar todas as páginas
        df_final = pd.concat(all_data, ignore_index=True)
        logger.info(f"✓ Total de {len(df_final)} registros para código {codigo}")
        
        return df_final
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro HTTP ao buscar código {codigo}: {e}")
        return None
    except Exception as e:
        logger.error(f"Erro ao processar código {codigo}: {e}")
        import traceback
        logger.error(traceback.format_exc())
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
        elif target_type == "DATE":
            return pd.to_datetime(series, errors='coerce').dt.date
        elif target_type == "TIMESTAMP":
            return pd.to_datetime(series, errors='coerce', utc=True)
        else:
            return series
    except Exception as e:
        logger.warning(f"Erro ao converter coluna: {e}")
        return series

def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nomes de colunas para minúsculas"""
    df.columns = df.columns.str.lower().str.strip()
    return df

def map_and_clean_dataframe(df: pd.DataFrame, schema: Dict[str, str]) -> pd.DataFrame:
    """Mapeia e limpa DataFrame conforme schema"""
    if df.empty:
        return pd.DataFrame(columns=list(schema.keys()) + ['data_extracao'])
    
    # Normalizar nomes de colunas
    df = normalize_column_names(df)
    
    result_df = pd.DataFrame()
    
    # Mapear colunas do schema
    for col, dtype in schema.items():
        if col in df.columns:
            result_df[col] = convert_column_type(df[col], dtype)
        else:
            result_df[col] = None
            logger.debug(f"Coluna {col} não encontrada no CSV")
    
    result_df['data_extracao'] = datetime.utcnow()
    
    return result_df

# =====================================================
# FUNÇÕES DE CARGA
# =====================================================

def load_precos_to_cockroach(df: pd.DataFrame) -> bool:
    """Carrega preços no CockroachDB"""
    if df.empty:
        logger.warning("DataFrame vazio - nada para inserir")
        return False
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        columns = list(PRECOS_SCHEMA.keys()) + ['data_extracao']
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        # Usar iditemcompra como chave única
        insert_query = f"""
            INSERT INTO precos_catalogo ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (idcompraitem)
            DO UPDATE SET data_extracao = EXCLUDED.data_extracao
        """
        
        # Preparar dados
        data_tuples = [tuple(row) for row in df[columns].replace({np.nan: None}).values]
        
        # Executar batch insert
        execute_batch(cursor, insert_query, data_tuples, page_size=1000)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"✓ {len(df)} registros de preços inseridos/atualizados")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao inserir preços: {e}")
        import traceback
        logger.error(traceback.format_exc())
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False

# =====================================================
# PROCESSAMENTO DE CÓDIGO
# =====================================================

def process_single_code(codigo: str, tipo: str) -> bool:
    """Processa um único código de catálogo"""
    try:
        logger.info(f"--- Processando código: {codigo} ({tipo}) ---")
        
        # Buscar dados da API
        df_raw = fetch_all_pages(codigo, tipo)
        
        if df_raw is None or df_raw.empty:
            logger.warning(f"Sem dados para código {codigo}")
            return False
        
        # Transformar dados
        df_clean = map_and_clean_dataframe(df_raw, PRECOS_SCHEMA)
        
        # Carregar no banco
        success = load_precos_to_cockroach(df_clean)
        
        return success
        
    except Exception as e:
        logger.error(f"Erro ao processar código {codigo}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

# =====================================================
# FUNÇÃO PRINCIPAL
# =====================================================

def main():
    """Orquestração principal do pipeline de preços"""
    logger.info("=== Iniciando Pipeline de Preços de Catálogo ===")
    
    try:
        # 1. Buscar códigos pendentes
        pending_codes = get_pending_codes()
        
        if not pending_codes:
            logger.info("Nenhum código pendente para processar")
            return
        
        # 2. Processar em batches
        total = len(pending_codes)
        processed = 0
        failed = 0
        
        for i in range(0, len(pending_codes), CONFIG["BATCH_SIZE"]):
            batch = pending_codes[i:i + CONFIG["BATCH_SIZE"]]
            
            logger.info(f"\n>>> Processando lote {i//CONFIG['BATCH_SIZE'] + 1} ({len(batch)} códigos)")
            
            for codigo, tipo in batch:
                retry_count = 0
                success = False
                
                while retry_count < CONFIG["MAX_RETRIES_PER_ITEM"] and not success:
                    if retry_count > 0:
                        logger.info(f"Tentativa {retry_count + 1} para código {codigo}")
                        time.sleep(CONFIG["RETRY_DELAY_SECONDS"])
                    
                    success = process_single_code(codigo, tipo)
                    retry_count += 1
                
                if success:
                    processed += 1
                    time.sleep(CONFIG["SUCCESS_DELAY_SECONDS"])
                else:
                    failed += 1
                    logger.error(f"Código {codigo} falhou após {CONFIG['MAX_RETRIES_PER_ITEM']} tentativas")
            
            logger.info(f"Progresso: {processed}/{total} processados | {failed} falhas")
        
        # 3. Relatório final
        logger.info("\n=== Pipeline Concluído ===")
        logger.info(f"Total: {total} códigos")
        logger.info(f"Sucesso: {processed}")
        logger.info(f"Falhas: {failed}")
        logger.info(f"Taxa de sucesso: {(processed/total)*100:.2f}%")
        
    except Exception as e:
        logger.error(f"Erro no pipeline principal: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise

if __name__ == "__main__":
    main()
