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
import re

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
    "MAX_CONSECUTIVE_FAILURES": 30,
    "SCRIPT_VERSION": "v1.0.0",
    
    # ===== MODO TESTE =====
    "MODO_TESTE": True,
    "TESTE_CODIGOS": ["439495"],
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
# SCHEMA PRECOS_CATALOGO
# =====================================================

PRECOS_SCHEMA = {
    "idcompra": "STRING",
    "numeroitemcompra": "INTEGER",
    "coditemcatalogo": "STRING",
    "unidadeorgaocodigounidade": "STRING",
    "unidadeorgaonomeunidade": "STRING",
    "unidadeorgaouf": "STRING",
    "descricaodetalhada": "STRING",
    "quantidadehomologada": "FLOAT",
    "unidademedida": "STRING",
    "valorunitariohomologado": "FLOAT",
    "percentualdesconto": "FLOAT",
    "marca": "STRING",
    "nifornecedor": "STRING",
    "nomefornecedor": "STRING",
    "datacompra": "DATE",
}

# =====================================================
# FUNÇÕES AUXILIARES (do código antigo)
# =====================================================

def normalizar_nome_coluna(nome: str) -> str:
    """Converte CamelCase para snake_case"""
    if not isinstance(nome, str):
        return ''
    s = nome.strip()
    # camelCase → snake_case: idCompra → id_compra
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    # Remove caracteres especiais
    s = re.sub(r'[^a-zA-Z0-9_]+', '_', s)
    return s.lower().strip('_')

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
    """Retorna lista de (codigo_catalogo, tipo)"""
    try:
        if CONFIG["MODO_TESTE"]:
            logger.warning("⚠️  MODO TESTE ATIVADO ⚠️")
            logger.warning(f"Processando apenas códigos: {CONFIG['TESTE_CODIGOS']}")
            
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'itens_compra' 
                  AND column_name IN ('coditemcatalogo', 'codigoitemcatalogo')
            """)
            
            result = cursor.fetchone()
            col_itens = result[0] if result else 'coditemcatalogo'
            
            test_codes = []
            for codigo in CONFIG["TESTE_CODIGOS"]:
                cursor.execute(f"""
                    SELECT DISTINCT LOWER(materialouserviconome)
                    FROM itens_compra
                    WHERE TRIM(TRAILING '0' FROM TRIM(TRAILING '.' FROM REGEXP_REPLACE({col_itens}, '\.0+$', ''))) = %s
                    LIMIT 1
                """, (codigo,))
                
                result = cursor.fetchone()
                if result:
                    tipo_lower = result[0]
                    tipo = 'MATERIAL' if 'material' in tipo_lower else 'SERVICO'
                    test_codes.append((codigo, tipo))
                else:
                    test_codes.append((codigo, 'MATERIAL'))
            
            cursor.close()
            conn.close()
            
            logger.info(f"Total de códigos em MODO TESTE: {len(test_codes)}")
            return test_codes
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'itens_compra' 
              AND column_name IN ('coditemcatalogo', 'codigoitemcatalogo')
        """)
        
        result_itens = cursor.fetchone()
        
        if not result_itens:
            logger.error("Nenhuma coluna de código de catálogo encontrada")
            cursor.close()
            conn.close()
            return []
        
        col_itens = result_itens[0]
        
        query = f"""
        WITH codigos_itens AS (
            SELECT DISTINCT 
                TRIM(TRAILING '0' FROM TRIM(TRAILING '.' FROM REGEXP_REPLACE({col_itens}, '\.0+$', ''))) as codigo,
                LOWER(materialouserviconome) as tipo_lower
            FROM itens_compra
            WHERE {col_itens} IS NOT NULL 
              AND {col_itens} != ''
              AND materialouserviconome IS NOT NULL
        ),
        codigos_processados AS (
            SELECT DISTINCT 
                TRIM(TRAILING '0' FROM TRIM(TRAILING '.' FROM REGEXP_REPLACE(coditemcatalogo, '\.0+$', ''))) as codigo,
                MAX(data_extracao) as ultima_extracao
            FROM precos_catalogo
            GROUP BY TRIM(TRAILING '0' FROM TRIM(TRAILING '.' FROM REGEXP_REPLACE(coditemcatalogo, '\.0+$', '')))
        )
        SELECT 
            ci.codigo,
            CASE 
                WHEN ci.tipo_lower LIKE '%material%' THEN 'MATERIAL'
                WHEN ci.tipo_lower LIKE '%servi%' THEN 'SERVICO'
                ELSE 'MATERIAL'
            END as tipo
        FROM codigos_itens ci
        LEFT JOIN codigos_processados cp ON ci.codigo = cp.codigo
        WHERE ci.codigo ~ '^[0-9]+$'
        ORDER BY 
            CASE WHEN cp.codigo IS NULL THEN 0 ELSE 1 END,
            cp.ultima_extracao ASC NULLS FIRST,
            ci.codigo::INT
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        logger.info(f"Total de códigos para processar: {len(results)}")
        return results
        
    except Exception as e:
        logger.error(f"Erro ao buscar códigos pendentes: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []

# =====================================================
# FUNÇÕES DE EXTRAÇÃO DA API
# =====================================================

def fetch_all_pages(codigo: str, tipo: str) -> Optional[pd.DataFrame]:
    """Busca todas as páginas de um código"""
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
            
            if tipo == "MATERIAL":
                params['tamanhoPagina'] = CONFIG["PAGE_SIZE"]
            
            logger.info(f"Buscando código {codigo} ({tipo}) - Página {pagina}")
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            content = response.content.decode('utf-8-sig')
            
            if not content.strip():
                break
            
            lines = content.strip().split('\n')
            if lines and 'totalRegistros:' in lines[-1]:
                lines = lines[:-1]
            
            if len(lines) <= 1:
                break
            
            clean_csv = '\n'.join(lines)
            
            # CORREÇÃO: Usar separador ; (não \t)
            df_page = pd.read_csv(
                StringIO(clean_csv),
                sep=';',  # ← MUDOU de \t para ;
                encoding='utf-8',
                on_bad_lines='warn',
                engine='python',
                dtype=str
            )
            
            if df_page.empty:
                break
            
            all_data.append(df_page)
            
            if len(df_page) < CONFIG["PAGE_SIZE"]:
                break
            
            pagina += 1
            time.sleep(1)
        
        if not all_data:
            logger.warning(f"Nenhum dado encontrado para código {codigo}")
            return None
        
        df_final = pd.concat(all_data, ignore_index=True)
        logger.info(f"✓ Total de {len(df_final)} registros para código {codigo}")
        
        return df_final
        
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
            result = series.astype(str).replace(['nan', 'None', '<NA>', ''], None)
            if result is not None and hasattr(result, 'str'):
                result = result.str.replace(r'\.0+$', '', regex=True)
            return result
        elif target_type == "INTEGER":
            return pd.to_numeric(series, errors='coerce').astype('Int64')
        elif target_type == "FLOAT":
            return pd.to_numeric(series, errors='coerce')
        elif target_type == "DATE":
            return pd.to_datetime(series, errors='coerce').dt.date
        else:
            return series
    except Exception as e:
        logger.warning(f"Erro ao converter coluna: {e}")
        return series

def map_csv_to_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Mapeia colunas do CSV para o schema do banco"""
    if df.empty:
        return pd.DataFrame(columns=list(PRECOS_SCHEMA.keys()) + ['data_extracao', 'versao_script'])
    
    logger.info(f"Registros no CSV original: {len(df)}")
    logger.info(f"Colunas originais: {df.columns.tolist()[:5]}...")
    
    # NORMALIZAÇÃO ROBUSTA (do código antigo)
    df.columns = [normalizar_nome_coluna(col) for col in df.columns]
    
    # Tratar NaN/None (do código antigo)
    df = df.where(pd.notna(df), None)
    
    logger.info(f"Colunas normalizadas: {df.columns.tolist()[:5]}...")
    
    # Mapeamento: CSV normalizado → Banco
    column_mapping = {
        'id_compra': 'idcompra',
        'numero_item_compra': 'numeroitemcompra',
        'codigo_item_catalogo': 'coditemcatalogo',
        'codigo_uasg': 'unidadeorgaocodigounidade',
        'nome_uasg': 'unidadeorgaonomeunidade',
        'estado': 'unidadeorgaouf',
        'descricao_item': 'descricaodetalhada',
        'quantidade': 'quantidadehomologada',
        'sigla_unidade_medida': 'unidademedida',
        'preco_unitario': 'valorunitariohomologado',
        'percentual_maior_desconto': 'percentualdesconto',
        'marca': 'marca',
        'ni_fornecedor': 'nifornecedor',
        'nome_fornecedor': 'nomefornecedor',
        'data_compra': 'datacompra',
    }
    
    # Construir dicionário de dados (MANTÉM LINHAS)
    result_data = {}
    
    for csv_col, schema_col in column_mapping.items():
        if csv_col in df.columns:
            result_data[schema_col] = convert_column_type(
                df[csv_col],
                PRECOS_SCHEMA.get(schema_col, "STRING")
            )
            logger.debug(f"✓ Mapeado: {csv_col} → {schema_col}")
        else:
            result_data[schema_col] = [None] * len(df)  # ← Preencher com None mas manter número de linhas
            logger.warning(f"❌ Coluna '{csv_col}' não encontrada")
    
    # Adicionar colunas faltantes do schema
    for col in PRECOS_SCHEMA.keys():
        if col not in result_data:
            result_data[col] = [None] * len(df)
    
    # Criar DataFrame do dicionário (preserva linhas)
    result_df = pd.DataFrame(result_data)
    
    # Adicionar metadados
    result_df['data_extracao'] = datetime.utcnow()
    result_df['versao_script'] = CONFIG["SCRIPT_VERSION"]
    
    logger.info(f"✓ DataFrame final: {len(result_df)} registros, {len(result_df.columns)} colunas")
    
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
        
        columns = list(PRECOS_SCHEMA.keys()) + ['data_extracao', 'versao_script']
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        insert_query = f"""
            INSERT INTO precos_catalogo ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (idcompra, numeroitemcompra)
            DO UPDATE SET 
                data_extracao = EXCLUDED.data_extracao,
                versao_script = EXCLUDED.versao_script
        """
        
        data_tuples = [tuple(row) for row in df[columns].replace({np.nan: None}).values]
        
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
        
        df_raw = fetch_all_pages(codigo, tipo)
        
        if df_raw is None or df_raw.empty:
            logger.warning(f"Sem dados para código {codigo}")
            return False
        
        df_clean = map_csv_to_schema(df_raw)
        
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
    
    if CONFIG["MODO_TESTE"]:
        logger.info("="*60)
        logger.info("⚠️  EXECUTANDO EM MODO TESTE ⚠️")
        logger.info(f"Códigos: {CONFIG['TESTE_CODIGOS']}")
        logger.info("="*60)
    else:
        logger.info("=== Pipeline de Preços de Catálogo (PRODUÇÃO) ===")
    
    try:
        pending_codes = get_pending_codes()
        
        if not pending_codes:
            logger.info("Nenhum código pendente")
            return
        
        total = len(pending_codes)
        processed = 0
        failed = 0
        consecutive_failures = 0
        
        for i in range(0, len(pending_codes), CONFIG["BATCH_SIZE"]):
            batch = pending_codes[i:i + CONFIG["BATCH_SIZE"]]
            
            logger.info(f"\n>>> Lote {i//CONFIG['BATCH_SIZE'] + 1} ({len(batch)} códigos)")
            logger.info(f"Falhas consecutivas: {consecutive_failures}/{CONFIG['MAX_CONSECUTIVE_FAILURES']}")
            
            for codigo, tipo in batch:
                if consecutive_failures >= CONFIG["MAX_CONSECUTIVE_FAILURES"]:
                    logger.critical(f"🛑 LIMITE ATINGIDO ({CONFIG['MAX_CONSECUTIVE_FAILURES']})")
                    return
                
                retry_count = 0
                success = False
                
                while retry_count < CONFIG["MAX_RETRIES_PER_ITEM"] and not success:
                    if retry_count > 0:
                        time.sleep(CONFIG["RETRY_DELAY_SECONDS"])
                    
                    success = process_single_code(codigo, tipo)
                    retry_count += 1
                
                if success:
                    processed += 1
                    consecutive_failures = 0
                    logger.info(f"✅ Sucesso")
                    time.sleep(CONFIG["SUCCESS_DELAY_SECONDS"])
                else:
                    failed += 1
                    consecutive_failures += 1
                    logger.error(f"❌ Falhou após {CONFIG['MAX_RETRIES_PER_ITEM']} tentativas")
            
            logger.info(f"Progresso: {processed}/{total}")
        
        logger.info("\n=== CONCLUÍDO ===")
        
    except Exception as e:
        logger.error(f"Erro fatal: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise

if __name__ == "__main__":
    main()
