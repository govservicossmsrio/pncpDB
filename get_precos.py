import os
import time
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from io import StringIO
import psycopg2
from psycopg2.extras import execute_batch
from typing import Dict, List, Optional, Tuple
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import Timeout, ConnectionError

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
    "PARALLEL_REQUESTS": 3,
    "STAGGER_DELAY_SECONDS": 1,
    "API_ERROR_RETRY_DELAY": 5,
    "MAX_CONSECUTIVE_API_ERRORS": 6,
    "MAX_ERRORS_PER_CODE": 10,
    "EXECUTION_TIME_LIMIT_HOURS": 1,
    "SCRIPT_VERSION": "v2.0.0",
    
    # ===== MODO TESTE =====
    "MODO_TESTE": False,
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
# VARIÁVEIS GLOBAIS DE CONTROLE
# =====================================================

execution_start_time = None
should_stop = False
db_errors_log = []

# =====================================================
# SCHEMA PRECOS_CATALOGO
# =====================================================

PRECOS_SCHEMA = {
    "idcompraitem": "STRING", 
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
# CLASSES DE ERRO PERSONALIZADAS
# =====================================================

class APIError(Exception):
    """Erro relacionado à API (timeout, bloqueio, rate limit)"""
    pass

class DatabaseError(Exception):
    """Erro relacionado ao banco de dados"""
    pass

class DataValidationError(Exception):
    """Erro de validação de dados (CSV malformado)"""
    pass

# =====================================================
# FUNÇÕES AUXILIARES
# =====================================================

def normalizar_nome_coluna(nome: str) -> str:
    """Converte CamelCase para snake_case"""
    if not isinstance(nome, str):
        return ''
    s = nome.strip()
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    s = re.sub(r'[^a-zA-Z0-9_]+', '_', s)
    return s.lower().strip('_')

def check_execution_time() -> bool:
    """Verifica se o tempo de execução foi excedido"""
    global execution_start_time, should_stop
    
    if should_stop:
        return False
    
    elapsed = datetime.now() - execution_start_time
    limit = timedelta(hours=CONFIG["EXECUTION_TIME_LIMIT_HOURS"])
    
    if elapsed >= limit:
        logger.warning(f"⏰ Tempo de execução atingido: {elapsed} >= {limit}")
        should_stop = True
        return False
    
    return True

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
        raise DatabaseError(f"Falha na conexão: {e}")

# =====================================================
# TABELA DE CONTROLE DE EXECUÇÃO
# =====================================================

def create_control_table():
    """Cria tabela de controle de execução se não existir"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS precos_catalogo_controle (
                codigo_catalogo STRING PRIMARY KEY,
                tipo STRING NOT NULL,
                tentativas_totais INT DEFAULT 0,
                ultima_tentativa TIMESTAMP,
                ultimo_erro TEXT,
                ultimo_sucesso TIMESTAMP,
                status STRING DEFAULT 'PENDENTE'
            )
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("✓ Tabela de controle verificada/criada")
        
    except Exception as e:
        logger.error(f"Erro ao criar tabela de controle: {e}")
        raise DatabaseError(f"Falha ao criar tabela de controle: {e}")

def update_control_record(codigo: str, tipo: str, success: bool, error_msg: Optional[str] = None):
    """Atualiza registro de controle de execução"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if success:
            cursor.execute("""
                INSERT INTO precos_catalogo_controle 
                    (codigo_catalogo, tipo, tentativas_totais, ultima_tentativa, ultimo_sucesso, status)
                VALUES (%s, %s, 1, NOW(), NOW(), 'SUCESSO')
                ON CONFLICT (codigo_catalogo)
                DO UPDATE SET
                    tentativas_totais = precos_catalogo_controle.tentativas_totais + 1,
                    ultima_tentativa = NOW(),
                    ultimo_sucesso = NOW(),
                    status = 'SUCESSO'
            """, (codigo, tipo))
        else:
            cursor.execute("""
                INSERT INTO precos_catalogo_controle 
                    (codigo_catalogo, tipo, tentativas_totais, ultima_tentativa, ultimo_erro, status)
                VALUES (%s, %s, 1, NOW(), %s, 'ERRO')
                ON CONFLICT (codigo_catalogo)
                DO UPDATE SET
                    tentativas_totais = precos_catalogo_controle.tentativas_totais + 1,
                    ultima_tentativa = NOW(),
                    ultimo_erro = %s,
                    status = 'ERRO'
            """, (codigo, tipo, error_msg, error_msg))
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Erro ao atualizar controle para código {codigo}: {e}")

# =====================================================
# FUNÇÕES DE BUSCA DE CÓDIGOS
# =====================================================

def get_pending_codes() -> List[Tuple[str, str]]:
    """Retorna lista de (codigo_catalogo, tipo) priorizando nunca processados"""
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
        )
        SELECT 
            ci.codigo,
            CASE 
                WHEN ci.tipo_lower LIKE '%material%' THEN 'MATERIAL'
                WHEN ci.tipo_lower LIKE '%servi%' THEN 'SERVICO'
                ELSE 'MATERIAL'
            END as tipo
        FROM codigos_itens ci
        LEFT JOIN precos_catalogo_controle ctrl ON ci.codigo = ctrl.codigo_catalogo
        WHERE ci.codigo ~ '^[0-9]+$'
          AND (ctrl.tentativas_totais IS NULL OR ctrl.tentativas_totais < {CONFIG['MAX_ERRORS_PER_CODE']})
        ORDER BY 
            CASE 
                WHEN ctrl.codigo_catalogo IS NULL THEN 0
                WHEN ctrl.status = 'ERRO' THEN 1
                WHEN ctrl.status = 'SUCESSO' THEN 2
                ELSE 3
            END,
            ctrl.ultima_tentativa ASC NULLS FIRST,
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
        raise DatabaseError(f"Falha ao buscar códigos: {e}")

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
            
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
            except (Timeout, ConnectionError) as e:
                raise APIError(f"Timeout/ConnectionError na API: {e}")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code in [429, 503]:
                    raise APIError(f"API bloqueada/indisponível (HTTP {e.response.status_code})")
                raise APIError(f"Erro HTTP na API: {e}")
            
            content = response.content.decode('utf-8-sig')
            
            if not content.strip():
                break
            
            lines = content.strip().split('\n')
            if lines and 'totalRegistros:' in lines[-1]:
                lines = lines[:-1]
            
            if len(lines) <= 1:
                break
            
            clean_csv = '\n'.join(lines)
            
            df_page = pd.read_csv(
                StringIO(clean_csv),
                sep=';',
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
        
    except APIError:
        raise
    except Exception as e:
        logger.error(f"Erro inesperado ao processar código {codigo}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise DataValidationError(f"Erro ao processar dados: {e}")

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
        elif target_type == "TIMESTAMP":
            return pd.to_datetime(series, errors='coerce')
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
    logger.info(f"Total de colunas no CSV: {len(df.columns)}")
    logger.info(f"Colunas originais (primeiras 10): {df.columns.tolist()[:10]}")
    
    # Normalização
    df.columns = [normalizar_nome_coluna(col) for col in df.columns]
    df = df.where(pd.notna(df), None)
    
    logger.info(f"Colunas normalizadas (primeiras 10): {df.columns.tolist()[:10]}")
    
    # Validação de colunas obrigatórias
    if 'id_compra' not in df.columns or 'numero_item_compra' not in df.columns:
        logger.error("❌ Colunas obrigatórias 'id_compra' e/ou 'numero_item_compra' não encontradas")
        logger.error(f"Colunas disponíveis: {df.columns.tolist()}")
        raise DataValidationError("Colunas obrigatórias ausentes no CSV")
    
    # =====================================================
    # CONSTRUÇÃO DO idcompraitem (PRIMARY KEY COMPOSTA)
    # =====================================================
    df['idcompraitem_construido'] = (
        df['id_compra'].astype(str) + 
        df['numero_item_compra'].astype(str).str.zfill(5)
    )
    
    logger.info(f"✓ idcompraitem construído (exemplo): {df['idcompraitem_construido'].iloc[0]}")
    
    # =====================================================
    # TRATAMENTO DE DUPLICATAS
    # (usa data_hora_atualizacao_item mas NÃO a salva no banco)
    # =====================================================
    registros_antes = len(df)
    
    if 'data_hora_atualizacao_item' in df.columns:
        df['data_hora_atualizacao_item'] = pd.to_datetime(
            df['data_hora_atualizacao_item'], 
            errors='coerce'
        )
        
        # Ordena do mais recente para o mais antigo
        df = df.sort_values('data_hora_atualizacao_item', ascending=False)
        
        # Remove duplicatas mantendo o primeiro (mais recente)
        df = df.drop_duplicates(subset=['idcompraitem_construido'], keep='first')
        
        registros_removidos = registros_antes - len(df)
        if registros_removidos > 0:
            logger.warning(f"⚠️  {registros_removidos} duplicatas removidas (mantido registro mais recente)")
    else:
        logger.warning("⚠️  Coluna 'data_hora_atualizacao_item' não encontrada - duplicatas não tratadas")
        df = df.drop_duplicates(subset=['idcompraitem_construido'], keep='first')
    
    logger.info(f"Registros após deduplicação: {len(df)}")
    
    # =====================================================
    # MAPEAMENTO: CSV normalizado → Banco
    # (data_hora_atualizacao_item NÃO é mapeada)
    # =====================================================
    column_mapping = {
        'idcompraitem_construido': 'idcompraitem',
        'id_compra': 'idcompra',
        'numero_item_compra': 'numeroitemcompra',
        'codigo_item_catalogo': 'coditemcatalogo',
        'descricao_item': 'descricaodetalhada',
        'quantidade': 'quantidadehomologada',
        'sigla_unidade_medida': 'unidademedida',
        'preco_unitario': 'valorunitariohomologado',
        'percentual_maior_desconto': 'percentualdesconto',
        'marca': 'marca',
        'ni_fornecedor': 'nifornecedor',
        'nome_fornecedor': 'nomefornecedor',
        'codigo_uasg': 'unidadeorgaocodigounidade',
        'nome_uasg': 'unidadeorgaonomeunidade',
        'estado': 'unidadeorgaouf',
        'data_compra': 'datacompra',
    }
    
    result_data = {}
    
    for csv_col, schema_col in column_mapping.items():
        if csv_col in df.columns:
            result_data[schema_col] = convert_column_type(
                df[csv_col],
                PRECOS_SCHEMA.get(schema_col, "STRING")
            )
            logger.debug(f"✓ Mapeado: {csv_col} → {schema_col}")
        else:
            logger.warning(f"❌ Coluna '{csv_col}' não encontrada no CSV")
            result_data[schema_col] = [None] * len(df)
    
    # Adicionar colunas faltantes do schema
    for col in PRECOS_SCHEMA.keys():
        if col not in result_data:
            result_data[col] = [None] * len(df)
            logger.debug(f"⚠️  Coluna '{col}' preenchida com NULL (não encontrada no CSV)")
    
    result_df = pd.DataFrame(result_data)
    
    result_df['data_extracao'] = datetime.utcnow()
    result_df['versao_script'] = CONFIG["SCRIPT_VERSION"]
    
    logger.info(f"✓ DataFrame final: {len(result_df)} registros, {len(result_df.columns)} colunas")
    
    # Verificação de colunas NULL críticas
    colunas_criticas = ['quantidadehomologada', 'unidademedida', 'valorunitariohomologado']
    for col in colunas_criticas:
        null_count = result_df[col].isna().sum()
        if null_count > 0:
            logger.warning(f"⚠️  Coluna '{col}' tem {null_count} valores NULL de {len(result_df)} registros")
    
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
            ON CONFLICT (idcompraitem)
            DO UPDATE SET 
                idcompra = EXCLUDED.idcompra,
                numeroitemcompra = EXCLUDED.numeroitemcompra,
                coditemcatalogo = EXCLUDED.coditemcatalogo,
                unidadeorgaocodigounidade = EXCLUDED.unidadeorgaocodigounidade,
                unidadeorgaonomeunidade = EXCLUDED.unidadeorgaonomeunidade,
                unidadeorgaouf = EXCLUDED.unidadeorgaouf,
                descricaodetalhada = EXCLUDED.descricaodetalhada,
                quantidadehomologada = EXCLUDED.quantidadehomologada,
                unidademedida = EXCLUDED.unidademedida,
                valorunitariohomologado = EXCLUDED.valorunitariohomologado,
                percentualdesconto = EXCLUDED.percentualdesconto,
                marca = EXCLUDED.marca,
                nifornecedor = EXCLUDED.nifornecedor,
                nomefornecedor = EXCLUDED.nomefornecedor,
                datacompra = EXCLUDED.datacompra,
                data_extracao = EXCLUDED.data_extracao,
                versao_script = EXCLUDED.versao_script
        """
        
        data_tuples = [tuple(row) for row in df[columns].replace({np.nan: None}).values]
        
        logger.info(f"Inserindo {len(data_tuples)} registros...")
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
        raise DatabaseError(f"Falha ao inserir no banco: {e}")

# =====================================================
# PROCESSAMENTO DE CÓDIGO
# =====================================================

def process_single_code(codigo: str, tipo: str) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Processa um único código de catálogo
    
    Returns:
        tipo_erro pode ser: 'API', 'DATABASE', 'VALIDATION', None
    """
    try:
        logger.info(f"--- Processando código: {codigo} ({tipo}) ---")
        
        # Busca na API
        try:
            df_raw = fetch_all_pages(codigo, tipo)
        except APIError as e:
            return (False, 'API', str(e))
        
        if df_raw is None or df_raw.empty:
            logger.warning(f"Sem dados para código {codigo}")
            return (False, 'VALIDATION', 'Nenhum dado retornado pela API')
        
        # Transformação
        try:
            df_clean = map_csv_to_schema(df_raw)
        except DataValidationError as e:
            return (False, 'VALIDATION', str(e))
        
        # Carga no banco
        try:
            load_precos_to_cockroach(df_clean)
            return (True, None, None)
        except DatabaseError as e:
            return (False, 'DATABASE', str(e))
        
    except Exception as e:
        logger.error(f"Erro inesperado ao processar código {codigo}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return (False, 'UNKNOWN', str(e))

def process_code_with_retry(codigo: str, tipo: str) -> bool:
    """
    Processa código com retry para erros de API
    
    Returns:
        True se sucesso, False caso contrário
    """
    success, error_type, error_msg = process_single_code(codigo, tipo)
    
    if success:
        update_control_record(codigo, tipo, True)
        return True
    
    # Se erro de API, tenta novamente após delay
    if error_type == 'API':
        logger.warning(f"⚠️  Erro de API para {codigo}, aguardando {CONFIG['API_ERROR_RETRY_DELAY']}s...")
        time.sleep(CONFIG['API_ERROR_RETRY_DELAY'])
        
        success, error_type, error_msg = process_single_code(codigo, tipo)
        
        if success:
            update_control_record(codigo, tipo, True)
            return True
    
    # Se erro de banco, apenas loga e pula
    if error_type == 'DATABASE':
        global db_errors_log
        db_errors_log.append({
            'codigo': codigo,
            'tipo': tipo,
            'erro': error_msg,
            'timestamp': datetime.now()
        })
        logger.error(f"❌ Erro de banco para {codigo} - pulando")
    
    # Atualiza controle com falha
    update_control_record(codigo, tipo, False, f"[{error_type}] {error_msg}")
    
    return False

# =====================================================
# PROCESSAMENTO PARALELO
# =====================================================

def process_batch_parallel(batch: List[Tuple[str, str]]) -> Tuple[int, int, int]:
    """
    Processa lote de códigos em paralelo com stagger
    
    Returns:
        (sucessos, falhas_api, falhas_outras)
    """
    sucessos = 0
    falhas_api = 0
    falhas_outras = 0
    
    with ThreadPoolExecutor(max_workers=CONFIG["PARALLEL_REQUESTS"]) as executor:
        futures = {}
        
        for i, (codigo, tipo) in enumerate(batch):
            # Stagger: aguarda 1 segundo entre cada submissão
            if i > 0:
                time.sleep(CONFIG["STAGGER_DELAY_SECONDS"])
            
            future = executor.submit(process_code_with_retry, codigo, tipo)
            futures[future] = (codigo, tipo)
            logger.info(f"🚀 Iniciada busca paralela: {codigo}")
        
        # Aguarda conclusão de todas
        for future in as_completed(futures):
            codigo, tipo = futures[future]
            
            try:
                success = future.result()
                
                if success:
                    sucessos += 1
                    logger.info(f"✅ Concluído: {codigo}")
                else:
                    # Verifica tipo de erro no log de controle
                    try:
                        conn = get_db_connection()
                        cursor = conn.cursor()
                        cursor.execute(
                            "SELECT ultimo_erro FROM precos_catalogo_controle WHERE codigo_catalogo = %s",
                            (codigo,)
                        )
                        result = cursor.fetchone()
                        cursor.close()
                        conn.close()
                        
                        if result and result[0] and '[API]' in result[0]:
                            falhas_api += 1
                        else:
                            falhas_outras += 1
                    except:
                        falhas_outras += 1
                    
                    logger.error(f"❌ Falhou: {codigo}")
                    
            except Exception as e:
                logger.error(f"❌ Exceção não capturada para {codigo}: {e}")
                falhas_outras += 1
    
    return (sucessos, falhas_api, falhas_outras)

# =====================================================
# FUNÇÃO PRINCIPAL
# =====================================================

def main():
    """Orquestração principal do pipeline de preços"""
    global execution_start_time, should_stop, db_errors_log
    
    execution_start_time = datetime.now()
    should_stop = False
    db_errors_log = []
    
    logger.info("="*70)
    if CONFIG["MODO_TESTE"]:
        logger.info("⚠️  EXECUTANDO EM MODO TESTE ⚠️")
        logger.info(f"Códigos: {CONFIG['TESTE_CODIGOS']}")
    else:
        logger.info("=== Pipeline de Preços de Catálogo (PRODUÇÃO) ===")
    logger.info(f"Tempo limite: {CONFIG['EXECUTION_TIME_LIMIT_HOURS']} hora(s)")
    logger.info(f"Processamento paralelo: {CONFIG['PARALLEL_REQUESTS']} requisições simultâneas")
    logger.info("="*70)
    
    try:
        # Cria tabela de controle
        create_control_table()
        
        # Busca códigos pendentes
        pending_codes = get_pending_codes()
        
        if not pending_codes:
            logger.info("Nenhum código pendente")
            return
        
        total = len(pending_codes)
        processed = 0
        total_success = 0
        total_failed = 0
        consecutive_api_errors = 0
        
        # Processa em lotes de 3
        for i in range(0, len(pending_codes), CONFIG["PARALLEL_REQUESTS"]):
            # Verifica tempo de execução
            if not check_execution_time():
                logger.warning("⏰ Encerrando execução por limite de tempo")
                break
            
            batch = pending_codes[i:i + CONFIG["PARALLEL_REQUESTS"]]
            batch_num = (i // CONFIG["PARALLEL_REQUESTS"]) + 1
            
            logger.info(f"\n{'='*70}")
            logger.info(f">>> LOTE {batch_num} - {len(batch)} códigos")
            logger.info(f"Erros API consecutivos: {consecutive_api_errors}/{CONFIG['MAX_CONSECUTIVE_API_ERRORS']}")
            logger.info(f"{'='*70}")
            
            # Processa lote em paralelo
            sucessos, falhas_api, falhas_outras = process_batch_parallel(batch)
            
            # Atualiza contadores
            processed += len(batch)
            total_success += sucessos
            total_failed += (falhas_api + falhas_outras)
            
            # Gerencia contador de erros consecutivos de API
            if falhas_api > 0:
                consecutive_api_errors += falhas_api
            else:
                consecutive_api_errors = 0  # Reseta se teve algum sucesso
            
            # Verifica limite de erros consecutivos de API
            if consecutive_api_errors >= CONFIG["MAX_CONSECUTIVE_API_ERRORS"]:
                logger.critical(f"🛑 LIMITE DE ERROS DE API ATINGIDO ({CONFIG['MAX_CONSECUTIVE_API_ERRORS']})")
                logger.critical("Possível problema sistêmico com a API - encerrando execução")
                break
            
            logger.info(f"Lote {batch_num} concluído: {sucessos} sucessos, {falhas_api} falhas API, {falhas_outras} outras falhas")
        
        # Relatório final
        logger.info("\n" + "="*70)
        logger.info("=== EXECUÇÃO CONCLUÍDA ===")
        logger.info(f"Total processado: {processed}/{total}")
        logger.info(f"Sucessos: {total_success}")
        logger.info(f"Falhas: {total_failed}")
        logger.info(f"Tempo de execução: {datetime.now() - execution_start_time}")
        
        # Log de erros de banco
        if db_errors_log:
            logger.warning(f"\n⚠️  ERROS DE BANCO DE DADOS ({len(db_errors_log)}):")
            for err in db_errors_log:
                logger.warning(f"  - {err['codigo']} ({err['tipo']}): {err['erro'][:100]}")
        
        logger.info("="*70)
        
    except DatabaseError as e:
        logger.critical(f"❌ ERRO CRÍTICO DE BANCO: {e}")
        logger.critical("Impossível continuar -
