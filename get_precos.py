#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

# ===== IMPORTS GOOGLE SHEETS =====
from google.oauth2 import service_account
import gspread

# =====================================================
# CONFIGURAÇÃO
# =====================================================

CONFIG = {
    "COCKROACH_CONNECTION_STRING": os.getenv(
        "COCKROACH_CONNECTION_STRING",
        "postgresql://sgc_admin:<password>@scary-quetzal-18026.j77.aws-us-east-1.cockroachlabs.cloud:26257/defaultdb?sslmode=require"
    ),
    "SPREADSHEET_ID": os.getenv("SPREADSHEET_ID", "18P9l9_g-QE-DWsfRCokY18M5RLZe7mV-CWY1bfw6hlA"),
    "SHEET_NAME_CATALOGO": "idlista_catalogo",
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
    "SCRIPT_VERSION": "v2.1.1",
    
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
    "numeroitemcompra": "STRING",  
    "coditemcatalogo": "STRING",
    "unidadeorgaocodigounidade": "STRING",
    "unidadeorgaonomeunidade": "STRING",
    "unidadeorgaouf": "STRING",
    "descricaodetalhada": "STRING",
    "quantidadehomologada": "STRING",
    "unidademedida": "STRING",
    "valorunitariohomologado": "STRING",
    "percentualdesconto": "STRING",
    "marca": "STRING",
    "nifornecedor": "STRING",
    "nomefornecedor": "STRING",
    "datacompra": "STRING",
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
# GOOGLE SHEETS - FUNÇÕES
# =====================================================

def get_sheets_client():
    """Autentica e retorna cliente do Google Sheets"""
    logger.debug("🔐 Iniciando autenticação Google Sheets...")
    try:
        creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        logger.debug(f"📄 Arquivo de credenciais: {creds_path}")
        
        if not creds_path or not os.path.exists(creds_path):
            raise ValueError(f"Arquivo de credenciais não encontrado: {creds_path}")
        
        logger.debug("🔑 Carregando credenciais do arquivo...")
        credentials = service_account.Credentials.from_service_account_file(
            creds_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        
        logger.debug("🔗 Autorizando cliente gspread...")
        client = gspread.authorize(credentials)
        logger.debug("✅ Autenticação Google Sheets concluída")
        return client
        
    except Exception as e:
        logger.error(f"❌ Erro ao inicializar Google Sheets: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise

def initialize_sheets_tab():
    """Cria/valida aba idlista_catalogo com cabeçalhos"""
    logger.info("📊 Inicializando aba idlista_catalogo...")
    try:
        gc = get_sheets_client()
        spreadsheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"])
        
        # Tenta obter a aba, se não existir, cria
        try:
            sheet = spreadsheet.worksheet(CONFIG["SHEET_NAME_CATALOGO"])
            logger.info(f"✅ Aba '{CONFIG['SHEET_NAME_CATALOGO']}' já existe")
        except gspread.WorksheetNotFound:
            logger.info(f"➕ Criando aba '{CONFIG['SHEET_NAME_CATALOGO']}'...")
            sheet = spreadsheet.add_worksheet(
                title=CONFIG["SHEET_NAME_CATALOGO"],
                rows=1000,
                cols=4
            )
        
        # Verifica/adiciona cabeçalhos
        values = sheet.get_all_values()
        if not values or values[0] != ["cod_br", "idcompra", "status", "ultima_busca"]:
            logger.info("📝 Configurando cabeçalhos da planilha...")
            # CORREÇÃO: Ordem correta dos argumentos (values primeiro)
            sheet.update(values=[["cod_br", "idcompra", "status", "ultima_busca"]], range_name='A1:D1')
            logger.info("✅ Cabeçalhos configurados")
        
        return sheet
        
    except Exception as e:
        logger.error(f"❌ Erro ao inicializar aba Sheets: {e}")
        # Não levanta exceção - processamento pode continuar sem Sheets
        return None

def write_catalogo_status(codigo: str, idcompra_list: Optional[List[str]], status: str):
    """
    Escreve/atualiza status na aba idlista_catalogo
    
    Args:
        codigo: Código do catálogo (cod_br)
        idcompra_list: Lista de idcompra encontrados (None se erro antes de obter dados)
        status: Mensagem de status
    """
    logger.debug(f"📊 Preparando escrita no Sheets para código {codigo}...")
    
    try:
        # Formata coluna idcompra
        if idcompra_list and len(idcompra_list) > 0:
            sample = idcompra_list[:3]
            idcompra_display = "; ".join(sample)
            if len(idcompra_list) > 3:
                idcompra_display += f" (+{len(idcompra_list) - 3})"
            logger.debug(f"📋 IDs formatados: {idcompra_display}")
        else:
            idcompra_display = "N/A"
            logger.debug("📋 Nenhum ID encontrado, usando 'N/A'")
        
        # Timestamp UTC
        dt = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        logger.debug(f"🕒 Timestamp: {dt}")
        
        # Abre planilha
        logger.debug("🔗 Conectando ao Google Sheets...")
        gc = get_sheets_client()
        sheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME_CATALOGO"])
        
        # Busca linha existente
        logger.debug(f"🔍 Procurando código {codigo} na planilha...")
        values = sheet.get_all_values()
        
        row_index = None
        for i in range(1, len(values)):  # Pula cabeçalho
            if values[i][0].strip() == codigo:
                row_index = i + 1  # +1 porque sheet é 1-indexed
                logger.debug(f"✓ Código encontrado na linha {row_index}")
                break
        
        # Atualiza ou insere
        if row_index:
            logger.debug(f"🔄 Atualizando linha existente {row_index}...")
            sheet.update_cell(row_index, 2, idcompra_display)  # Coluna B
            sheet.update_cell(row_index, 3, status)            # Coluna C
            sheet.update_cell(row_index, 4, dt)                # Coluna D
            logger.info(f"✅ Status atualizado no Sheets para {codigo}")
        else:
            logger.debug(f"➕ Inserindo nova linha para código {codigo}...")
            sheet.append_row([codigo, idcompra_display, status, dt])
            logger.info(f"✅ Nova linha criada no Sheets para {codigo}")
        
    except Exception as e:
        logger.error(f"❌ Erro ao escrever no Sheets para {codigo}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        # Não levanta exceção - processamento continua

def populate_initial_codes():
    """Popula aba com todos os códigos que serão processados (apenas cod_br)"""
    logger.info("📝 Populando códigos iniciais na planilha...")
    try:
        pending_codes = get_pending_codes()
        
        if not pending_codes:
            logger.info("ℹ️ Nenhum código para popular")
            return
        
        gc = get_sheets_client()
        sheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME_CATALOGO"])
        
        # Obter códigos já existentes
        values = sheet.get_all_values()
        existing_codes = {row[0].strip() for row in values[1:] if row}  # Pula cabeçalho
        
        # Filtrar novos códigos
        new_codes = [(codigo, tipo) for codigo, tipo in pending_codes if codigo not in existing_codes]
        
        if new_codes:
            logger.info(f"➕ Adicionando {len(new_codes)} novos códigos à planilha...")
            rows_to_add = [[codigo, "", "pendente", ""] for codigo, _ in new_codes]
            
            # Adiciona em lotes de 100
            for i in range(0, len(rows_to_add), 100):
                batch = rows_to_add[i:i+100]
                sheet.append_rows(batch)
                logger.debug(f"✓ Lote {i//100 + 1} adicionado ({len(batch)} códigos)")
            
            logger.info(f"✅ {len(new_codes)} códigos adicionados à planilha")
        else:
            logger.info("✅ Todos os códigos já estão na planilha")
            
    except Exception as e:
        logger.error(f"❌ Erro ao popular códigos iniciais: {e}")
        # Não levanta exceção - processamento continua

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

def convert_brazilian_number_to_decimal(value) -> Optional[str]:
    """
    Converte número brasileiro para formato decimal aceito pelo banco
    
    Exemplos:
        "1,00" → "1.00"
        "4.668,00" → "4668.00"
        "19.760,00" → "19760.00"
        "" → None
        None → None
    """
    if pd.isna(value) or value is None or value == '' or str(value).strip() == '':
        return None
    
    value_str = str(value).strip()
    
    # Trata strings que representam valores nulos
    if value_str.lower() in ['null', 'none', 'nan', 'nat', '<na>']:
        return None
    
    # Remove pontos de milhar e troca vírgula por ponto
    # Formato brasileiro: 1.234.567,89
    # Formato americano: 1234567.89
    value_str = value_str.replace('.', '')  # Remove pontos de milhar
    value_str = value_str.replace(',', '.')  # Troca vírgula por ponto
    
    return value_str

def convert_to_string_safe(value) -> Optional[str]:
    """
    Converte valor para string de forma segura, retornando None para vazios
    
    CORREÇÃO: Trata strings "null", "None", "nan" como None
    """
    if pd.isna(value) or value is None or value == '':
        return None
    
    value_str = str(value).strip()
    
    # Trata strings que representam valores nulos
    if value_str.lower() in ['null', 'none', 'nan', 'nat', '<na>']:
        return None
    
    if value_str == '':
        return None
    
    return value_str

def convert_to_integer_safe(value) -> Optional[int]:
    """
    Converte valor para integer de forma segura, retornando None para inválidos
    
    NOVO: Função específica para campos INTEGER
    """
    if pd.isna(value) or value is None or value == '':
        return None
    
    value_str = str(value).strip()
    
    # Trata strings que representam valores nulos
    if value_str.lower() in ['null', 'none', 'nan', 'nat', '<na>', '']:
        return None
    
    # Remove decimais se for número float (ex: "123.0" -> "123")
    if '.' in value_str:
        try:
            float_val = float(value_str)
            if float_val.is_integer():
                value_str = str(int(float_val))
            else:
                # Se tem decimal não-zero, tenta arredondar
                value_str = str(round(float_val))
        except:
            return None
    
    try:
        return int(value_str)
    except (ValueError, TypeError):
        logger.warning(f"⚠️ Não foi possível converter '{value_str}' para INTEGER - usando None")
        return None

def convert_to_date_safe(value) -> Optional[str]:
    """
    Converte valor para data de forma segura, retornando None para inválidos
    
    NOVO: Função específica para campos DATE
    """
    if pd.isna(value) or value is None or value == '':
        return None
    
    value_str = str(value).strip()
    
    # Trata strings que representam valores nulos
    if value_str.lower() in ['null', 'none', 'nan', 'nat', '<na>', '']:
        return None
    
    # Tenta converter para data
    try:
        date_obj = pd.to_datetime(value_str, errors='coerce')
        if pd.isna(date_obj):
            return None
        return date_obj.strftime('%Y-%m-%d')
    except:
        return None

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
        
        logger.debug("🔍 Identificando coluna de código de catálogo...")
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
        logger.debug(f"✓ Coluna identificada: {col_itens}")
        
        logger.debug("🔍 Buscando códigos pendentes no banco...")
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
        logger.debug(f"🌐 Iniciando busca na API para código {codigo} ({tipo})")
        
        endpoint = CONFIG["ENDPOINTS"][tipo]
        url = f"{CONFIG['PNCP_BASE_URL']}/{endpoint}"
        logger.debug(f"📍 URL: {url}")
        
        all_data = []
        pagina = 1
        
        while True:
            params = {
                'pagina': pagina,
                'codigoItemCatalogo': codigo
            }
            
            if tipo == "MATERIAL":
                params['tamanhoPagina'] = CONFIG["PAGE_SIZE"]
            
            logger.info(f"🔍 Buscando código {codigo} ({tipo}) - Página {pagina}")
            logger.debug(f"📋 Parâmetros: {params}")
            
            try:
                logger.debug("⏳ Fazendo requisição HTTP...")
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                logger.debug(f"✓ Resposta recebida - Status: {response.status_code}")
            except (Timeout, ConnectionError) as e:
                logger.error(f"❌ Timeout/ConnectionError na API: {e}")
                raise APIError(f"erro ao puxar os dados da API - timeout após 30s")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code in [429, 503]:
                    logger.error(f"❌ API bloqueada/indisponível (HTTP {e.response.status_code})")
                    raise APIError(f"erro ao puxar os dados da API - rate limit/bloqueio ({e.response.status_code})")
                logger.error(f"❌ Erro HTTP na API: {e}")
                raise APIError(f"erro ao puxar os dados da API - HTTP {e.response.status_code}")
            
            logger.debug("📝 Decodificando conteúdo CSV...")
            content = response.content.decode('utf-8-sig')
            
            if not content.strip():
                logger.debug("⚠️ Conteúdo vazio - fim da paginação")
                break
            
            lines = content.strip().split('\n')
            logger.debug(f"📊 {len(lines)} linhas recebidas")
            
            if lines and 'totalRegistros:' in lines[-1]:
                logger.debug("🔧 Removendo linha de metadados do final")
                lines = lines[:-1]
            
            if len(lines) <= 1:
                logger.debug("⚠️ Apenas cabeçalho - sem dados")
                break
            
            clean_csv = '\n'.join(lines)
            
            logger.debug("🔄 Convertendo CSV para DataFrame...")
            df_page = pd.read_csv(
                StringIO(clean_csv),
                sep=';',
                encoding='utf-8',
                on_bad_lines='warn',
                engine='python',
                dtype=str,
                keep_default_na=False
            )
            
            if df_page.empty:
                logger.debug("⚠️ DataFrame vazio após parse")
                break
            
            logger.debug(f"✓ {len(df_page)} registros parseados nesta página")
            all_data.append(df_page)
            
            if len(df_page) < CONFIG["PAGE_SIZE"]:
                logger.debug(f"✓ Última página (menos de {CONFIG['PAGE_SIZE']} registros)")
                break
            
            pagina += 1
            logger.debug("⏳ Aguardando 1s antes da próxima página...")
            time.sleep(1)
        
        if not all_data:
            logger.warning(f"⚠️ Nenhum dado encontrado para código {codigo}")
            return None
        
        logger.debug(f"🔗 Concatenando {len(all_data)} páginas...")
        df_final = pd.concat(all_data, ignore_index=True)
        logger.info(f"✅ Total de {len(df_final)} registros obtidos para código {codigo}")
        
        return df_final
        
    except APIError:
        raise
    except Exception as e:
        logger.error(f"❌ Erro inesperado ao processar código {codigo}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise DataValidationError(f"erro ao processar dados CSV - {str(e)[:100]}")

# =====================================================
# FUNÇÕES DE TRANSFORMAÇÃO
# =====================================================

def map_csv_to_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Mapeia colunas do CSV para o schema do banco"""
    if df.empty:
        return pd.DataFrame(columns=list(PRECOS_SCHEMA.keys()) + ['data_extracao', 'versao_script'])
    
    logger.debug(f"🔄 Iniciando transformação dos dados...")
    logger.info(f"📊 Registros no CSV original: {len(df)}")
    logger.debug(f"📋 Total de colunas no CSV: {len(df.columns)}")
    
    # Normalização
    logger.debug("🔧 Normalizando nomes das colunas...")
    df.columns = [normalizar_nome_coluna(col) for col in df.columns]
    logger.debug(f"✓ Colunas normalizadas: {df.columns.tolist()[:5]}...")
    
    # Validação de colunas obrigatórias
    logger.debug("✅ Validando colunas obrigatórias...")
    if 'id_compra' not in df.columns or 'numero_item_compra' not in df.columns:
        logger.error("❌ Colunas obrigatórias 'id_compra' e/ou 'numero_item_compra' não encontradas")
        logger.error(f"Colunas disponíveis: {df.columns.tolist()}")
        raise DataValidationError("erro ao validar dados CSV - colunas obrigatórias ausentes")
    
    # Construção do idcompraitem
    logger.debug("🔨 Construindo coluna idcompraitem...")
    df['idcompraitem_construido'] = (
        df['id_compra'].astype(str).str.strip() + 
        df['numero_item_compra'].astype(str).str.strip().str.replace('.0', '', regex=False).str.zfill(5)
    )
    logger.debug(f"✓ idcompraitem construído (exemplo): {df['idcompraitem_construido'].iloc[0]}")
    
    # Tratamento de duplicatas
    registros_antes = len(df)
    logger.debug(f"🔍 Verificando duplicatas (total antes: {registros_antes})...")
    
    if 'data_hora_atualizacao_item' in df.columns:
        logger.debug("📅 Ordenando por data de atualização...")
        df['data_hora_atualizacao_item'] = pd.to_datetime(
            df['data_hora_atualizacao_item'], 
            errors='coerce'
        )
        
        df = df.sort_values('data_hora_atualizacao_item', ascending=False)
        df = df.drop_duplicates(subset=['idcompraitem_construido'], keep='first')
        
        registros_removidos = registros_antes - len(df)
        if registros_removidos > 0:
            logger.warning(f"⚠️  {registros_removidos} duplicatas removidas")
    else:
        df = df.drop_duplicates(subset=['idcompraitem_construido'], keep='first')
    
    logger.info(f"📊 Registros após deduplicação: {len(df)}")
    
    # =====================================================
    # MAPEAMENTO COM CONVERSÃO CORRIGIDA
    # =====================================================
    logger.debug("🔄 Mapeando colunas para o schema do banco...")
    
    column_mapping = {
        'idcompraitem_construido': ('idcompraitem', 'string'),
        'id_compra': ('idcompra', 'string'),
        'numero_item_compra': ('numeroitemcompra', 'integer'),  # ← CORRIGIDO
        'codigo_item_catalogo': ('coditemcatalogo', 'string'),
        'descricao_item': ('descricaodetalhada', 'string'),
        'quantidade': ('quantidadehomologada', 'decimal'),
        'sigla_unidade_medida': ('unidademedida', 'string'),
        'preco_unitario': ('valorunitariohomologado', 'decimal'),
        'percentual_maior_desconto': ('percentualdesconto', 'decimal'),
        'marca': ('marca', 'string'),
        'ni_fornecedor': ('nifornecedor', 'string'),
        'nome_fornecedor': ('nomefornecedor', 'string'),
        'codigo_uasg': ('unidadeorgaocodigounidade', 'string'),
        'nome_uasg': ('unidadeorgaonomeunidade', 'string'),
        'estado': ('unidadeorgaouf', 'string'),
        'data_compra': ('datacompra', 'date'),  # ← CORRIGIDO
    }
    
    result_data = {}
    
    for csv_col, (schema_col, col_type) in column_mapping.items():
        if csv_col in df.columns:
            if col_type == 'decimal':
                logger.debug(f"🔢 Convertendo campo numérico: {csv_col} → {schema_col}")
                result_data[schema_col] = df[csv_col].apply(convert_brazilian_number_to_decimal)
            elif col_type == 'integer':
                logger.debug(f"🔢 Convertendo campo inteiro: {csv_col} → {schema_col}")
                result_data[schema_col] = df[csv_col].apply(convert_to_integer_safe)
            elif col_type == 'date':
                logger.debug(f"📅 Convertendo campo data: {csv_col} → {schema_col}")
                result_data[schema_col] = df[csv_col].apply(convert_to_date_safe)
            else:
                result_data[schema_col] = df[csv_col].apply(convert_to_string_safe)
            
            not_null_count = result_data[schema_col].notna().sum()
            logger.debug(f"✓ {csv_col} → {schema_col} ({not_null_count}/{len(df)} não-nulos)")
        else:
            if schema_col != 'marca':
                logger.debug(f"⚠️ Coluna '{csv_col}' não encontrada no CSV")
            result_data[schema_col] = [None] * len(df)
    
    # Adicionar colunas faltantes do schema
    for col in PRECOS_SCHEMA.keys():
        if col not in result_data:
            result_data[col] = [None] * len(df)
    
    result_df = pd.DataFrame(result_data)
    
    logger.debug("➕ Adicionando metadados...")
    result_df['data_extracao'] = datetime.utcnow()
    result_df['versao_script'] = CONFIG["SCRIPT_VERSION"]
    
    logger.info(f"✅ DataFrame final: {len(result_df)} registros, {len(result_df.columns)} colunas")
    
    # Verificação de colunas NULL críticas
    logger.debug("🔍 Verificando colunas críticas...")
    colunas_criticas = ['quantidadehomologada', 'unidademedida', 'valorunitariohomologado', 'numeroitemcompra']
    for col in colunas_criticas:
        null_count = result_df[col].isna().sum()
        not_null_count = result_df[col].notna().sum()
        if null_count > 0:
            logger.warning(f"⚠️  Coluna '{col}': {not_null_count} preenchidos, {null_count} NULL")
    
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
        logger.debug("🔗 Conectando ao banco de dados...")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        columns = list(PRECOS_SCHEMA.keys()) + ['data_extracao', 'versao_script']
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        logger.debug("📝 Preparando query de inserção...")
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
        
        logger.debug("🔄 Convertendo DataFrame para tuplas...")
        data_tuples = [tuple(row) for row in df[columns].replace({np.nan: None, pd.NaT: None}).values]
        
        logger.info(f"💾 Inserindo {len(data_tuples)} registros na tabela precos_catalogo...")
        execute_batch(cursor, insert_query, data_tuples, page_size=1000)
        
        logger.debug("✅ Commit da transação...")
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"✅ {len(df)} registros de preços inseridos/atualizados com sucesso")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao inserir preços: {e}")
        import traceback
        logger.error(traceback.format_exc())
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        raise DatabaseError(f"erro ao inserir na tabela precos_catalogo - {str(e)[:100]}")

# =====================================================
# PROCESSAMENTO DE CÓDIGO
# =====================================================

def process_single_code(codigo: str, tipo: str) -> Tuple[bool, Optional[str], Optional[List[str]]]:
    """
    Processa um único código de catálogo
    
    Returns:
        (success, error_message, idcompra_list)
    """
    try:
        logger.info(f"{'='*70}")
        logger.info(f"🔄 PROCESSANDO CÓDIGO: {codigo} ({tipo})")
        logger.info(f"{'='*70}")
        
        logger.debug(f"📍 Etapa 1/3: Extração da API")
        try:
            df_raw = fetch_all_pages(codigo, tipo)
        except APIError as e:
            logger.error(f"❌ Erro na API: {e}")
            return (False, str(e), None)
        
        if df_raw is None or df_raw.empty:
            logger.warning(f"⚠️ Sem dados para código {codigo}")
            return (False, 'nenhum dado retornado pela API', None)
        
        # Extrai lista de idcompra antes de transformar
        logger.debug("📋 Normalizando colunas temporariamente para extrair idcompra...")
        df_temp = df_raw.copy()
        df_temp.columns = [normalizar_nome_coluna(col) for col in df_temp.columns]
        
        if 'id_compra' in df_temp.columns:
            idcompra_list = df_temp['id_compra'].unique().tolist()
            logger.debug(f"✓ {len(idcompra_list)} idcompra únicos encontrados")
        else:
            idcompra_list = []
            logger.warning("⚠️ Coluna id_compra não encontrada")
        
        logger.debug(f"📍 Etapa 2/3: Transformação dos dados")
        try:
            df_clean = map_csv_to_schema(df_raw)
        except DataValidationError as e:
            logger.error(f"❌ Erro na validação: {e}")
            return (False, str(e), idcompra_list)
        
        logger.debug(f"📍 Etapa 3/3: Carga no banco de dados")
        try:
            load_precos_to_cockroach(df_clean)
            logger.info(f"✅ Código {codigo} processado com sucesso!")
            return (True, None, idcompra_list)
        except DatabaseError as e:
            logger.error(f"❌ Erro no banco: {e}")
            return (False, str(e), idcompra_list)
        
    except Exception as e:
        logger.error(f"❌ Erro inesperado ao processar código {codigo}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return (False, f'erro inesperado - {str(e)[:100]}', None)

def process_code_with_retry(codigo: str, tipo: str) -> bool:
    """Processa código com retry para erros de API e atualiza Sheets"""
    logger.debug(f"🚀 Iniciando processamento de {codigo} com retry habilitado")
    
    success, error_msg, idcompra_list = process_single_code(codigo, tipo)
    
    if success:
        logger.debug("✅ Sucesso na primeira tentativa")
        status = f"sucesso - {len(idcompra_list) if idcompra_list else 0} registros inseridos"
        update_control_record(codigo, tipo, True)
        write_catalogo_status(codigo, idcompra_list, status)
        return True
    
    # Verifica se é erro de API para fazer retry
    if error_msg and ('erro ao puxar os dados da API' in error_msg or 'timeout' in error_msg or 'rate limit' in error_msg):
        logger.warning(f"⚠️  Erro de API detectado para {codigo}, aguardando {CONFIG['API_ERROR_RETRY_DELAY']}s para retry...")
        time.sleep(CONFIG['API_ERROR_RETRY_DELAY'])
        
        logger.info(f"🔄 Tentativa 2/2 para código {codigo}")
        success, error_msg, idcompra_list = process_single_code(codigo, tipo)
        
        if success:
            logger.info("✅ Sucesso na segunda tentativa")
            status = f"sucesso - {len(idcompra_list) if idcompra_list else 0} registros inseridos"
            update_control_record(codigo, tipo, True)
            write_catalogo_status(codigo, idcompra_list, status)
            return True
    
    # Falha definitiva
    logger.error(f"❌ Falha definitiva para código {codigo}: {error_msg}")
    
    # Registra erro de banco para log especial
    if error_msg and 'erro ao inserir na tabela' in error_msg:
        global db_errors_log
        db_errors_log.append({
            'codigo': codigo,
            'tipo': tipo,
            'erro': error_msg,
            'timestamp': datetime.now()
        })
        logger.error(f"🗄️ Erro de banco registrado para {codigo}")
    
    update_control_record(codigo, tipo, False, error_msg)
    write_catalogo_status(codigo, idcompra_list, error_msg)
    
    return False

# =====================================================
# PROCESSAMENTO PARALELO
# =====================================================

def process_batch_parallel(batch: List[Tuple[str, str]]) -> Tuple[int, int, int]:
    """Processa lote de códigos em paralelo com stagger"""
    logger.debug(f"🔀 Iniciando processamento paralelo de {len(batch)} códigos")
    
    sucessos = 0
    falhas_api = 0
    falhas_outras = 0
    
    with ThreadPoolExecutor(max_workers=CONFIG["PARALLEL_REQUESTS"]) as executor:
        futures = {}
        
        for i, (codigo, tipo) in enumerate(batch):
            if i > 0:
                logger.debug(f"⏳ Stagger delay: {CONFIG['STAGGER_DELAY_SECONDS']}s")
                time.sleep(CONFIG["STAGGER_DELAY_SECONDS"])
            
            future = executor.submit(process_code_with_retry, codigo, tipo)
            futures[future] = (codigo, tipo)
            logger.info(f"🚀 Thread iniciada para código: {codigo}")
        
        for future in as_completed(futures):
            codigo, tipo = futures[future]
            
            try:
                success = future.result()
                
                if success:
                    sucessos += 1
                    logger.info(f"✅ Concluído com sucesso: {codigo}")
                else:
                    # Verifica tipo de erro no controle
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
                        
                        if result and result[0] and 'erro ao puxar os dados da API' in result[0]:
                            falhas_api += 1
                        else:
                            falhas_outras += 1
                    except:
                        falhas_outras += 1
                    
                    logger.error(f"❌ Falhou: {codigo}")
                    
            except Exception as e:
                logger.error(f"❌ Exceção não capturada para {codigo}: {e}")
                falhas_outras += 1
    
    logger.debug(f"✓ Lote concluído: {sucessos} sucessos, {falhas_api} falhas API, {falhas_outras} outras falhas")
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
    
    logger.info("="*80)
    if CONFIG["MODO_TESTE"]:
        logger.info("⚠️  EXECUTANDO EM MODO TESTE ⚠️")
        logger.info(f"Códigos de teste: {CONFIG['TESTE_CODIGOS']}")
    else:
        logger.info("=== Pipeline de Preços de Catálogo (PRODUÇÃO) ===")
    logger.info(f"Versão do script: {CONFIG['SCRIPT_VERSION']}")
    logger.info(f"Tempo limite: {CONFIG['EXECUTION_TIME_LIMIT_HOURS']} hora(s)")
    logger.info(f"Processamento paralelo: {CONFIG['PARALLEL_REQUESTS']} requisições simultâneas")
    logger.info("="*80)
    
    try:
        logger.info("📝 Etapa 1/5: Criação de tabelas de controle")
        create_control_table()
        
        logger.info("📝 Etapa 2/5: Inicialização da planilha Google Sheets")
        initialize_sheets_tab()
        
        logger.info("📝 Etapa 3/5: Obtenção de códigos pendentes")
        pending_codes = get_pending_codes()
        
        if not pending_codes:
            logger.info("ℹ️ Nenhum código pendente para processar")
            return
        
        logger.info("📝 Etapa 4/5: Populando códigos na planilha")
        populate_initial_codes()
        
        logger.info("📝 Etapa 5/5: Processamento dos códigos")
        logger.info(f"📊 Total de códigos a processar: {len(pending_codes)}")
        
        total = len(pending_codes)
        processed = 0
        total_success = 0
        total_failed = 0
        consecutive_api_errors = 0
        
        for i in range(0, len(pending_codes), CONFIG["PARALLEL_REQUESTS"]):
            if not check_execution_time():
                logger.warning("⏰ Encerrando execução por limite de tempo")
                break
            
            batch = pending_codes[i:i + CONFIG["PARALLEL_REQUESTS"]]
            batch_num = (i // CONFIG["PARALLEL_REQUESTS"]) + 1
            total_batches = (len(pending_codes) + CONFIG["PARALLEL_REQUESTS"] - 1) // CONFIG["PARALLEL_REQUESTS"]
            
            logger.info(f"\n{'='*80}")
            logger.info(f">>> LOTE {batch_num}/{total_batches}")
            logger.info(f"Códigos neste lote: {[c for c, _ in batch]}")
            logger.info(f"Progresso total: {processed}/{total} ({(processed/total*100):.1f}%)")
            logger.info(f"Erros API consecutivos: {consecutive_api_errors}/{CONFIG['MAX_CONSECUTIVE_API_ERRORS']}")
            logger.info(f"{'='*80}")
            
            sucessos, falhas_api, falhas_outras = process_batch_parallel(batch)
            
            processed += len(batch)
            total_success += sucessos
            total_failed += (falhas_api + falhas_outras)
            
            if falhas_api > 0:
                consecutive_api_errors += falhas_api
            else:
                consecutive_api_errors = 0
            
            logger.info(f"\n📊 Resumo do Lote {batch_num}:")
            logger.info(f"  ✅ Sucessos: {sucessos}")
            logger.info(f"  ❌ Falhas API: {falhas_api}")
            logger.info(f"  ❌ Outras falhas: {falhas_outras}")
            
            if consecutive_api_errors >= CONFIG["MAX_CONSECUTIVE_API_ERRORS"]:
                logger.critical(f"\n🛑 LIMITE DE ERROS DE API ATINGIDO ({CONFIG['MAX_CONSECUTIVE_API_ERRORS']})")
                logger.critical("Possível problema sistêmico com a API - encerrando execução")
                break
            
            # Delay entre lotes
            if i + CONFIG["PARALLEL_REQUESTS"] < len(pending_codes):
                logger.debug(f"⏳ Aguardando 2s antes do próximo lote...")
                time.sleep(2)
        
        # Relatório final
        elapsed_time = datetime.now() - execution_start_time
        
        logger.info("\n" + "="*80)
        logger.info("=== EXECUÇÃO CONCLUÍDA ===")
        logger.info(f"Tempo de execução: {elapsed_time}")
        logger.info(f"Total processado: {processed}/{total} ({(processed/total*100):.1f}%)")
        logger.info(f"✅ Sucessos: {total_success} ({(total_success/processed*100):.1f}%)")
        logger.info(f"❌ Falhas: {total_failed} ({(total_failed/processed*100):.1f}%)")
        
        if db_errors_log:
            logger.warning(f"\n⚠️  ERROS DE BANCO DE DADOS ({len(db_errors_log)}):")
            for err in db_errors_log:
                logger.warning(f"  - {err['codigo']} ({err['tipo']}): {err['erro'][:100]}")
        
        logger.info("="*80)
        
    except DatabaseError as e:
        logger.critical(f"❌ ERRO CRÍTICO DE BANCO: {e}")
        logger.critical("Impossível continuar - verifique conexão e credenciais")
        raise
        
    except Exception as e:
        logger.error(f"❌ Erro fatal não tratado: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise

if __name__ == "__main__":
    main()
