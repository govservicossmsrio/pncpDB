import os
import sys
import time
import json
import logging
import pandas as pd
import requests
import gspread
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime

# --- CONFIGURAÇÃO CENTRALIZADA ---
CONFIG = {
    "GCP_SECRET_NAME": "GCP_SA_KEY",
    "GCP_PROJECT_ID": "pncpDB",
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

# --- CONFIGURAÇÃO DE LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)

# --- ESCOPOS E CREDENCIAIS ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/bigquery']

# --- FUNÇÕES DE INFRAESTRUTURA E AUTENTICAÇÃO ---
def get_gcp_credentials():
    sa_key_json = os.getenv(CONFIG["GCP_SECRET_NAME"])
    if not sa_key_json:
        logging.critical(f"A variável de ambiente '{CONFIG['GCP_SECRET_NAME']}' não foi encontrada.")
        raise ValueError(f"Secret '{CONFIG['GCP_SECRET_NAME']}' não configurado.")
    try:
        info = json.loads(sa_key_json)
        return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    except Exception as e:
        logging.critical(f"Falha ao decodificar ou usar o JSON da conta de serviço: {e}")
        raise

def get_ids_from_sheet(creds):
    try:
        logging.info("Lendo IDs da planilha Google Sheets...")
        gc = gspread.authorize(creds)
        worksheet = gc.open_by_key(CONFIG["SPREADSHEET_ID"]).worksheet(CONFIG["SHEET_NAME"])
        ids = worksheet.col_values(1)[1:]
        logging.info(f"Encontrados {len(ids)} IDs na planilha.")
        return [item for item in ids if item]
    except Exception as e:
        logging.critical(f"Falha ao ler a planilha do Google Sheets: {e}")
        raise

def load_df_to_bigquery(df, table_id, client):
    if df.empty:
        return
    table_full_id = f"{CONFIG['GCP_PROJECT_ID']}.{CONFIG['BIGQUERY_DATASET']}.{table_id}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
    try:
        job = client.load_table_from_dataframe(df, table_full_id, job_config=job_config)
        job.result()
        logging.info(f"  -> Sucesso: {len(df)} registros carregados para '{table_full_id}'.")
    except Exception as e:
        logging.error(f"  -> Falha ao carregar dados para '{table_full_id}': {e}")
        if hasattr(job, 'errors') and job.errors:
            for error in job.errors:
                logging.error(f"     Erro no BigQuery: {error['message']}")

def should_process_compra(id_compra, client):
    query = f"""
        SELECT
            COUNT(idCompraItem) AS total_items,
            COUNTIF(LOWER(situacaoCompraItemNome) IN ('homologado', 'fracassado', 'deserto', 'anulado/revogado/cancelado')) AS final_items
        FROM `{CONFIG['GCP_PROJECT_ID']}.{CONFIG['BIGQUERY_DATASET']}.itens_compra`
        WHERE idCompra = @id_compra
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("id_compra", "STRING", id_compra)])
    try:
        results = client.query(query, job_config=job_config).to_dataframe()
        if not results.empty:
            row = results.iloc[0]
            if row['total_items'] > 0 and row['total_items'] == row['final_items']:
                logging.info(f"ID {id_compra} já está finalizado no BD. Pulando.")
                return False
        return True
    except Exception:
        return True

def get_pncp_data(endpoint_key, id_param, param_type="codigo"):
    url = f"{CONFIG['PNCP_BASE_URL']}/{CONFIG['ENDPOINTS'][endpoint_key]}"
    params = {'tipo': 'idCompra' if endpoint_key != 'RESULTADOS' else 'idCompraItem', param_type: id_param}
    try:
        response = requests.get(url, params=params, timeout=20, headers={'accept': '*/*'})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.warning(f"  -> API Call Falhou para {endpoint_key} com ID {id_param}: {e}")
        return None

# --- FUNÇÃO DE TRANSFORMAÇÃO ---

def processar_compra_data(id_compra):
    """Orquestra a extração e transformação para um único idCompra, alinhado com o schema."""
    dataframes = {"compras": pd.DataFrame(), "itens_compra": pd.DataFrame(), "resultados_itens": pd.DataFrame()}

    compra_data = get_pncp_data("CONTRATACOES", id_compra)
    if not compra_data or not compra_data.get('data'):
        return None

    itens_data = get_pncp_data("ITENS", id_compra)
    itens = itens_data.get('data', []) if itens_data else []

    # 1. Processar tabela 'compras'
    df_compras = pd.json_normalize(compra_data['data'], sep='_')
    
    # Renomeação para alinhar com o schema
    df_compras.rename(columns={
        'id': 'idCompra', 'modalidade_codigo': 'codigoModalidade', 'modalidade_nome': 'modalidadeNome',
        'unidadeOrgao_codigoUnidade': 'unidadeOrgaoCodigoUnidade', 'unidadeOrgao_nomeUnidade': 'unidadeOrgaoNomeUnidade',
        'unidadeOrgao_municipio_nome': 'unidadeOrgaoMunicipioNome', 'unidadeOrgao_uf_sigla': 'unidadeOrgaoUfSigla',
        'orgaoEntidade_esfera_id': 'orgaoEntidadeEsferaId'
    }, inplace=True)
    
    # Adicionar atributos customizados
    df_compras['dataExtracao'] = datetime.now().isoformat()
    df_compras['itensTotal'] = len(itens)
    df_compras['itensResultados'] = sum(1 for item in itens if item.get('temResultado', False))
    df_compras['itensHomologados'] = sum(1 for item in itens if str(item.get('situacaoCompraItem', {}).get('nome', '')).lower() == 'homologado')
    df_compras['itensFracassados'] = sum(1 for item in itens if str(item.get('situacaoCompraItem', {}).get('nome', '')).lower() == 'fracassado')
    df_compras['itensDesertos'] = sum(1 for item in itens if str(item.get('situacaoCompraItem', {}).get('nome', '')).lower() == 'deserto')
    df_compras['itensOutros'] = sum(1 for item in itens if str(item.get('situacaoCompraItem', {}).get('nome', '')).lower() == 'anulado/revogado/cancelado')
    
    # Garantir que todas as colunas do schema existam
    schema_compras = [
        'idCompra', 'numeroCompra', 'anoCompraPncp', 'codigoModalidade', 'modalidadeNome', 'srp', 
        'unidadeOrgaoCodigoUnidade', 'unidadeOrgaoNomeUnidade', 'unidadeOrgaoMunicipioNome', 'unidadeOrgaoUfSigla', 
        'orgaoEntidadeEsferaId', 'processo', 'objetoCompra', 'valorTotalEstimado', 'valorTotalHomologado', 
        'existeResultado', 'dataAberturaPropostaPncp', 'contratacaoExcluida', 'dataExtracao', 'itensTotal', 
        'itensResultados', 'itensHomologados', 'itensFracassados', 'itensDesertos', 'itensOutros'
    ]
    dataframes["compras"] = df_compras.reindex(columns=schema_compras)

    if not itens:
        return dataframes

    # 2. Processar tabela 'itens_compra'
    df_itens = pd.json_normalize(itens, sep='_')
    df_itens.rename(columns={
        'id': 'idCompraItem', 'numero': 'numeroItemCompra', 'materialOuServico_nome': 'materialOuServicoNome',
        'tipoBeneficio_nome': 'tipoBeneficioNome', 'catalogo_codigo': 'codItemCatalogo',
        'situacaoCompraItem_nome': 'situacaoCompraItemNome', 'fornecedor_cnpj': 'cnpjFornecedor', 'fornecedor_nome': 'nomeFornecedor'
    }, inplace=True)
    df_itens['data_extracao'] = datetime.now().isoformat()

    schema_itens = [
        'idCompraItem', 'idCompra', 'numeroItemCompra', 'numeroGrupo', 'materialOuServicoNome', 'tipoBeneficioNome', 
        'codItemCatalogo', 'descricaoResumida', 'descricaodetalhada', 'quantidade', 'unidadeMedida', 
        'valorUnitarioEstimado', 'valorTotal', 'temResultado', 'situacaoCompraItemNome', 
        'cnpjFornecedor', 'nomeFornecedor', 'data_extracao'
    ]
    dataframes["itens_compra"] = df_itens.reindex(columns=schema_itens)

    # 3. Processar tabela 'resultados_itens'
    resultados_list = []
    for item in itens:
        if item.get('temResultado'):
            id_item = item.get('id')
            resultado_data = get_pncp_data("RESULTADOS", id_item, param_type='codigo')
            if resultado_data and resultado_data.get('data'):
                for res in resultado_data['data']:
                    res.update({'idCompraItem': id_item, 'idCompra': id_compra, 'data_extracao': datetime.now().isoformat()})
                    resultados_list.append(res)
    
    if resultados_list:
        df_resultados = pd.json_normalize(resultados_list, sep='_')
        df_resultados.rename(columns={
            'naturezaJuridica_nome': 'naturezaJuridicaNome', 'porteFornecedor_nome': 'porteFornecedorNome',
            'paisOrigemProdutoServico_id': 'paisOrigemProdutoServicoId'
        }, inplace=True)
        schema_resultados = [
            'idCompraItem', 'idCompra', 'niFornecedor', 'tipoPessoa', 'nomeRazaoSocialFornecedor', 
            'naturezaJuridicaNome', 'porteFornecedorNome', 'quantidadeHomologada', 'valorUnitarioHomologado', 
            'valorTotalHomologado', 'percentualDesconto', 'dataResultadoPncp', 'aplicacaoBeneficioMeepp', 
            'moedaEstrangeiraId', 'dataCotacaoMoedaEstrangeira', 'valorNominalMoedaEstrangeira', 
            'paisOrigemProdutoServicoId', 'data_extracao'
        ]
        dataframes["resultados_itens"] = df_resultados.reindex(columns=schema_resultados)

    return dataframes

# --- FUNÇÃO PRINCIPAL DE ORQUESTRAÇÃO ---

def main():
    logging.info("--- INICIANDO PIPELINE DE ETL DO PNCP ---")
    try:
        creds = get_gcp_credentials()
        bq_client = bigquery.Client(credentials=creds, project=CONFIG["GCP_PROJECT_ID"])
        all_ids = get_ids_from_sheet(creds)
    except Exception as e:
        logging.critical(f"Falha na configuração inicial. Abortando. Erro: {e}")
        sys.exit(1)

    work_list = [id_compra for id_compra in all_ids if should_process_compra(id_compra, bq_client)]
    logging.info(f"Após filtragem, {len(work_list)} IDs a serem processados.")
    
    failed_attempts = {id_compra: 0 for id_compra in work_list}
    consecutive_batch_failures = 0

    while work_list:
        current_batch = work_list[:CONFIG["BATCH_SIZE"]]
        batch_had_success = False
        logging.info(f"--- Processando lote de {len(current_batch)} IDs: {current_batch} ---")

        for id_compra in current_batch:
            logging.info(f"Processando ID: {id_compra} (Tentativa {failed_attempts[id_compra] + 1})")
            processed_data = processar_compra_data(id_compra)
            if processed_data:
                batch_had_success = True
                for table_name, df in processed_data.items():
                    load_df_to_bigquery(df, table_name, bq_client)
                work_list.remove(id_compra)
            else:
                logging.warning(f"Falha ao processar ID: {id_compra}")
                failed_attempts[id_compra] += 1
                if failed_attempts[id_compra] >= CONFIG["MAX_RETRIES_PER_ITEM"]:
                    logging.error(f"ID {id_compra} atingiu o máximo de {CONFIG['MAX_RETRIES_PER_ITEM']} falhas. Desistindo.")
                    work_list.remove(id_compra)
        
        if batch_had_success:
            consecutive_batch_failures = 0
            if work_list: # Só espera se ainda houver trabalho a fazer
                logging.info(f"Lote concluído. Aguardando {CONFIG['SUCCESS_DELAY_SECONDS']}s...")
                time.sleep(CONFIG['SUCCESS_DELAY_SECONDS'])
        else:
            consecutive_batch_failures += 1
            logging.warning(f"Lote inteiro falhou. Contagem de falhas consecutivas: {consecutive_batch_failures}")
            delay = CONFIG["RETRY_DELAYS_SECONDS"].get(consecutive_batch_failures)
            if delay == "CANCEL":
                logging.critical(f"Atingido o máximo de {consecutive_batch_failures} falhas de lote. Abortando.")
                sys.exit(1)
            elif delay:
                logging.info(f"Aguardando {delay}s antes da próxima tentativa...")
                time.sleep(delay)
    
    logging.info("--- PIPELINE DE ETL DO PNCP CONCLUÍDO COM SUCESSO ---")

if __name__ == "__main__":
    main()
