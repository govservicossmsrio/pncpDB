#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DEBUG: Verifica ordem das colunas no INSERT
"""

import pandas as pd
import psycopg2
import os
import re
from io import StringIO

COCKROACH_CONNECTION_STRING = os.getenv("COCKROACH_CONNECTION_STRING")

def normalizar_nome_coluna(nome: str) -> str:
    if not isinstance(nome, str):
        return ''
    s = nome.strip()
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    s = re.sub(r'[^a-zA-Z0-9_]+', '_', s)
    return s.lower().strip('_')

# =====================================================
# 1. ORDEM DAS COLUNAS NO BANCO
# =====================================================
print("\n" + "="*80)
print("📋 ORDEM DAS COLUNAS NO BANCO (ordinal_position)")
print("="*80)

conn = psycopg2.connect(COCKROACH_CONNECTION_STRING)
cursor = conn.cursor()

cursor.execute("""
    SELECT ordinal_position, column_name, data_type
    FROM information_schema.columns 
    WHERE table_name = 'precos_catalogo'
    ORDER BY ordinal_position
""")

colunas_banco = cursor.fetchall()
for pos, col_name, col_type in colunas_banco:
    destaque = "⚠️ BIGINT!" if col_type == 'bigint' else ""
    print(f"  {pos:2}. {col_name:35} [{col_type:15}] {destaque}")

cursor.close()
conn.close()

# =====================================================
# 2. SCHEMA USADO NO CÓDIGO PYTHON
# =====================================================
print("\n" + "="*80)
print("📋 SCHEMA DEFINIDO NO CÓDIGO PYTHON (PRECOS_SCHEMA)")
print("="*80)

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

for idx, (col_name, col_type) in enumerate(PRECOS_SCHEMA.items(), 1):
    print(f"  {idx:2}. {col_name:35} [{col_type}]")

# =====================================================
# 3. ORDEM DE INSERÇÃO NO CÓDIGO
# =====================================================
print("\n" + "="*80)
print("📋 ORDEM DE INSERÇÃO USADA NO CÓDIGO")
print("="*80)

columns = list(PRECOS_SCHEMA.keys()) + ['data_extracao', 'versao_script']
print(f"\nTotal de colunas no INSERT: {len(columns)}")
print(f"Comando SQL: INSERT INTO precos_catalogo ({', '.join(columns)}) VALUES (...)")

print("\nOrdem das colunas no INSERT:")
for idx, col in enumerate(columns, 1):
    print(f"  {idx:2}. {col}")

# =====================================================
# 4. COMPARAÇÃO: Banco vs Código
# =====================================================
print("\n" + "="*80)
print("🔍 COMPARAÇÃO: BANCO vs CÓDIGO")
print("="*80)

banco_cols = [col[1] for col in colunas_banco]
codigo_cols = columns

print("\nColunas no BANCO mas NÃO no CÓDIGO:")
for col in banco_cols:
    if col not in codigo_cols:
        print(f"  ❌ {col}")

print("\nColunas no CÓDIGO mas NÃO no BANCO:")
for col in codigo_cols:
    if col not in banco_cols:
        print(f"  ❌ {col}")

# =====================================================
# 5. SIMULAÇÃO DE INSERT
# =====================================================
print("\n" + "="*80)
print("🧪 SIMULAÇÃO: O que vai para cada coluna do banco")
print("="*80)

# Dados de exemplo do CSV
csv_exemplo = """9,4300105914782E+016	9956701	SISRP	5	V	12	PINÇA CIRÚRGICA	467756			UNIDADE	UN	0	500	21,98	0	40649293000157	ANA JULIA	COOPERFLEX	943001	GOVERNO CE	2304400	FORTALEZA	CE	86940	GOVERNO CE	E	E	2025-11-05 00:00:00.0	2025-11-06T01:10:11,484	2025-11-06T03:00:07,708	2025-11-05 00:00:00.0	2025-06-17T11:15	6515	INSTRUMENTOS"""

linha = csv_exemplo.split('\t')

# Colunas do CSV (normalizadas)
csv_cols = ['id_compra', 'id_item_compra', 'forma', 'modalidade', 'criterio_julgamento', 
            'numero_item_compra', 'descricao_item', 'codigo_item_catalogo', 'nome_unidade_medida',
            'sigla_unidade_medida', 'nome_unidade_fornecimento', 'sigla_unidade_fornecimento',
            'capacidade_unidade_fornecimento', 'quantidade', 'preco_unitario', 'percentual_maior_desconto',
            'ni_fornecedor', 'nome_fornecedor', 'marca', 'codigo_uasg', 'nome_uasg', 'codigo_municipio',
            'municipio', 'estado', 'codigo_orgao', 'nome_orgao', 'poder', 'esfera', 'data_compra',
            'data_hora_atualizacao_compra', 'data_hora_atualizacao_item', 'data_resultado',
            'data_hora_atualizacao_uasg', 'codigo_classe', 'nome_classe']

# Mapeamento usado no código
column_mapping = {
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

# Constrói idcompraitem
idcompraitem = linha[0] + str(linha[5]).zfill(5)  # id_compra + numero_item_compra

print("\nValores que seriam inseridos:")
print(f"\n  idcompraitem: '{idcompraitem}'")

for csv_col, banco_col in column_mapping.items():
    idx = csv_cols.index(csv_col)
    valor = linha[idx] if idx < len(linha) else 'N/A'
    print(f"  {banco_col:35} ← {valor}")

# Verifica a posição 18 (numeroitemcompra - BIGINT)
print("\n" + "="*80)
print("🚨 VERIFICAÇÃO CRÍTICA")
print("="*80)
print(f"\nColuna 18 do banco: numeroitemcompra (BIGINT)")
print(f"Valor CSV (numero_item_compra): '{linha[5]}'")
print(f"Tipo: {type(linha[5])}")
print(f"É número? {linha[5].isdigit()}")

# Lista todas as colunas na ordem do INSERT
print(f"\n📝 ORDEM EXATA DO INSERT:")
for i, col in enumerate(columns, 1):
    tipo_banco = next((c[2] for c in colunas_banco if c[1] == col), 'N/A')
    posicao_banco = next((c[0] for c in colunas_banco if c[1] == col), 'N/A')
    print(f"  INSERT pos {i:2} → Banco pos {posicao_banco:2} | {col:35} [{tipo_banco}]")
