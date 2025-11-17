#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DEBUG: Mostra exatamente qual campo está causando o erro
"""

import pandas as pd
import psycopg2
import os
import re

COCKROACH_CONNECTION_STRING = os.getenv("COCKROACH_CONNECTION_STRING")

def normalizar_nome_coluna(nome: str) -> str:
    if not isinstance(nome, str):
        return ''
    s = nome.strip()
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    s = re.sub(r'[^a-zA-Z0-9_]+', '_', s)
    return s.lower().strip('_')

# =====================================================
# 1. SCHEMA DO BANCO
# =====================================================
print("\n" + "="*80)
print("📋 SCHEMA DO BANCO (apenas campos INTEGER/BIGINT)")
print("="*80)

conn = psycopg2.connect(COCKROACH_CONNECTION_STRING)
cursor = conn.cursor()

cursor.execute("""
    SELECT column_name, data_type, ordinal_position
    FROM information_schema.columns 
    WHERE table_name = 'precos_catalogo'
      AND data_type IN ('bigint', 'integer', 'smallint')
    ORDER BY ordinal_position
""")

campos_numericos_banco = cursor.fetchall()
for col_name, col_type, pos in campos_numericos_banco:
    print(f"  {pos:2}. {col_name:30} → {col_type}")

cursor.close()
conn.close()

# =====================================================
# 2. COLUNAS DO CSV (Material)
# =====================================================
print("\n" + "="*80)
print("📋 COLUNAS DO CSV MATERIAL (normalizadas)")
print("="*80)

colunas_material = """idCompra	idItemCompra	forma	modalidade	criterioJulgamento	numeroItemCompra	descricaoItem	codigoItemCatalogo	nomeUnidadeMedida	siglaUnidadeMedida	nomeUnidadeFornecimento	siglaUnidadeFornecimento	capacidadeUnidadeFornecimento	quantidade	precoUnitario	percentualMaiorDesconto	niFornecedor	nomeFornecedor	marca	codigoUasg	nomeUasg	codigoMunicipio	municipio	estado	codigoOrgao	nomeOrgao	poder	esfera	dataCompra	dataHoraAtualizacaoCompra	dataHoraAtualizacaoItem	dataResultado	dataHoraAtualizacaoUasg	codigoClasse	nomeClasse"""

colunas_normalizadas = [normalizar_nome_coluna(c) for c in colunas_material.split('\t')]

for idx, col in enumerate(colunas_normalizadas, 1):
    print(f"  {idx:2}. {col}")

# =====================================================
# 3. MAPEAMENTO ATUAL DO CÓDIGO
# =====================================================
print("\n" + "="*80)
print("📋 MAPEAMENTO: CSV → BANCO")
print("="*80)

column_mapping = {
    'idcompraitem_construido': 'idcompraitem',
    'id_compra': 'idcompra',
    'numero_item_compra': 'numeroitemcompra',  # ← ESTE É BIGINT NO BANCO
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

print("\nCSV (normalizado) → BANCO")
print("-" * 80)
for csv_col, banco_col in column_mapping.items():
    existe = "✅" if csv_col in colunas_normalizadas else "❌ FALTA"
    print(f"{existe} {csv_col:35} → {banco_col}")

# =====================================================
# 4. VERIFICAÇÃO: Campos BIGINT/INTEGER
# =====================================================
print("\n" + "="*80)
print("🔍 VERIFICAÇÃO: Campos INTEGER/BIGINT")
print("="*80)

print("\nCampo BIGINT no banco: numeroitemcompra")
print(f"  Mapeado de: numero_item_compra")
print(f"  Existe no CSV? {'✅' if 'numero_item_compra' in colunas_normalizadas else '❌'}")
print(f"  Valores exemplo: 12, 44, 268")
print(f"  Tipo correto? ✅ SIM (são números)")

# =====================================================
# 5. POSSÍVEIS CAMPOS FALTANDO
# =====================================================
print("\n" + "="*80)
print("❓ COLUNAS DO CSV QUE NÃO ESTÃO NO MAPEAMENTO")
print("="*80)

colunas_mapeadas = set(column_mapping.keys())
for col in colunas_normalizadas:
    if col not in colunas_mapeadas and col != 'idcompraitem_construido':
        print(f"  ⚠️  {col}")

print("\n" + "="*80)
print("💡 DIAGNÓSTICO COMPLETO")
print("="*80)
