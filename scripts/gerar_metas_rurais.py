"""Gera o JSON rural sem alterar os arquivos de metas gerais.

Uso: python scripts/gerar_metas_rurais.py
"""
import json
import re
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MESES = 'jan fev mar abr mai jun jul ago set out nov dez'.split()


def gerar():
    texto = (ROOT / 'META_RURAL_2026.md').read_text(encoding='utf-8-sig')
    cadastro = {}
    for nome, codigo, pop in re.findall(r'^([^\n|]+?)\t+(19\.\d+\.\d+\.\d+)\t+([\d.]+)\s*$', texto, re.M):
        cadastro[nome.strip()] = (codigo, int(pop.replace('.', '')))
    if len(cadastro) != 21:
        raise ValueError('Esperados 21 municípios no cadastro rural.')
    municipios = {nome: {'municipio': nome, 'codigo_local': codigo,
                        'codigo_companhia': '.'.join(codigo.split('.')[:2]),
                        'codigo_pelotao': '.'.join(codigo.split('.')[:3]),
                        'populacao_rural': pop, 'metas': {}}
                  for nome, (codigo, pop) in cadastro.items()}
    indicadores = {'IMV': 'imv', 'ICVPe': 'icvpe', 'ICVPa': 'icvpa', 'FURTO': 'furtos'}
    blocos = re.findall(r'^## META (\S+) RURAL 2026\s*\n(.*?)(?=^## |\Z)', texto, re.M | re.S)
    if {nome for nome, _ in blocos} != set(indicadores):
        raise ValueError('Indicadores rurais ausentes ou inesperados.')
    for nome, bloco in blocos:
        vistos = set()
        for linha in bloco.splitlines():
            if not linha.strip():
                continue
            campos = linha.strip().split('|')
            if len(campos) != 14:
                raise ValueError(f'{nome}: esperados município, população e 12 meses: {linha}')
            municipio, pop, *valores = campos
            if municipio in vistos or municipio not in municipios:
                raise ValueError(f'Município duplicado ou desconhecido: {municipio}')
            vistos.add(municipio)
            if int(pop) != municipios[municipio]['populacao_rural']:
                raise ValueError(f'População divergente: {municipio}')
            numeros = [Decimal(v.replace(',', '.')) for v in valores]
            if any(not v.is_finite() or v < 0 for v in numeros):
                raise ValueError(f'Meta inválida: {municipio}')
            municipios[municipio]['metas'][indicadores[nome]] = dict(zip(MESES, map(float, numeros)))
        if vistos != set(municipios):
            raise ValueError(f'{nome}: lista de municípios incompleta.')
    totais = {}
    for municipio in municipios.values():
        codigo = municipio['codigo_companhia']
        totais[codigo] = totais.get(codigo, 0) + municipio['populacao_rural']
    if totais != {'19.42': 28573, '19.155': 21032, '19.232': 62936}:
        raise ValueError(f'Totais de população divergentes: {totais}')
    return {'ano': 2026, 'abrangencia': 'rural', 'fonte': 'META_RURAL_2026.md',
            'ordem_meses': MESES, 'populacao_rural_total': sum(totais.values()),
            'municipios': list(municipios.values())}


if __name__ == '__main__':
    destino = ROOT / 'public/data/metas_rurais_2026.json'
    destino.write_text(json.dumps(gerar(), ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(f'Gerado: {destino}')
