"""Validação dos dados rurais: python -m unittest discover -s scripts -p test_metas_rurais.py"""
import json
import unittest
from collections import defaultdict
from gerar_metas_rurais import ROOT, MESES, gerar


class MetasRuraisTest(unittest.TestCase):
    def test_json_corresponde_ao_documento(self):
        salvo = json.loads((ROOT / 'public/data/metas_rurais_2026.json').read_text(encoding='utf-8'))
        self.assertEqual(salvo, gerar())

    def test_cobertura_e_populacao(self):
        dados = gerar()
        self.assertEqual(dados['populacao_rural_total'], 112541)
        self.assertEqual(len(dados['municipios']), 21)
        totais = defaultdict(int)
        for municipio in dados['municipios']:
            totais[municipio['codigo_pelotao']] += municipio['populacao_rural']
            self.assertEqual(set(municipio['metas']), {'imv', 'icvpe', 'icvpa', 'furtos'})
            for meses in municipio['metas'].values():
                self.assertEqual(list(meses), MESES)
        self.assertEqual(dict(totais), {'19.42.3': 28573, '19.155.1': 13273,
            '19.155.2': 7759, '19.232.1': 15049, '19.232.2': 19105, '19.232.3': 28782})

    def test_valores_fracionarios_e_zeros(self):
        municipios = {m['municipio']: m for m in gerar()['municipios']}
        self.assertEqual(municipios['MALACACHETA']['metas']['furtos']['jun'], 2.16430020283977)
        self.assertEqual(municipios['TEOFILO OTONI']['metas']['imv']['dez'], 0.8890237)
        self.assertEqual(sum(municipios['PESCADOR']['metas']['furtos'].values()), 0)


if __name__ == '__main__':
    unittest.main()
