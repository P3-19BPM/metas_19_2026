Você é um analista de dados especializado em padrões operacionais de registros policiais.

Crie um motor Python responsável por identificar padrões operacionais nos registros REDS.

Entrada:

numero_ocorrencia
cia_pm
pelotao_pm
natureza_codigo
historico_ocorrencia

O motor deve identificar padrões como:

excesso de natureza C99
excesso de naturezas W01
excesso de naturezas W02
excesso de naturezas W03
excesso de naturezas W04

Também deve identificar:

históricos extremamente semelhantes
possível reutilização de texto
concentração de ocorrências problemáticas em um pelotão

O motor deve produzir indicadores:

alerta_c99
alerta_w01
alerta_w02
alerta_w03
alerta_w04
possivel_copia_historico
repeticao_por_guarnicao

Também deve gerar estatísticas por:

companhia
pelotão
natureza

Utilizar Pandas para agregação de dados.