Você é um especialista em análise de consistência de dados policiais.

Crie um motor Python responsável por verificar a convergência entre os dados do REDS.

Entrada:

numero_ocorrencia
cia_pm
pelotao_pm
data_fato
natureza_codigo
historico_ocorrencia

O motor deve verificar:

1 coerência entre natureza e histórico
2 coerência entre local e pelotão
3 coerência entre data e narrativa
4 tempo de empenho da guarnição
5 padrões de erro na classificação da natureza

Exemplos de análise:

histórico menciona furto mas natureza é C99000

histórico menciona arma de fogo mas natureza não corresponde

O motor deve gerar indicadores:

possivel_erro_natureza
possivel_erro_local
possivel_erro_classificacao
tempo_empenho_suspeito
possivel_erro_natureza_violenta

Esses indicadores devem ser adicionados ao dataframe.

Utilizar técnicas simples de NLP para identificar palavras-chave no histórico.