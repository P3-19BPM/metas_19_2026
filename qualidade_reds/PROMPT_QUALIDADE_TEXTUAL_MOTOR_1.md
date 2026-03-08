Você é um especialista em processamento de linguagem natural.

Crie um motor Python responsável por avaliar a qualidade textual dos históricos das ocorrências policiais.

O motor receberá um dataframe contendo:

numero_ocorrencia
historico_ocorrencia

O motor deverá realizar as seguintes análises:

1 análise ortográfica
2 análise gramatical
3 tamanho do histórico
4 presença de pontuação adequada
5 identificação de textos excessivamente curtos
6 presença de elementos essenciais

Elementos essenciais esperados no histórico:

ação policial
local do fato
descrição do autor
descrição da vítima
diligências realizadas

O motor deve gerar um score de qualidade textual.

Exemplo:

score_texto = 0 a 100

Também deve gerar indicadores:

erro_ortografico
texto_curto
ausencia_pontuacao
historico_incompleto
uso_excessivo_texto_padrao

O motor deve retornar os resultados em um dataframe adicional contendo os indicadores para cada ocorrência.

Bibliotecas sugeridas:

Pandas
LanguageTool
spaCy