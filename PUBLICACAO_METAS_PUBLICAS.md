# Metas públicas — v9.2.8

Página: `/metas19/` (arquivo `public/metas19/index.html`). Consulta sem login de IMV, ICVPe e ICVPa,
com filtros de companhia e meses e exportação CSV/XLSX. As linhas recolhidas
também são exportadas. Os valores numéricos mantêm a precisão dos dados.

Links diretos:

- `/metas19/?indicador=meta_imv`
- `/metas19/?indicador=meta_icvpe`
- `/metas19/?indicador=meta_icvpa`

Os endereços `/metas19` e `/gdo_unico_metas.html` redirecionam para
`/metas19/`, preservando os parâmetros do indicador.

A página usa os JSONs existentes em `public/data/` e `metas_plr.json`.
O exportador SheetJS 0.20.3 e sua licença estão em `public/vendor/`.
O restante do painel mantém a autenticação; a consulta pública não cria sessão.

## Metas rurais 2026

Foram acrescentadas abas independentes de IMV Rural, ICVPe Rural, ICVPa Rural
e Furtos Rural. As metas gerais e seus JSONs são preservados.

Dados: `public/data/metas_rurais_2026.json`, com 21 municípios, população rural,
códigos de companhia/pelotão/local e 12 meses por indicador. A população total
é 112.541; a fonte é `META_RURAL_2026.md`. A ordem dos valores mensais na fonte
é interpretada como janeiro a dezembro. Os valores fracionários são mantidos
no JSON e nas exportações; a tela apresenta até duas casas decimais.

Para regenerar e validar:

```powershell
python scripts/gerar_metas_rurais.py
python -m unittest discover -s scripts -p test_metas_rurais.py
```

Links diretos: `/metas19/?indicador=meta_imv_rural`,
`/metas19/?indicador=meta_icvpe_rural`,
`/metas19/?indicador=meta_icvpa_rural` e
`/metas19/?indicador=meta_furtos_rural`.

As abas reutilizam filtros, cards e exportação CSV/XLSX. As tabelas rurais
também mostram as colunas dos meses selecionados. As taxas usam exclusivamente
a população rural, e os dados rurais não usam a PLR das metas gerais.
O Dockerfile incremental inclui somente o novo JSON rural além da página e dos
arquivos que já copiava. A versão v9.2.8 descrita abaixo é a próxima publicação
prevista, baseada na v9.2.7 atualmente disponível no Docker Hub.

## Publicação incremental

Como existem alterações locais de outras tarefas, esta versão usa a imagem
de produção v9.2.7 como base. O Dockerfile incremental copia a página,
o novo JSON rural e o exportador e aplica a exceção de autenticação. Os demais arquivos e dados
da produção são preservados. A compilação falha se o middleware de produção
não corresponder ao formato esperado.

```powershell
docker buildx build --platform linux/amd64 -f Dockerfile.public-metas -t mmpgnovaiscrud/painel-gdo-19bpm:v9.2.8 --push .
docker buildx imagetools inspect mmpgnovaiscrud/painel-gdo-19bpm:v9.2.8
```

A arquitetura é `linux/amd64`, a mesma disponibilizada pela imagem v9.2.7.
A imagem base está fixada pelo digest para permitir reprodução da publicação.

Após publicar, configurar o serviço existente no EasyPanel para
`mmpgnovaiscrud/painel-gdo-19bpm:v9.2.8` e realizar o redeploy.
Para reverter, usar novamente `v9.2.7`.

O `docker-compose.yml` da raiz é legado e não representa o serviço FastAPI atual.
