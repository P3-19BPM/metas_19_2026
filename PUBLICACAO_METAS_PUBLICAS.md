# Metas públicas — v9.2.7

Página: `/gdo_unico_metas.html`. Consulta sem login de IMV, ICVPe e ICVPa,
com filtros de companhia e meses e exportação CSV/XLSX. As linhas recolhidas
também são exportadas. Os valores numéricos mantêm a precisão dos dados.

Links diretos:

- `/gdo_unico_metas.html?indicador=meta_imv`
- `/gdo_unico_metas.html?indicador=meta_icvpe`
- `/gdo_unico_metas.html?indicador=meta_icvpa`

A página usa os JSONs existentes em `public/data/` e `metas_plr.json`.
O exportador SheetJS 0.20.3 e sua licença estão em `public/vendor/`.
O restante do painel mantém a autenticação; a consulta pública não cria sessão.

## Publicação incremental

Como existem alterações locais de outras tarefas, esta versão usa a imagem
de produção v9.2.6 como base. O Dockerfile incremental copia apenas a página
e o exportador e aplica a exceção de autenticação. Os demais arquivos e dados
da produção são preservados. A compilação falha se o middleware de produção
não corresponder ao formato esperado.

```powershell
docker buildx build --platform linux/amd64 -f Dockerfile.public-metas -t mmpgnovaiscrud/painel-gdo-19bpm:v9.2.7 --push .
docker buildx imagetools inspect mmpgnovaiscrud/painel-gdo-19bpm:v9.2.7
```

A arquitetura é `linux/amd64`, a mesma disponibilizada pela imagem v9.2.6.
A imagem base está fixada pelo digest para permitir reprodução da publicação.

Após publicar, configurar o serviço existente no EasyPanel para
`mmpgnovaiscrud/painel-gdo-19bpm:v9.2.7` e realizar o redeploy.
Para reverter, usar novamente `v9.2.6`.

O `docker-compose.yml` da raiz é legado e não representa o serviço FastAPI atual.
