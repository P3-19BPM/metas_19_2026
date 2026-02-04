# Publicação de Versões - Painel GDO 19º BPM

Este documento descreve o fluxo padrão para publicar novas versões da imagem Docker no Docker Hub.

## Pré-requisitos

- Docker Desktop (ou Docker Engine) instalado.
- Buildx habilitado (`docker buildx version`).
- Conta no Docker Hub com acesso ao repositório `mmpgnovaiscrud/painel-gdo-19bpm`.
- Estar na pasta raiz do projeto (`metas_19_2026`).

## Convenção de versão

- Última publicada: `v5`
- Próxima publicação: `v6`
- Formato de tag: `mmpgnovaiscrud/painel-gdo-19bpm:vN`

## Passo a passo (versão v6)

1. Validar alterações locais

```powershell
git status
```

2. (Opcional, recomendado) Commit das mudanças

```powershell
git add .
git commit -m "feat: ajustes IRTD e melhorias de layout - v6"
```

3. Fazer login no Docker Hub

```powershell
docker login
```

4. Gerar e publicar imagem multi-arquitetura (AMD64 + ARM64)

```powershell
docker buildx build --platform linux/amd64,linux/arm64 -t mmpgnovaiscrud/painel-gdo-19bpm:v6 --push .
```

5. Verificar publicação

- Conferir no Docker Hub se a tag `v6` apareceu.
- Validar digest/plataformas da imagem publicada.

## Atualização da VPS / EasyPanel

Após publicar a `v6`, atualizar o deploy para usar a nova tag.

### Opção A - via docker-compose

No arquivo `docker-compose.yml`, trocar a imagem para:

```yaml
image: mmpgnovaiscrud/painel-gdo-19bpm:v6
```

Aplicar atualização:

```powershell
docker compose pull
docker compose up -d
```

### Opção B - via painel (EasyPanel)

- Editar serviço e alterar a imagem para `mmpgnovaiscrud/painel-gdo-19bpm:v6`.
- Salvar e realizar restart/redeploy do serviço.

## Checklist rápido pós-deploy

- A aplicação abre sem erro de `Failed to fetch`.
- Ícones/favicon carregam corretamente.
- Indicadores IMV/ICVPe/ICVPa com PLR normal.
- IRTD exibindo somente Meta (conforme regra atual).

## Modelo para próximas versões

Substituir apenas o número da tag:

```powershell
docker buildx build --platform linux/amd64,linux/arm64 -t mmpgnovaiscrud/painel-gdo-19bpm:v6 --push .
```

## Histórico de referência

- `v5`: última versão publicada antes deste documento.
- `v6`: próxima publicação com os ajustes recentes de IRTD.
