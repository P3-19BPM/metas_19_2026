# Plano de Commits (Boas Práticas)

Este arquivo organiza os commits por responsabilidade, usando Conventional Commits.
Cada arquivo aparece em apenas **um** commit.

---

## Commit 1

### Título
`feat(frontend): implementar suporte completo do IRTD com exibição focada em Meta`

### Arquivos deste commit
- `public/index.html`

### Detalhe do commit
- Implementa carregamento da meta IRTD a partir de JSON dedicado.
- Separa regra de IRTD da lógica PLR (IMV/ICVPe/ICVPa).
- Exibe IRTD com foco em **Meta** (ocultando Total/Taxa quando aplicável).
- Ajusta visual e renderização condicional por indicador.
- Corrige paths relativos de ícones/favicons para funcionamento em ambiente local.

### Comando sugerido
```bash
git add public/index.html
git commit -m "feat(frontend): implementar suporte completo do IRTD com exibição focada em Meta"
```

---

## Commit 2

### Título
`feat(assets): adicionar pacote de ícones e favicons da aplicação`

### Arquivos deste commit
- `public/imagens/Icones_logo_19bpm/android-chrome-192x192.png`
- `public/imagens/Icones_logo_19bpm/android-chrome-512x512.png`
- `public/imagens/Icones_logo_19bpm/apple-touch-icon.png`
- `public/imagens/Icones_logo_19bpm/favicon-16x16.png`
- `public/imagens/Icones_logo_19bpm/favicon-32x32.png`
- `public/imagens/Icones_logo_19bpm/favicon-96x96.png`
- `public/imagens/Icones_logo_19bpm/favicon.ico`
- `public/imagens/Icones_logo_19bpm/mstile-150x150.png`

### Detalhe do commit
- Adiciona os assets oficiais de ícone para navegador, atalhos mobile e tiles.
- Dá suporte ao branding consistente em desktop/mobile.

### Comando sugerido
```bash
git add public/imagens/Icones_logo_19bpm/android-chrome-192x192.png public/imagens/Icones_logo_19bpm/android-chrome-512x512.png public/imagens/Icones_logo_19bpm/apple-touch-icon.png public/imagens/Icones_logo_19bpm/favicon-16x16.png public/imagens/Icones_logo_19bpm/favicon-32x32.png public/imagens/Icones_logo_19bpm/favicon-96x96.png public/imagens/Icones_logo_19bpm/favicon.ico public/imagens/Icones_logo_19bpm/mstile-150x150.png
git commit -m "feat(assets): adicionar pacote de ícones e favicons da aplicação"
```

---

## Commit 3

### Título
`feat(data): incluir base de metas PLR para consumo do painel`

### Arquivos deste commit
- `public/data/metas_plr.json`
- `public/data/.gitkeep`

### Detalhe do commit
- Adiciona arquivo de metas PLR usado pelos indicadores compatíveis.
- Garante presença da pasta `public/data` no repositório com `.gitkeep`.

### Comando sugerido
```bash
git add public/data/metas_plr.json public/data/.gitkeep
git commit -m "feat(data): incluir base de metas PLR para consumo do painel"
```

---

## Commit 4

### Título
`chore(docker): ajustar empacotamento e deploy da aplicação`

### Arquivos deste commit
- `Dockerfile`
- `docker-compose.yml`
- `.dockerignore`

### Detalhe do commit
- Ajusta build da imagem e artefatos incluídos no container.
- Atualiza orquestração de execução/deploy.
- Melhora higiene de contexto de build com `.dockerignore`.

### Comando sugerido
```bash
git add Dockerfile docker-compose.yml .dockerignore
git commit -m "chore(docker): ajustar empacotamento e deploy da aplicação"
```

---

## Commit 5

### Título
`chore(repo): atualizar regras de versionamento e artefato de suporte`

### Arquivos deste commit
- `.gitignore`
- `.vscode.zip`

### Detalhe do commit
- Atualiza regras de ignore do repositório.
- Versiona pacote de suporte da IDE conforme fluxo atual do projeto.

### Comando sugerido
```bash
git add .gitignore .vscode.zip
git commit -m "chore(repo): atualizar regras de versionamento e artefato de suporte"
```

---

## Commit 6

### Título
`docs(release): documentar fluxo de versionamento e publicação da v6`

### Arquivos deste commit
- `documentacao.md`
- `comit.md`

### Detalhe do commit
- Documenta processo de build/push multi-arquitetura.
- Define passo a passo de atualização para VPS/EasyPanel.
- Registra plano de commits por responsabilidade.

### Comando sugerido
```bash
git add documentacao.md comit.md
git commit -m "docs(release): documentar fluxo de versionamento e publicação da v6"
```

---

## Commits pendentes (alterações atuais)

## Commit 7

### Título
`feat(ppag): adicionar base e página de metas PPAG`

### Arquivos deste commit
- `public/data/metas_ppag.json`
- `public/metas_ppag.html`

### Detalhe do commit
- Adiciona a base de dados de metas PPAG em JSON para consumo no frontend.
- Inclui página HTML de referência do PPAG com estrutura e conteúdo operacional.
- Prepara os insumos para integração da visualização PPAG ao painel principal.

### Comando sugerido
```bash
git add public/data/metas_ppag.json public/metas_ppag.html
git commit -m "feat(ppag): adicionar base e página de metas PPAG"
```

---

## Commit 8

### Título
`feat(frontend): integrar PPAG ao painel principal e ajustar navegação`

### Arquivos deste commit
- `public/index.html`

### Detalhe do commit
- Integra o módulo PPAG ao `index.html` no padrão visual do sistema.
- Ajusta navegação/sidebar para coexistência com os demais indicadores.
- Consolida filtros e renderização para suportar os novos dados PPAG.

### Comando sugerido
```bash
git add public/index.html
git commit -m "feat(frontend): integrar PPAG ao painel principal e ajustar navegação"
```
