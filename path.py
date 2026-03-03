import os
from pathlib import Path
from flask import Flask, render_template_string, jsonify
import tkinter as tk
from tkinter import filedialog

app = Flask(__name__)

IGNORE_DIR_NAMES = {
    "__pycache__",
    "node_modules",
    ".git",
    ".idea",
    ".vscode",
    "dist",
    "build",
    ".next",
    ".nuxt",
    ".cache",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".tox",
    ".gradle",
    ".terraform",
    ".expo",
    ".parcel-cache",
}

IGNORE_DIR_PREFIXES = (
    "venv",
    ".venv",
)

HTML_PAGE = r"""
<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>MMPG - Gerador de Caminhos</title>

  <link rel="icon" href="/static/image/logo-mmpg.ico" />

  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet" />

  <style>
    body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
  </style>
</head>

<body class="bg-slate-100 min-h-screen flex items-center justify-center p-4">

  <div class="w-full max-w-6xl bg-white rounded-2xl shadow-2xl overflow-hidden flex flex-col h-[92vh] border border-slate-200">

    <!-- TOP BAR / BRAND -->
    <div class="bg-gradient-to-r from-indigo-950 via-indigo-900 to-indigo-950 text-white px-5 py-4 flex flex-col md:flex-row md:items-center md:justify-between gap-4 shrink-0">

      <div class="flex items-center gap-4">
        <div class="w-12 h-12 rounded-xl bg-white/10 border border-white/20 flex items-center justify-center overflow-hidden">
          <img src="/static/image/logo_mmpg.png" alt="Logo MMPG" class="w-10 h-10 object-contain" />
        </div>

        <div>
          <h1 class="text-lg md:text-xl font-extrabold tracking-wide leading-tight">
            MMPG SOLUÇÕES
          </h1>
          <p class="text-xs md:text-sm text-indigo-200 leading-tight">
            Práticas e Rápidas • Gerador de Caminhos do Projeto
          </p>
        </div>
      </div>

      <div class="flex flex-wrap gap-2 justify-start md:justify-end">
        <button onclick="selectFolder()"
          class="bg-indigo-600 hover:bg-indigo-500 text-white font-bold py-2 px-4 rounded-xl shadow transition duration-200 flex items-center gap-2 border border-indigo-400 cursor-pointer">
          <i class="fas fa-folder-open"></i>
          <span>Selecionar Pasta</span>
        </button>

        <button onclick="copyToClipboard()"
          class="bg-emerald-500 hover:bg-emerald-400 text-white font-bold py-2 px-4 rounded-xl shadow transition duration-200 flex items-center gap-2 border border-emerald-300 cursor-pointer">
          <i class="fas fa-copy"></i>
          <span>Copiar Tudo</span>
        </button>

        <button onclick="openHelp()"
          class="bg-white/10 hover:bg-white/20 text-white font-bold py-2 px-4 rounded-xl shadow transition duration-200 flex items-center gap-2 border border-white/20 cursor-pointer">
          <i class="fas fa-circle-question"></i>
          <span>Ajuda</span>
        </button>
      </div>
    </div>

    <!-- INFO BAR -->
    <div class="bg-slate-50 border-b px-5 py-3 text-sm text-slate-700 flex flex-col md:flex-row md:items-center md:justify-between gap-2 shrink-0">
      <div class="truncate">
        <span class="font-semibold text-slate-900">Pasta selecionada:</span>
        <span id="current-folder" class="text-slate-600">Nenhuma</span>
      </div>

      <div class="text-xs text-slate-500">
        Ignorando: <span class="font-semibold">__pycache__</span>, <span class="font-semibold">node_modules</span>,
        <span class="font-semibold">venv*</span>, <span class="font-semibold">.venv*</span>, <span class="font-semibold">.git</span>...
      </div>
    </div>

    <!-- TEXTAREA -->
    <div class="flex-grow relative bg-slate-50">
      <textarea id="source-code"
        class="w-full h-full p-6 font-mono text-sm text-slate-800 bg-[#f8fafc] focus:outline-none resize-none border-none"
        readonly spellcheck="false">Clique em "Selecionar Pasta" para gerar os caminhos do seu projeto...</textarea>
    </div>

    <!-- FOOTER -->
    <div class="bg-white border-t px-5 py-3 text-xs text-slate-500 flex items-center justify-between shrink-0">
      <span>© <span id="year"></span> MMPG SOLUÇÕES • Ferramentas rápidas para produtividade</span>
      <span class="text-slate-400">Localhost • Seguro • Sem envio de dados</span>
    </div>
  </div>

  <!-- TOAST -->
  <div id="toast"
    class="fixed bottom-8 right-8 bg-emerald-600 text-white px-6 py-4 rounded-xl shadow-xl transform translate-y-20 opacity-0 transition-all duration-300 flex items-center gap-3 z-50">
    <i class="fas fa-check-circle text-xl"></i>
    <div>
      <h4 class="font-bold">Copiado!</h4>
      <p class="text-sm opacity-90">Os caminhos foram enviados para a área de transferência.</p>
    </div>
  </div>

  <!-- HELP MODAL -->
  <div id="help-modal" class="fixed inset-0 bg-black/50 hidden items-center justify-center z-50 p-4">
    <div class="w-full max-w-2xl bg-white rounded-2xl shadow-2xl overflow-hidden border border-slate-200">

      <div class="bg-indigo-950 text-white px-5 py-4 flex items-center justify-between">
        <div class="flex items-center gap-3">
          <i class="fas fa-circle-question text-lg"></i>
          <h3 class="font-extrabold tracking-wide">Ajuda • Como funciona</h3>
        </div>
        <button onclick="closeHelp()" class="text-white/80 hover:text-white transition">
          <i class="fas fa-xmark text-xl"></i>
        </button>
      </div>

      <div class="p-5 text-slate-700 space-y-4 text-sm leading-relaxed">
        <p>
          Este gerador lista automaticamente todos os <b>caminhos de pastas e arquivos</b> de um projeto,
          em ordem hierárquica, pronto para você copiar e colar onde quiser.
        </p>

        <div class="bg-slate-50 border border-slate-200 rounded-xl p-4">
          <p class="font-bold text-slate-900 mb-2">O que ele ignora automaticamente?</p>
          <ul class="list-disc pl-5 space-y-1">
            <li><b>__pycache__</b> (cache do Python)</li>
            <li><b>node_modules</b> (dependências do Node)</li>
            <li><b>venv*</b> e <b>.venv*</b> (ambientes virtuais)</li>
            <li><b>.git</b> e pastas comuns de build/cache</li>
          </ul>
        </div>

        <div class="bg-emerald-50 border border-emerald-200 rounded-xl p-4">
          <p class="font-bold text-emerald-900 mb-1">Privacidade</p>
          <p class="text-emerald-800">
            O processo roda <b>100% local</b> no seu computador. Nenhum arquivo é enviado para a internet.
          </p>
        </div>

        <div class="bg-indigo-50 border border-indigo-200 rounded-xl p-4">
          <p class="font-bold text-indigo-900 mb-2">Como usar (passo a passo)</p>
          <ol class="list-decimal pl-5 space-y-1">
            <li>Clique em <b>Selecionar Pasta</b></li>
            <li>Escolha a pasta raiz do projeto (ex: <code class="bg-white px-1 rounded">E:\Projetos\GDO_2026</code>)</li>
            <li>Os caminhos serão listados no painel</li>
            <li>Clique em <b>Copiar Tudo</b> e cole onde desejar</li>
          </ol>
        </div>
      </div>

      <div class="p-5 border-t bg-white flex justify-end">
        <button onclick="closeHelp()"
          class="bg-indigo-600 hover:bg-indigo-500 text-white font-bold py-2 px-5 rounded-xl shadow transition duration-200 border border-indigo-400">
          Entendi
        </button>
      </div>

    </div>
  </div>

  <script>
    document.getElementById("year").textContent = new Date().getFullYear();

    async function selectFolder() {
      const textarea = document.getElementById('source-code');
      const folderSpan = document.getElementById('current-folder');

      textarea.value = "Aguardando seleção da pasta...";
      folderSpan.textContent = "Abrindo seletor...";

      try {
        const res = await fetch("/select-folder", { method: "POST" });
        const data = await res.json();

        if (!data.ok) {
          textarea.value = data.error || "Erro ao selecionar pasta.";
          folderSpan.textContent = "Nenhuma";
          return;
        }

        folderSpan.textContent = data.folder;
        textarea.value = data.content;

      } catch (e) {
        textarea.value = "Erro de comunicação com o servidor.";
        folderSpan.textContent = "Nenhuma";
      }
    }

    function copyToClipboard() {
      const textarea = document.getElementById('source-code');
      textarea.select();
      textarea.setSelectionRange(0, 999999);

      try {
        document.execCommand('copy');
        showToast();
      } catch (err) {
        alert('Erro ao copiar. Use Ctrl+C.');
      }
    }

    function showToast() {
      const toast = document.getElementById('toast');
      toast.classList.remove('translate-y-20', 'opacity-0');

      setTimeout(() => {
        toast.classList.add('translate-y-20', 'opacity-0');
      }, 2200);
    }

    function openHelp() {
      const modal = document.getElementById("help-modal");
      modal.classList.remove("hidden");
      modal.classList.add("flex");
    }

    function closeHelp() {
      const modal = document.getElementById("help-modal");
      modal.classList.add("hidden");
      modal.classList.remove("flex");
    }

    // Fecha modal clicando fora
    document.getElementById("help-modal").addEventListener("click", function(e) {
      if (e.target.id === "help-modal") closeHelp();
    });

    // Fecha modal com ESC
    document.addEventListener("keydown", function(e) {
      if (e.key === "Escape") closeHelp();
    });
  </script>
</body>
</html>
"""


def should_ignore_dir(dir_name: str) -> bool:
    if dir_name in IGNORE_DIR_NAMES:
        return True
    for prefix in IGNORE_DIR_PREFIXES:
        if dir_name.startswith(prefix):
            return True
    return False


def list_paths_hierarchical(root_path: Path) -> str:
    root_path = root_path.resolve()
    lines = [str(root_path)]

    for current_root, dirs, files in os.walk(root_path):
        current_root_path = Path(current_root)

        # impede entrar em pastas ignoradas
        dirs[:] = sorted([d for d in dirs if not should_ignore_dir(d)])

        # lista pastas
        for d in dirs:
            lines.append(str((current_root_path / d).resolve()))

        # lista arquivos
        for f in sorted(files):
            lines.append(str((current_root_path / f).resolve()))

    return "\n".join(lines)


def open_folder_dialog() -> str | None:
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)

    folder = filedialog.askdirectory(title="Selecione a pasta raiz do projeto")
    if not folder:
        return None
    return folder


@app.route("/")
def index():
    return render_template_string(HTML_PAGE)


@app.route("/select-folder", methods=["POST"])
def select_folder():
    folder = open_folder_dialog()
    if not folder:
        return jsonify({"ok": False, "error": "Seleção cancelada pelo usuário."})

    try:
        content = list_paths_hierarchical(Path(folder))
        return jsonify({"ok": True, "folder": folder, "content": content})
    except Exception as e:
        return jsonify({"ok": False, "error": f"Erro ao listar caminhos: {str(e)}"})


if __name__ == "__main__":
    # Acesse: http://127.0.0.1:5000
    app.run(host="127.0.0.1", port=5000, debug=False)
