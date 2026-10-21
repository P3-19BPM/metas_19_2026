"""Apply only the public consultation exception to the production image."""

import ast
from pathlib import Path


def enable_public_metas(source: str) -> str:
    legacy_public = '    is_public_metas = path == "/gdo_unico_metas.html"'
    if source.count(legacy_public) == 1:
        source = source.replace(legacy_public + "\n", "", 1)
        source = source.replace("if is_html and not is_public_metas and ", "if is_html and ")
    gate = "    if is_html and not _is_trusted_embed(request) and not _has_login_session(request):"
    cookie = "    if is_html and _is_trusted_embed(request) and not _has_valid_session_cookie(request):"
    if source.count(gate) != 1 or source.count(cookie) != 1:
        raise RuntimeError("Unexpected production middleware; refusing to patch authentication")
    source = source.replace(
        gate,
        '    if method == "GET" and path in {"/metas19", "/gdo_unico_metas.html"}:\n'
        '        query = f"?{request.url.query}" if request.url.query else ""\n'
        '        return RedirectResponse(url=f"/metas19/{query}", status_code=308)\n'
        '    # Consulta publica somente das metas desdobradas; nao concede sessao.\n'
        '    is_public_metas = path in {"/metas19/", "/metas19/index.html"}\n'
        '    if is_html and not is_public_metas and not _is_trusted_embed(request) and not _has_login_session(request):',
    ).replace(
        cookie,
        '    if is_html and not is_public_metas and _is_trusted_embed(request) and not _has_valid_session_cookie(request):',
    )
    ast.parse(source.lstrip("\ufeff"))
    return source


if __name__ == "__main__":
    path = Path("/app/api/app/main.py")
    path.write_text(enable_public_metas(path.read_text(encoding="utf-8")), encoding="utf-8")
