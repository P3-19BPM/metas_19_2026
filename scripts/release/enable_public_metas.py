"""Apply only the public consultation exception to the production image."""

import ast
from pathlib import Path


def enable_public_metas(source: str) -> str:
    gate = "    if is_html and not _is_trusted_embed(request) and not _has_login_session(request):"
    cookie = "    if is_html and _is_trusted_embed(request) and not _has_valid_session_cookie(request):"
    if source.count(gate) != 1 or source.count(cookie) != 1:
        raise RuntimeError("Unexpected production middleware; refusing to patch authentication")
    source = source.replace(
        gate,
        '    # Consulta publica somente das metas desdobradas; nao concede sessao.\n'
        '    is_public_metas = path == "/gdo_unico_metas.html"\n'
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
