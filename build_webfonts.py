#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


PT_BR_CODEPOINTS = [
    0x00C1,  # A acute
    0x00C2,  # A circumflex
    0x00C3,  # A tilde
    0x00C0,  # A grave
    0x00C9,  # E acute
    0x00CA,  # E circumflex
    0x00CD,  # I acute
    0x00D3,  # O acute
    0x00D4,  # O circumflex
    0x00D5,  # O tilde
    0x00DA,  # U acute
    0x00C7,  # C cedilla
    0x00E1,  # a acute
    0x00E2,  # a circumflex
    0x00E3,  # a tilde
    0x00E0,  # a grave
    0x00E9,  # e acute
    0x00EA,  # e circumflex
    0x00ED,  # i acute
    0x00F3,  # o acute
    0x00F4,  # o circumflex
    0x00F5,  # o tilde
    0x00FA,  # u acute
    0x00E7,  # c cedilla
]


@dataclass
class FontMeta:
    path: Path
    family: str
    subfamily: str
    full_name: str
    flavor: str | None
    missing_pt_br: list[str]


def _require_fonttools():
    try:
        from fontTools.ttLib import TTFont  # type: ignore
    except Exception as exc:  # pragma: no cover
        print("ERRO: fontTools nao esta instalado.")
        print("Instale com: pip install fonttools brotli")
        print(f"Detalhe: {exc}")
        raise SystemExit(2)
    return TTFont


def _name_record_to_text(rec) -> str:
    try:
        return rec.toUnicode()
    except Exception:
        try:
            return rec.string.decode("utf-8", errors="replace")
        except Exception:
            return str(rec.string)


def read_font_meta(path: Path) -> FontMeta:
    TTFont = _require_fonttools()
    font = TTFont(str(path))
    try:
        names = font["name"].names
        cmap = font.getBestCmap() or {}
        family = next((_name_record_to_text(n) for n in names if n.nameID == 1), "")
        subfamily = next((_name_record_to_text(n) for n in names if n.nameID == 2), "")
        full_name = next((_name_record_to_text(n) for n in names if n.nameID == 4), "")
        flavor = getattr(font, "flavor", None)
        missing = [hex(cp) for cp in PT_BR_CODEPOINTS if cp not in cmap]
        return FontMeta(
            path=path,
            family=family,
            subfamily=subfamily,
            full_name=full_name,
            flavor=flavor,
            missing_pt_br=missing,
        )
    finally:
        font.close()


def convert_font(src: Path, flavor: str, overwrite: bool) -> tuple[bool, str, Path]:
    TTFont = _require_fonttools()
    dst = src.with_suffix(f".{flavor}")
    if dst.exists() and not overwrite:
        return True, "skip (exists)", dst
    try:
        font = TTFont(str(src))
        try:
            font.flavor = flavor
            font.save(str(dst))
        finally:
            font.close()
        return True, "ok", dst
    except Exception as exc:
        return False, str(exc), dst


def validate_generated_font(dst: Path, expected_src_meta: FontMeta, expected_flavor: str) -> tuple[bool, str]:
    if not dst.exists():
        return False, "arquivo nao foi gerado"
    try:
        meta = read_font_meta(dst)
    except Exception as exc:
        return False, f"falha ao abrir fonte gerada: {exc}"
    problems: list[str] = []
    if (meta.flavor or "") != expected_flavor:
        problems.append(f"flavor={meta.flavor!r} (esperado {expected_flavor!r})")
    if expected_src_meta.family and meta.family != expected_src_meta.family:
        problems.append(f"family={meta.family!r} (esperado {expected_src_meta.family!r})")
    if expected_src_meta.subfamily and meta.subfamily != expected_src_meta.subfamily:
        problems.append(f"subfamily={meta.subfamily!r} (esperado {expected_src_meta.subfamily!r})")
    if meta.missing_pt_br:
        problems.append(f"faltam glifos PT-BR: {', '.join(meta.missing_pt_br)}")
    if problems:
        return False, "; ".join(problems)
    return True, "ok"


def iter_sources(fonts_root: Path, explicit: list[str]) -> Iterable[Path]:
    if explicit:
        for item in explicit:
            p = Path(item)
            if not p.is_absolute():
                p = Path.cwd() / p
            if p.is_dir():
                yield from sorted([x for x in p.rglob("*") if x.suffix.lower() in {".ttf", ".otf"}])
            elif p.suffix.lower() in {".ttf", ".otf"}:
                yield p
        return
    yield from sorted([x for x in fonts_root.rglob("*") if x.suffix.lower() in {".ttf", ".otf"}])


def print_meta(meta: FontMeta) -> None:
    rel = meta.path
    print(f"- {rel}")
    print(f"  family: {meta.family or '-'}")
    print(f"  subfamily: {meta.subfamily or '-'}")
    print(f"  full: {meta.full_name or '-'}")
    print(f"  flavor: {meta.flavor or 'sfnt/ttf'}")
    print(f"  pt-BR glyphs: {'OK' if not meta.missing_pt_br else 'FALTANDO'}")


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Converte fontes TTF/OTF para WOFF/WOFF2 e valida metadados/cobertura PT-BR."
    )
    ap.add_argument("--fonts-root", default="public/fonts", help="Pasta base para busca recursiva (padrao: public/fonts)")
    ap.add_argument(
        "--paths",
        nargs="*",
        default=[],
        help="Arquivos/pastas especificos para converter (sobrescreve busca em --fonts-root).",
    )
    ap.add_argument("--overwrite", action="store_true", help="Sobrescreve .woff/.woff2 existentes.")
    ap.add_argument("--woff-only", action="store_true", help="Gera apenas WOFF.")
    ap.add_argument("--woff2-only", action="store_true", help="Gera apenas WOFF2.")
    ap.add_argument("--validate-only", action="store_true", help="Nao converte; apenas valida arquivos ja gerados.")
    args = ap.parse_args()

    if args.woff_only and args.woff2_only:
        print("ERRO: use apenas um entre --woff-only e --woff2-only.")
        return 2

    fonts_root = Path(args.fonts_root)
    sources = list(dict.fromkeys(iter_sources(fonts_root, args.paths)))
    if not sources:
        print("Nenhuma fonte .ttf/.otf encontrada.")
        return 1

    targets: list[str]
    if args.woff_only:
        targets = ["woff"]
    elif args.woff2_only:
        targets = ["woff2"]
    else:
        targets = ["woff", "woff2"]

    print(f"Fontes encontradas: {len(sources)}")
    ok_count = 0
    fail_count = 0

    for src in sources:
        try:
            src_meta = read_font_meta(src)
        except Exception as exc:
            print(f"\n[ERRO] {src}")
            print(f"  Falha ao ler fonte original: {exc}")
            fail_count += 1
            continue

        print()
        print_meta(src_meta)

        for flavor in targets:
            dst = src.with_suffix(f".{flavor}")
            if args.validate_only:
                ok, msg = validate_generated_font(dst, src_meta, flavor)
                print(f"  [{flavor}] validar {dst.name}: {'OK' if ok else 'ERRO'} - {msg}")
                ok_count += int(ok)
                fail_count += int(not ok)
                continue

            ok_conv, conv_msg, out_path = convert_font(src, flavor, args.overwrite)
            print(f"  [{flavor}] gerar {out_path.name}: {'OK' if ok_conv else 'ERRO'} - {conv_msg}")
            if not ok_conv:
                fail_count += 1
                continue
            ok_val, val_msg = validate_generated_font(out_path, src_meta, flavor)
            print(f"  [{flavor}] validar {out_path.name}: {'OK' if ok_val else 'ERRO'} - {val_msg}")
            ok_count += int(ok_val)
            fail_count += int(not ok_val)

    print()
    print(f"Resumo: validacoes OK={ok_count} | erros={fail_count}")
    if fail_count:
        print("Dica: para WOFF2, normalmente precisa de 'brotli' instalado junto com fontTools.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
