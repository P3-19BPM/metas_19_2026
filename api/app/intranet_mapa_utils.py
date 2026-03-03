import io
import json
import zipfile
from functools import lru_cache
from pathlib import Path
from xml.sax.saxutils import escape


MAPA_GEOJSON_PATH = Path(__file__).resolve().parents[2] / "public" / "data" / "mapas" / "SubSetores_19BPM_estruturado.geojson"
MAPA_BAIRROS_GEOJSON_PATH = (
    Path(__file__).resolve().parents[2]
    / "public"
    / "data"
    / "mapas"
    / "Com_Bairros_Setorizacao_nivel_bairros_teo_noco_cruzeiro.json"
)


def _mojibake_score(text: str) -> int:
    if not text:
        return 0
    bad = ("Ã", "Â", "â", "\u0081", "\u009d", "\ufffd")
    return sum(text.count(ch) for ch in bad)


def normalize_mapa_text(value):
    if value is None:
        return ""
    if not isinstance(value, str):
        return value
    out = value
    if _mojibake_score(out) <= 0:
        return out
    for _ in range(2):
        try:
            cand = out.encode("latin-1").decode("utf-8")
        except Exception:
            break
        if _mojibake_score(cand) < _mojibake_score(out):
            out = cand
        else:
            break
    return out


def _as_text(value) -> str:
    if value is None:
        return ""
    return str(normalize_mapa_text(value))


@lru_cache(maxsize=1)
def load_mapa_geojson() -> dict:
    return json.loads(MAPA_GEOJSON_PATH.read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def load_mapa_bairros_geojson() -> dict:
    if not MAPA_BAIRROS_GEOJSON_PATH.exists():
        return {"type": "FeatureCollection", "features": []}
    return json.loads(MAPA_BAIRROS_GEOJSON_PATH.read_text(encoding="utf-8"))


def _norm_feature(feature: dict) -> dict:
    props = dict(feature.get("properties") or {})
    subsetor = _as_text(props.get("SUB_SETOR") or props.get("Name")).strip()
    setor = _as_text(props.get("SETOR")).strip()
    pelotao = _as_text(props.get("PELOTAO")).strip()
    cia = _as_text(props.get("CIA_PM")).strip()
    nm_mun = _as_text(props.get("NM_MUN") or props.get("municipio")).strip()
    cd_mun_raw = props.get("CD_MUNICIP")
    cd_mun = ""
    if cd_mun_raw is not None and cd_mun_raw != "":
        try:
            cd_mun = str(int(float(cd_mun_raw)))
        except Exception:
            cd_mun = _as_text(cd_mun_raw)

    area_raw = props.get("AREA")
    area = None
    try:
        area = float(area_raw) if area_raw is not None else None
    except Exception:
        area = None

    out_props = {
        "id": subsetor or _as_text(props.get("Name")).strip() or nm_mun,
        "name": _as_text(props.get("Name")).strip() or subsetor,
        "cia_code": cia,
        "pelotao_code": pelotao,
        "setor_code": setor,
        "subsetor_code": subsetor,
        "cd_municipio": cd_mun,
        "municipio_nome": nm_mun,
        "municipio": _as_text(props.get("municipio")).strip() or nm_mun.upper(),
        "area": area,
    }
    return {
        "type": "Feature",
        "geometry": feature.get("geometry"),
        "properties": out_props,
    }


def list_mapa_features() -> list[dict]:
    geo = load_mapa_geojson()
    return [_norm_feature(f) for f in list(geo.get("features") or [])]


def list_mapa_export_rows() -> list[dict]:
    rows = []
    for f in list_mapa_features():
        p = f["properties"]
        rows.append(
            {
                "municipio": p.get("municipio_nome") or "",
                "bairro": "",
                "cia_pm": p.get("cia_code") or "",
                "pelotao": p.get("pelotao_code") or "",
                "setor": p.get("setor_code") or "",
                "subsetor": p.get("subsetor_code") or "",
                "codigo_municipio": p.get("cd_municipio") or "",
                "area_aprox": p.get("area") if p.get("area") is not None else "",
            }
        )
    rows.sort(key=lambda r: (str(r.get("cia_pm") or ""), str(r.get("subsetor") or "")))
    return rows


def list_mapa_bairro_rows() -> list[dict]:
    geo = load_mapa_bairros_geojson()
    seen = set()
    rows: list[dict] = []
    for feature in list(geo.get("features") or []):
        props = dict(feature.get("properties") or {})
        subsetor = _as_text(props.get("SUB_SETOR") or props.get("Name")).strip()
        setor = _as_text(props.get("SETOR")).strip()
        pelotao = _as_text(props.get("PELOTAO")).strip()
        cia = _as_text(props.get("CIA_PM")).strip()
        municipio = _as_text(props.get("NM_MUN") or "").strip()
        bairro = _as_text(props.get("BAIRRO") or "").strip()
        cd_mun_raw = props.get("CD_MUNICIP")
        cd_mun = ""
        if cd_mun_raw is not None and cd_mun_raw != "":
            try:
                cd_mun = str(int(float(cd_mun_raw)))
            except Exception:
                cd_mun = _as_text(cd_mun_raw)
        if not subsetor:
            continue
        key = (subsetor, bairro, cd_mun, municipio)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "municipio": municipio,
                "bairro": bairro,
                "cia_pm": cia,
                "pelotao": pelotao,
                "setor": setor,
                "subsetor": subsetor,
                "codigo_municipio": cd_mun,
            }
        )
    rows.sort(key=lambda r: (str(r.get("cia_pm") or ""), str(r.get("subsetor") or ""), str(r.get("bairro") or "")))
    return rows


def _xlsx_col_name(idx: int) -> str:
    out = ""
    n = idx
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out


def _cell_xml(ref: str, value) -> str:
    if value is None:
        return f'<c r="{ref}"/>'
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f'<c r="{ref}"><v>{value}</v></c>'
    txt = escape(str(value))
    return f'<c r="{ref}" t="inlineStr"><is><t>{txt}</t></is></c>'


def build_xlsx_bytes(rows: list[dict], columns: list[tuple[str, str]], sheet_name: str = "Mapa") -> bytes:
    sheet_rows: list[str] = []
    # Header
    header_cells = []
    for cidx, (_, label) in enumerate(columns, start=1):
        header_cells.append(_cell_xml(f"{_xlsx_col_name(cidx)}1", label))
    sheet_rows.append(f'<row r="1">{"".join(header_cells)}</row>')

    for ridx, row in enumerate(rows, start=2):
        cells = []
        for cidx, (key, _) in enumerate(columns, start=1):
            cells.append(_cell_xml(f"{_xlsx_col_name(cidx)}{ridx}", row.get(key)))
        sheet_rows.append(f'<row r="{ridx}">{"".join(cells)}</row>')

    last_ref = f"{_xlsx_col_name(max(1, len(columns)))}{max(1, len(rows) + 1)}"
    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<dimension ref="A1:{last_ref}"/>'
        '<sheetViews><sheetView workbookViewId="0"/></sheetViews>'
        '<sheetFormatPr defaultRowHeight="15"/>'
        f"<sheetData>{''.join(sheet_rows)}</sheetData>"
        "</worksheet>"
    )

    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        "<sheets>"
        f'<sheet name="{escape(sheet_name)}" sheetId="1" r:id="rId1"/>'
        "</sheets>"
        "</workbook>"
    )

    wb_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/>'
        '<Relationship Id="rId2" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
        'Target="styles.xml"/>'
        "</Relationships>"
    )

    root_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>'
        "</Relationships>"
    )

    content_types_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '<Override PartName="/xl/styles.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        "</Types>"
    )

    styles_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>'
        '<fills count="2"><fill><patternFill patternType="none"/></fill>'
        '<fill><patternFill patternType="gray125"/></fill></fills>'
        '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>'
        '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
        "</styleSheet>"
    )

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types_xml)
        zf.writestr("_rels/.rels", root_rels_xml)
        zf.writestr("xl/workbook.xml", workbook_xml)
        zf.writestr("xl/_rels/workbook.xml.rels", wb_rels_xml)
        zf.writestr("xl/worksheets/sheet1.xml", sheet_xml)
        zf.writestr("xl/styles.xml", styles_xml)
    return buf.getvalue()
