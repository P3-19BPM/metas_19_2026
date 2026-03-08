import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any


def _permissions_file() -> Path:
    return Path(__file__).resolve().parents[3] / "public" / "data" / "comissao_qualidade_reds.json"


def normalize_user_nr(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def normalize_cia(value: Any) -> str:
    text = str(value or "").strip().upper()
    m = re.search(r"\b(\d{1,3})\b", text)
    if m:
        return str(int(m.group(1)))
    return text


@lru_cache(maxsize=1)
def load_permissions() -> dict[str, Any]:
    p = _permissions_file()
    if not p.exists():
        return {"dev": [], "comissao": [], "subcomissoes": {}}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"dev": [], "comissao": [], "subcomissoes": {}}

    dev = [normalize_user_nr(x) for x in (data.get("dev") or []) if normalize_user_nr(x)]
    comissao = [normalize_user_nr(x) for x in (data.get("comissao") or []) if normalize_user_nr(x)]
    sub = {}
    for cia_key, members in (data.get("subcomissoes") or {}).items():
        key_norm = normalize_cia(cia_key)
        sub[key_norm] = [normalize_user_nr(x) for x in (members or []) if normalize_user_nr(x)]
    return {"dev": dev, "comissao": comissao, "subcomissoes": sub}


def resolve_profile(user_nr: str) -> dict[str, Any]:
    nr = normalize_user_nr(user_nr)
    perms = load_permissions()
    if nr in set(perms.get("dev") or []):
        return {"perfil": "dev", "cias": []}
    if nr in set(perms.get("comissao") or []):
        return {"perfil": "comissao", "cias": []}

    cias = []
    for cia_key, members in (perms.get("subcomissoes") or {}).items():
        if nr in set(members or []):
            cias.append(str(cia_key))
    if cias:
        return {"perfil": "subcomissao", "cias": sorted(set(cias))}
    return {"perfil": "sem_acesso", "cias": []}


def can_access_all(user_nr: str) -> bool:
    perfil = resolve_profile(user_nr).get("perfil")
    return perfil in {"dev", "comissao"}


def can_access_cia(user_nr: str, cia: str) -> bool:
    if can_access_all(user_nr):
        return True
    profile = resolve_profile(user_nr)
    if profile.get("perfil") != "subcomissao":
        return False
    return normalize_cia(cia) in set(profile.get("cias") or [])


def filter_by_permission(user_nr: str, cia: str, df):
    profile = resolve_profile(user_nr)
    if profile.get("perfil") in {"dev", "comissao"}:
        return df

    allowed = set(profile.get("cias") or [])
    if not allowed:
        try:
            return df.iloc[0:0]
        except Exception:
            return []

    target_cia = normalize_cia(cia)
    if target_cia and target_cia in allowed:
        return df

    # DataFrame-like
    if hasattr(df, "columns") and hasattr(df, "__getitem__"):
        try:
            if "cia_pm" not in getattr(df, "columns", []):
                return df.iloc[0:0]
            series = df["cia_pm"].astype(str).apply(normalize_cia)
            return df[series.isin(list(allowed))]
        except Exception:
            return df.iloc[0:0]

    # List[dict]-like
    if isinstance(df, list):
        out = []
        for row in df:
            cia_row = normalize_cia((row or {}).get("cia_pm"))
            if cia_row in allowed:
                out.append(row)
        return out

    return df

