import json
import re
import unicodedata
from collections import Counter
from typing import Any


_GENERIC_PATTERNS = (
    "sem alteracao",
    "nada a relatar",
    "sem novidades",
    "sem novidades dignas",
    "feito contato no local",
)

_ESSENTIAL_BLOCKS = {
    "acao_policial": ("abord", "prend", "conduz", "localiz", "compareceu", "acionad"),
    "local_fato": ("rua", "av", "avenida", "bairro", "rodovia", "km", "residenc"),
    "autor": ("autor", "suspeit", "envolvid", "infrator"),
    "vitima": ("vitima", "ofendid", "solicitant"),
    "diligencia": ("rastream", "diligenc", "patrulh", "busca", "levantamento"),
}

_NATURE_KEYWORDS = {
    "ROUBO": ("roubo", "assalto", "subtraiu mediante", "grave ameaca"),
    "FURTO": ("furto", "furtou", "subtraiu"),
    "AMEACA": ("ameaca", "ameacou", "intimid"),
    "TRAFICO": ("trafico", "entorpec", "droga", "substancia"),
    "HOMICIDIO": ("homicidio", "matou", "cadaver", "morte"),
    "ESTUPRO_VULNERAVEL": ("estupro de vulneravel",),
    "ESTUPRO": ("estupro", "conjuncao carnal"),
}

_PLATE_PATTERNS = (
    re.compile(r"^[A-Z]{3}[0-9]{4}$"),  # antigo: ABC1234
    re.compile(r"^[A-Z]{3}[0-9][A-Z][0-9]{2}$"),  # mercosul: ABC1D23
    re.compile(r"^[A-Z]{3}[0-9][A-Z0-9][0-9]{2}$"),  # tolerante
)

_ALPHANUM_CODE_PATTERNS = (
    re.compile(r"^[A-Z][0-9]{5,}$"),  # ex: W01000
    re.compile(r"^[A-Z]{2,}[0-9]{2,}$"),
    re.compile(r"^[0-9]{5,}[A-Z]?$"),
)

_COMMON_MISSPELLINGS = {
    "ocorencia", "ocorrenciaa", "veiulo", "veciulo", "vitma", "vitinma",
    "testmunha", "abordajem", "condusido", "suspeitoo", "apreensaoo",
    "delegaciaa", "flagrantee", "homiicidio", "homissidio", "furtto",
    "rouboo", "ameacaa", "historioco", "guarnicaoo",
}


def _norm_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _tokenize(text: str) -> list[str]:
    return re.findall(r"[A-Za-z\u00C0-\u00FF0-9]+", text or "")


def _strip_accents(text: str) -> str:
    norm = unicodedata.normalize("NFD", text or "")
    return "".join(ch for ch in norm if unicodedata.category(ch) != "Mn")


def _norm_token(token: str) -> str:
    return _strip_accents(str(token or "").strip().lower())


def _is_vehicle_plate_token(token: str) -> bool:
    t = re.sub(r"[^A-Za-z0-9]", "", str(token or "")).upper()
    if len(t) < 7 or len(t) > 8:
        return False
    return any(p.match(t) for p in _PLATE_PATTERNS)


def _is_known_code_token(token: str) -> bool:
    t = re.sub(r"[^A-Za-z0-9]", "", str(token or "")).upper()
    return any(p.match(t) for p in _ALPHANUM_CODE_PATTERNS)


def _orthography_error_flag(words: list[str]) -> bool:
    valid_words = 0
    suspicious = 0
    explicit_miss = 0
    for raw in words:
        tok = str(raw or "").strip()
        if len(tok) <= 1:
            continue
        lw = _norm_token(tok)
        if not lw:
            continue

        # Ignore identifiers (placas/codigos) to reduce false positives.
        if _is_vehicle_plate_token(tok) or _is_known_code_token(tok):
            continue
        if lw.isdigit():
            continue

        valid_words += 1
        cur_suspicious = False
        if lw in _COMMON_MISSPELLINGS:
            explicit_miss += 1
            cur_suspicious = True
        if re.search(r"(.)\1{3,}", lw):
            cur_suspicious = True
        if len(lw) >= 7 and not re.search(r"[aeiou]", lw):
            cur_suspicious = True
        if re.search(r"[0-9]", tok) and re.search(r"[A-Za-z]", tok):
            cur_suspicious = True

        if cur_suspicious:
            suspicious += 1

    if valid_words == 0:
        return False
    ratio = suspicious / max(1, valid_words)
    if explicit_miss >= 1:
        return True
    if valid_words <= 12 and suspicious >= 1:
        return True
    if suspicious >= 2:
        return True
    return ratio >= 0.12


def _text_quality(historico: str) -> dict[str, Any]:
    t = _norm_text(historico)
    if not t:
        return {
            "score_textual": 0,
            "alerta_texto_curto": True,
            "erro_ortografico": False,
            "ausencia_pontuacao": True,
            "historico_incompleto": True,
            "uso_excessivo_texto_padrao": True,
        }

    words = _tokenize(t)
    word_count = len(words)
    short_text = word_count < 20
    no_punct = not bool(re.search(r"[.;,:!?]", t))
    generic = any(p in t for p in _GENERIC_PATTERNS)
    typo = _orthography_error_flag(words)

    essential_hits = 0
    for _, keys in _ESSENTIAL_BLOCKS.items():
        if any(k in t for k in keys):
            essential_hits += 1
    incomplete = essential_hits < 3

    score = 100
    if short_text:
        score -= 35
    if no_punct:
        score -= 15
    if incomplete:
        score -= 20
    if generic:
        score -= 10
    if typo:
        score -= 20
    score = max(0, min(100, score))

    return {
        "score_textual": int(score),
        "alerta_texto_curto": short_text,
        "erro_ortografico": typo,
        "ausencia_pontuacao": no_punct,
        "historico_incompleto": incomplete,
        "uso_excessivo_texto_padrao": generic,
    }


def _nature_suggestion(historico: str) -> str:
    t = _norm_text(historico)
    if not t:
        return ""
    for label, keys in _NATURE_KEYWORDS.items():
        if any(k in t for k in keys):
            return label
    return ""


def _convergence_alert(row: dict[str, Any], natureza_sugerida: str) -> dict[str, Any]:
    hist = _norm_text(row.get("historico_ocorrencia"))
    natureza_ref = _norm_text(f"{row.get('natureza_codigo')} {row.get('natureza_descricao_longa')}")

    mismatch = bool(natureza_sugerida) and natureza_sugerida.lower().replace("_", " ") not in natureza_ref
    local_hint = any(k in hist for k in ("rua ", "avenida", "bairro", "rodovia", "km "))
    no_pelotao = not str(row.get("pelotao_pm") or "").strip()
    possible_local_error = local_hint and no_pelotao

    hrs = [int(h) for h in re.findall(r"\b(\d{1,2})\s*h(?:oras?)?\b", hist)]
    tempo_suspeito = any(h >= 8 for h in hrs)

    violent_hint = any(k in hist for k in ("arma de fogo", "disparo", "homicidio", "cadaver", "lesao"))
    violent_nature = any(k in natureza_ref for k in ("homic", "roub", "lesao", "estupro"))
    erro_violenta = violent_hint and not violent_nature

    return {
        "alerta_convergencia": mismatch or possible_local_error or tempo_suspeito or erro_violenta,
        "possivel_erro_natureza": mismatch,
        "possivel_erro_local": possible_local_error,
        "tempo_empenho_suspeito": tempo_suspeito,
        "possivel_erro_natureza_violenta": erro_violenta,
    }


def _pattern_flags(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    text_keys = []
    for row in rows:
        text = _norm_text(row.get("historico_ocorrencia"))
        text_keys.append(text if len(text) >= 20 else "")
    freq = Counter([t for t in text_keys if t])

    by_pelotao: dict[str, list[str]] = {}
    for row in rows:
        pel = str(row.get("pelotao_pm") or "").strip() or "SEM_PELOTAO"
        nat = str(row.get("natureza_codigo") or "").strip().upper()
        by_pelotao.setdefault(pel, []).append(nat)

    pel_problem_ratio: dict[str, bool] = {}
    for pel, nats in by_pelotao.items():
        total = len(nats)
        if total < 5:
            pel_problem_ratio[pel] = False
            continue
        prob = sum(1 for n in nats if n.startswith(("C99", "W01", "W02", "W03", "W04")))
        pel_problem_ratio[pel] = (prob / total) >= 0.6

    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row.get("numero_ocorrencia") or "")
        nat = str(row.get("natureza_codigo") or "").strip().upper()
        pel = str(row.get("pelotao_pm") or "").strip() or "SEM_PELOTAO"
        text = _norm_text(row.get("historico_ocorrencia"))
        c99 = nat.startswith("C99")
        w01 = nat.startswith("W01")
        w02 = nat.startswith("W02")
        w03 = nat.startswith("W03")
        w04 = nat.startswith("W04")
        copy = bool(text) and freq.get(text, 0) >= 3
        rep_guarn = pel_problem_ratio.get(pel, False)
        out[key] = {
            "alerta_c99": c99,
            "alerta_w01": w01,
            "alerta_w02": w02,
            "alerta_w03": w03,
            "alerta_w04": w04,
            "possivel_copia_historico": copy,
            "repeticao_por_guarnicao": rep_guarn,
            "alerta_padrao_operacional": c99 or w01 or w02 or w03 or w04 or copy or rep_guarn,
        }
    return out


def analyze_quality_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"rows": [], "summary": {"total_analisados": 0, "inconformidades": 0}}

    pattern_map = _pattern_flags(rows)
    out_rows: list[dict[str, Any]] = []
    inconformidades = 0

    for row in rows:
        numero = str(row.get("numero_ocorrencia") or "").strip()
        if not numero:
            continue

        text_eval = _text_quality(str(row.get("historico_ocorrencia") or ""))
        natureza_sugerida = _nature_suggestion(str(row.get("historico_ocorrencia") or ""))
        conv = _convergence_alert(row, natureza_sugerida)
        pat = pattern_map.get(numero, {})
        alerta_classificacao = bool(natureza_sugerida) and conv.get("possivel_erro_natureza", False)
        alerta_envolvido_sem_cpf = bool(int(row.get("flag_envolvido_sem_cpf") or 0))
        alerta_envolvido_sem_rg = bool(int(row.get("flag_envolvido_sem_rg") or 0))
        alerta_envolvido_sem_email = bool(int(row.get("flag_envolvido_sem_email") or 0))
        alerta_envolvido_sem_telefone = bool(int(row.get("flag_envolvido_sem_telefone") or 0))
        alerta_campos_envolvido_incompletos = any(
            [
                alerta_envolvido_sem_cpf,
                alerta_envolvido_sem_rg,
                alerta_envolvido_sem_email,
                alerta_envolvido_sem_telefone,
            ]
        )
        nome_tipo_relatorio = str(row.get("nome_tipo_relatorio") or "").strip().upper()
        alerta_procedimento_relatorio_policial = bool(
            alerta_campos_envolvido_incompletos and "POLICIAL" in nome_tipo_relatorio
        )

        has_nonconformity = any(
            [
                text_eval["alerta_texto_curto"],
                text_eval["erro_ortografico"],
                text_eval["ausencia_pontuacao"],
                text_eval["historico_incompleto"],
                conv["alerta_convergencia"],
                bool(pat.get("alerta_padrao_operacional")),
                alerta_classificacao,
                alerta_campos_envolvido_incompletos,
                alerta_procedimento_relatorio_policial,
            ]
        )
        if has_nonconformity:
            inconformidades += 1

        recomendacoes = []
        if text_eval["alerta_texto_curto"]:
            recomendacoes.append("Detalhar melhor o historico da ocorrencia.")
        if conv["possivel_erro_natureza"]:
            recomendacoes.append("Revisar natureza da ocorrencia.")
        if pat.get("possivel_copia_historico"):
            recomendacoes.append("Verificar uso de texto padrao/copia.")
        if alerta_campos_envolvido_incompletos:
            recomendacoes.append("Revisar qualificacao de envolvidos (CPF, RG, email e telefone).")
        if alerta_procedimento_relatorio_policial:
            recomendacoes.append("Erro de procedimento: relatorio POLICIAL com envolvidos sem campos obrigatorios.")

        out_rows.append(
            {
                "numero_ocorrencia": numero,
                "cia_pm": str(row.get("cia_pm") or "").strip(),
                "pelotao_pm": str(row.get("pelotao_pm") or "").strip(),
                "data_hora_fato": row.get("data_hora_fato"),
                "data_fato": row.get("data_fato"),
                "natureza_codigo": row.get("natureza_codigo"),
                "natureza_descricao_longa": row.get("natureza_descricao_longa"),
                "historico_ocorrencia": row.get("historico_ocorrencia"),
                "score_textual": text_eval["score_textual"],
                "alerta_texto_curto": bool(text_eval["alerta_texto_curto"]),
                "erro_ortografico": bool(text_eval["erro_ortografico"]),
                "ausencia_pontuacao": bool(text_eval["ausencia_pontuacao"]),
                "historico_incompleto": bool(text_eval["historico_incompleto"]),
                "alerta_convergencia": bool(conv["alerta_convergencia"]),
                "alerta_padrao_operacional": bool(pat.get("alerta_padrao_operacional", False)),
                "alerta_classificacao": bool(alerta_classificacao),
                "alerta_c99": bool(pat.get("alerta_c99", False)),
                "alerta_w01": bool(pat.get("alerta_w01", False)),
                "alerta_w02": bool(pat.get("alerta_w02", False)),
                "alerta_w03": bool(pat.get("alerta_w03", False)),
                "alerta_w04": bool(pat.get("alerta_w04", False)),
                "possivel_copia_historico": bool(pat.get("possivel_copia_historico", False)),
                "repeticao_por_guarnicao": bool(pat.get("repeticao_por_guarnicao", False)),
                "tempo_empenho_suspeito": bool(conv["tempo_empenho_suspeito"]),
                "possivel_erro_natureza_violenta": bool(conv["possivel_erro_natureza_violenta"]),
                "alerta_envolvido_sem_cpf": bool(alerta_envolvido_sem_cpf),
                "alerta_envolvido_sem_rg": bool(alerta_envolvido_sem_rg),
                "alerta_envolvido_sem_email": bool(alerta_envolvido_sem_email),
                "alerta_envolvido_sem_telefone": bool(alerta_envolvido_sem_telefone),
                "alerta_campos_envolvido_incompletos": bool(alerta_campos_envolvido_incompletos),
                "alerta_procedimento_relatorio_policial": bool(alerta_procedimento_relatorio_policial),
                "natureza_sugerida": natureza_sugerida,
                "recomendacoes_json": json.dumps(recomendacoes, ensure_ascii=False),
            }
        )

    score_medio = round(sum(int(r["score_textual"]) for r in out_rows) / max(1, len(out_rows)), 2)
    return {
        "rows": out_rows,
        "summary": {
            "total_analisados": len(out_rows),
            "inconformidades": inconformidades,
            "score_medio": score_medio,
        },
    }


