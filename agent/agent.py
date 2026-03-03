import os
import time
import math
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

from extractor_bisp import run_extraction

load_dotenv(Path(__file__).resolve().parent / '.env')


def _env(name: str, default: str = '') -> str:
    return os.getenv(name, default).strip()


API_URL = _env('AGENT_API_URL').rstrip('/')
TOKEN = _env('AGENT_TOKEN')
AGENT_ID = _env('AGENT_ID', 'pc-local')
AGENT_VERSION = _env('AGENT_VERSION', '0.1.0')
HEARTBEAT_SECONDS = int(_env('AGENT_HEARTBEAT_SECONDS', '30'))
POLL_SECONDS = int(_env('AGENT_POLL_SECONDS', '20'))

HEADERS = {'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'}


def _ranges_by_indicador(rows: list[dict[str, Any]]) -> dict[str, tuple[str, str]]:
    ranges: dict[str, list[str]] = {}
    for r in rows or []:
        ind = str(r.get('indicador') or '').strip()
        ref = str(r.get('referencia_data') or '').strip()
        if not ind or not ref:
            continue
        if ind not in ranges:
            ranges[ind] = [ref, ref]
        else:
            if ref < ranges[ind][0]:
                ranges[ind][0] = ref
            if ref > ranges[ind][1]:
                ranges[ind][1] = ref
    return {k: (v[0], v[1]) for k, v in ranges.items()}

def _log(msg: str) -> None:
    print(f"[agent] {msg}", flush=True)


def _sanitize_json_value(value: Any) -> Any:
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    if hasattr(value, "item"):
        try:
            return _sanitize_json_value(value.item())
        except Exception:
            pass
    if isinstance(value, dict):
        return {k: _sanitize_json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_json_value(v) for v in value]
    return value


def execute_job(job: dict[str, Any]) -> dict[str, Any]:
    payload = job.get('payload', {}) if isinstance(job, dict) else {}
    run_mode = payload.get('run_mode', 'all')
    scripts = payload.get('scripts', [])
    _log(f"job iniciado: run_mode={run_mode} scripts={scripts}")
    result = run_extraction(run_mode=run_mode, scripts=scripts)
    _log(
        "extracao concluida: "
        f"scripts={result.get('scripts_executed')} rows_total={result.get('rows_total')} "
        f"kpi_rows_total={result.get('kpi_rows_total')} reds_rows_total={result.get('reds_rows_total')} "
        f"output_dir={result.get('output_dir')}"
    )
    kpi_rows = result.pop('kpi_rows', [])
    reds_rows = result.pop('reds_rows', [])
    fact_rows_by_indicador = result.pop('fact_rows_by_indicador', {}) or {}
    purge_ranges = result.pop('purge_ranges', {})

    # purge stale rows for the same indicador/date range before ingest
    ranges = _ranges_by_indicador(kpi_rows)
    reds_ranges = _ranges_by_indicador(reds_rows)
    for ind, (dmin, dmax) in reds_ranges.items():
        if ind not in ranges:
            ranges[ind] = (dmin, dmax)
        else:
            cur_min, cur_max = ranges[ind]
            if dmin < cur_min:
                cur_min = dmin
            if dmax > cur_max:
                cur_max = dmax
            ranges[ind] = (cur_min, cur_max)

    # merge ranges from extractor (sql-derived)
    for ind, r in (purge_ranges or {}).items():
        dmin = r.get('date_from')
        dmax = r.get('date_to')
        if not dmin or not dmax:
            continue
        if ind not in ranges:
            ranges[ind] = (dmin, dmax)
        else:
            cur_min, cur_max = ranges[ind]
            if dmin < cur_min:
                cur_min = dmin
            if dmax > cur_max:
                cur_max = dmax
            ranges[ind] = (cur_min, cur_max)

    for ind, (dmin, dmax) in ranges.items():
        _log(f"purge: indicador={ind} range={dmin}..{dmax}")
        resp = post('/ingest/purge', {'indicador': ind, 'date_from': dmin, 'date_to': dmax})
        _log(
            "purge: "
            f"kpi_deleted={resp.get('kpi_deleted')} reds_deleted={resp.get('reds_deleted')} "
            f"fact_deleted={resp.get('fact_deleted')}"
        )

    if kpi_rows:
        _log(f"enviando kpi: {len(kpi_rows)} linhas")
        batch_size = 1000
        total_upserted = 0
        for i in range(0, len(kpi_rows), batch_size):
            chunk = kpi_rows[i:i + batch_size]
            ingest_resp = post('/ingest/kpi', {'rows': chunk})
            total_upserted += int(ingest_resp.get('upserted', 0))
        result['kpi_ingest'] = {'received': len(kpi_rows), 'upserted': total_upserted}
        _log(f"kpi ingest: received={len(kpi_rows)} upserted={total_upserted}")
    if reds_rows:
        _log(f"enviando reds: {len(reds_rows)} linhas")
        batch_size = 1000
        total_upserted = 0
        for i in range(0, len(reds_rows), batch_size):
            chunk = reds_rows[i:i + batch_size]
            ingest_resp = post('/ingest/reds', {'rows': chunk})
            total_upserted += int(ingest_resp.get('upserted', 0))
        result['reds_ingest'] = {'received': len(reds_rows), 'upserted': total_upserted}
        _log(f"reds ingest: received={len(reds_rows)} upserted={total_upserted}")

    if fact_rows_by_indicador:
        batch_size = 1000
        fact_ingest: dict[str, dict[str, int]] = {}
        fact_errors: dict[str, str] = {}
        for indicador, rows in fact_rows_by_indicador.items():
            if not rows:
                continue
            _log(f"enviando fact: indicador={indicador} linhas={len(rows)}")
            try:
                total_upserted = 0
                for i in range(0, len(rows), batch_size):
                    chunk = rows[i:i + batch_size]
                    ingest_resp = post('/ingest/fact', {'indicador': indicador, 'rows': chunk})
                    total_upserted += int(ingest_resp.get('upserted', 0))
                fact_ingest[indicador] = {'received': len(rows), 'upserted': total_upserted}
                _log(f"fact ingest: indicador={indicador} received={len(rows)} upserted={total_upserted}")
            except Exception as exc:
                fact_errors[indicador] = str(exc)
                _log(f"fact ingest falhou: indicador={indicador} erro={exc}")
        if fact_ingest:
            result['fact_ingest'] = fact_ingest
        if fact_errors:
            result['fact_errors'] = fact_errors
    result['agent_id'] = AGENT_ID
    return result

def post(path: str, data: dict[str, Any]) -> dict[str, Any]:
    payload = _sanitize_json_value(data)
    resp = requests.post(f'{API_URL}{path}', json=payload, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def get(path: str) -> dict[str, Any]:
    resp = requests.get(f'{API_URL}{path}', headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def send_heartbeat() -> None:
    post('/heartbeat', {'agent_id': AGENT_ID, 'version': AGENT_VERSION})


def run_loop() -> None:
    last_heartbeat = 0.0

    while True:
        now = time.time()

        if now - last_heartbeat >= HEARTBEAT_SECONDS:
            try:
                send_heartbeat()
                last_heartbeat = now
                print('[agent] heartbeat ok')
            except Exception as exc:
                print(f'[agent] heartbeat falhou: {exc}')

        try:
            next_job = get('/jobs/next').get('job')
            if next_job:
                job_id = int(next_job['id'])
                post(f'/jobs/{job_id}/start', {'agent_id': AGENT_ID, 'version': AGENT_VERSION})
                try:
                    result = execute_job(next_job)
                    post(f'/jobs/{job_id}/result', {'success': True, 'result': result})
                    print(f'[agent] job {job_id} finalizado com sucesso')
                except Exception as exc:
                    post(
                        f'/jobs/{job_id}/result',
                        {'success': False, 'result': {}, 'error': str(exc)},
                    )
                    print(f'[agent] job {job_id} falhou: {exc}')
        except Exception as exc:
            print(f'[agent] polling falhou: {exc}')

        time.sleep(POLL_SECONDS)


if __name__ == '__main__':
    if not API_URL or not TOKEN:
        raise SystemExit('Defina AGENT_API_URL e AGENT_TOKEN no .env')
    run_loop()


