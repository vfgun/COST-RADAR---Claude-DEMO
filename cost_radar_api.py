#!/usr/bin/env python3
"""Cost Radar - API a job per il sito (Lovable) sopra analizzatore_fatture_v5.

La logica di analisi resta in analizzatore_fatture_v5.py (non modificato). Qui si aggiunge solo lo strato web:

  POST   /api/analisi        multipart: f=<file> (uno o piu'), paese=<opz.>  ->  202 {"id": "...", "stato": "in_coda"}
  GET    /api/analisi/<id>   ->  {"stato": "in_coda"|"in_corso"|"completata"|"errore", "risultato": {...}, "secondi": N}
  DELETE /api/analisi/<id>   ->  cancella subito il risultato
  GET    /api/salute         ->  {"ok": true}

Sicurezza e privacy
  * Ogni chiamata richiede l'header  X-API-Key  uguale a COSTRADAR_API_KEY. La chiave la conosce solo la Edge Function
    di Supabase (server-to-server): MAI nel browser.
  * I file caricati restano SOLO in memoria: non vengono scritti su disco e sono scartati appena finisce l'analisi.
  * Il risultato resta in memoria RESULT_TTL secondi (default 30 min), poi viene cancellato.
  * Limiti: dimensione totale, numero file, analisi contemporanee (MAX_JOBS) e coda massima (MAX_QUEUE).

Avvio locale:   COSTRADAR_API_KEY=segreto ANTHROPIC_API_KEY=sk-ant-... python cost_radar_api.py
Produzione:     gunicorn -w 1 --threads 16 -t 0 cost_radar_api:app      (UN SOLO worker: i job stanno in memoria)
"""
from __future__ import annotations

import hmac
import io
import os
import secrets
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from flask import jsonify, request

import analizzatore_fatture_v5 as core

app = core.app

API_KEY = os.environ.get("COSTRADAR_API_KEY", "")
ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "")            # solo per prove dal browser; in produzione lascia vuoto
MAX_JOBS = int(os.environ.get("MAX_JOBS", "3"))                  # analisi eseguite in parallelo (costo e rate limit)
MAX_QUEUE = int(os.environ.get("MAX_QUEUE", "20"))               # oltre questa coda: 429
MAX_FILES = int(os.environ.get("MAX_FILES", "10"))
MAX_TOTAL_BYTES = int(os.environ.get("MAX_TOTAL_MB", "40")) * 2**20
RESULT_TTL = int(os.environ.get("RESULT_TTL", "1800"))           # secondi di permanenza del risultato in memoria
JOB_TIMEOUT = int(os.environ.get("JOB_TIMEOUT", "1500"))         # oltre: la lettura dello stato segnala errore
ALLOWED_EXT = {"pdf", "jpg", "jpeg", "png"}

app.config["MAX_CONTENT_LENGTH"] = MAX_TOTAL_BYTES + 2**20

JOBS: dict[str, dict] = {}
LOCK = threading.Lock()
POOL = ThreadPoolExecutor(MAX_JOBS)


class _MemFile:
    """File caricato tenuto in memoria; espone read() e filename come l'oggetto di Flask."""
    def __init__(self, name: str, data: bytes):
        self.filename, self._data = name, data

    def read(self) -> bytes:
        return self._data


def _auth_ok() -> bool:
    sent = request.headers.get("X-API-Key", "")
    return bool(API_KEY) and hmac.compare_digest(sent.encode(), API_KEY.encode())


@app.before_request
def _guard():
    if not request.path.startswith("/api/"):
        return None
    if request.method == "OPTIONS":
        return ("", 204)
    if request.path != "/api/salute" and not _auth_ok():
        return jsonify(errore="Non autorizzato."), 401
    return None


@app.after_request
def _cors(resp):
    if ALLOWED_ORIGIN and request.path.startswith("/api/"):
        resp.headers["Access-Control-Allow-Origin"] = ALLOWED_ORIGIN
        resp.headers["Access-Control-Allow-Headers"] = "X-API-Key, Content-Type"
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, DELETE, OPTIONS"
    resp.headers["Cache-Control"] = "no-store"
    return resp


def _cleanup() -> None:
    now = time.time()
    with LOCK:
        for jid in [j for j, v in JOBS.items() if v["fine"] and now - v["fine"] > RESULT_TTL]:
            del JOBS[jid]


def _run(jid: str, files: list[_MemFile], paese: str) -> None:
    job = JOBS.get(jid)
    if not job:
        return
    job["stato"], job["inizio"] = "in_corso", time.time()
    try:
        # riuso la logica esistente (analyze) in un contesto di richiesta finto: nessuna riscrittura del motore
        data = {"f": [(io.BytesIO(f.read()), f.filename) for f in files]}
        if paese:
            data["paese"] = paese
        with app.test_request_context("/analyze", method="POST", data=data, content_type="multipart/form-data"):
            rv = core.analyze()
            resp = app.make_response(rv)
            out = resp.get_json(silent=True) or {}
        if out.get("errore"):
            job.update(stato="errore", errore=out["errore"])
        else:
            job.update(stato="completata", risultato=out)
    except Exception as e:  # nessun dettaglio interno verso il sito
        app.logger.exception("job %s fallito", jid)
        job.update(stato="errore", errore=f"Analisi non riuscita ({type(e).__name__}). Riprova tra poco.")
    finally:
        job["fine"] = time.time()
        files.clear()  # scarta subito i file dalla memoria


@app.post("/api/analisi")
def crea_analisi():
    _cleanup()
    uploads = request.files.getlist("f")
    if not uploads:
        return jsonify(errore="Nessun file caricato."), 400
    if len(uploads) > MAX_FILES:
        return jsonify(errore=f"Massimo {MAX_FILES} file per analisi."), 400
    files, total = [], 0
    for u in uploads:
        name = u.filename or ""
        if name.rsplit(".", 1)[-1].lower() not in ALLOWED_EXT:
            return jsonify(errore="Formato non supportato: usa PDF, JPG o PNG."), 400
        data = u.read()
        total += len(data)
        if total > MAX_TOTAL_BYTES:
            return jsonify(errore=f"Dimensione totale oltre {MAX_TOTAL_BYTES // 2**20} MB."), 413
        files.append(_MemFile(name, data))
    with LOCK:
        waiting = sum(1 for v in JOBS.values() if v["stato"] in ("in_coda", "in_corso"))
        if waiting >= MAX_JOBS + MAX_QUEUE:
            return jsonify(errore="Servizio occupato: riprova tra qualche minuto."), 429
        jid = secrets.token_urlsafe(16)
        JOBS[jid] = {"stato": "in_coda", "creata": time.time(), "inizio": None, "fine": None,
                     "risultato": None, "errore": None}
    POOL.submit(_run, jid, files, (request.form.get("paese") or "").strip())
    return jsonify(id=jid, stato="in_coda"), 202


@app.get("/api/analisi/<jid>")
def stato_analisi(jid: str):
    _cleanup()
    job = JOBS.get(jid)
    if not job:
        return jsonify(errore="Analisi non trovata o scaduta."), 404
    now = time.time()
    if job["stato"] in ("in_coda", "in_corso") and now - job["creata"] > JOB_TIMEOUT:
        job.update(stato="errore", errore="Analisi troppo lunga: riprova con meno file.", fine=now)
    body = {"id": jid, "stato": job["stato"], "secondi": round((job["fine"] or now) - job["creata"])}
    if job["stato"] == "completata":
        body["risultato"] = job["risultato"]
    elif job["stato"] == "errore":
        body["errore"] = job["errore"]
    return jsonify(body)


@app.delete("/api/analisi/<jid>")
def cancella_analisi(jid: str):
    with LOCK:
        existed = JOBS.pop(jid, None) is not None
    return jsonify(cancellata=existed)


@app.get("/api/salute")
def salute():
    return jsonify(ok=True)


if __name__ == "__main__":
    if not API_KEY:
        raise SystemExit("Imposta COSTRADAR_API_KEY (segreto condiviso con la Edge Function di Supabase).")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=False, threaded=True)
