#!/usr/bin/env python3
"""Analizzatore fatture fonia business - SOLO SIM voce, SIM dati, linea fissa e fonia fissa a consumo.

Carichi PDF/JPG/PNG di una fattura e ottieni TABELLE con: nuovi piani proposti,
risparmio annuo, rischi sulla qualita' del servizio e rating 1-10 per ogni utenza.

Novita' v5 - BOLLETTE LUCE E GAS (SENZA SUPPOSIZIONI)
  * Riconosce da solo bollette di luce, gas o luce+gas (anche vecchie, es. 2022).
  * Base di confronto = le condizioni della bolletta (prezzi/quota fissa, oppure la spesa di vendita del
    periodo) applicate ai consumi della bolletta: stesso comportamento, stessa necessita'.
  * Confronta con le offerte di OGGI (rilevate dal web una volta al giorno, tutti i fornitori) e calcola
    il risparmio su 12 mesi: separato (luce e gas dai fornitori piu' convenienti), insieme (stesso
    fornitore / offerta dual) e migliore combinazione. Se nessuna offerta e' piu' conveniente: pop-up.
  * Nessun dato stimato: un'offerta il cui prezzo di oggi non e' determinabile dalle fonti viene scartata.

Novita' v4 - FONIA FISSA A CONSUMO
  * Per le linee fisse (ISDN, analogiche, VoIP) con traffico a consumo legge: canali, chiamate, minuti e
    importi PER DESTINAZIONE (locale, nazionale, mobile, internazionale, numeri speciali).
  * Confronta con offerte VoIP/SIP trunk/fibra ricalcolando il costo: canone + canali aggiuntivi +
    minuti x tariffa di ogni destinazione (+ scatto alla risposta, arrotondamento al minuto).
  * Tabella dedicata "tariffe per destinazione" (oggi vs nuova offerta) e rating con controllo canali.

Novita' v3 - CACHE GIORNALIERA DELLE OFFERTE
  * Le offerte dei competitor vengono rilevate dal web UNA SOLA VOLTA AL GIORNO per paese
    (tutti gli operatori trovati, uno per uno) e salvate in ./cache_offerte/<paese>_<data>.json.
  * Tutte le fatture analizzate quel giorno riusano la cache: costo e tempo di ricerca = 0.
  * Per forzare un nuovo scraping nello stesso giorno: cancella il file della cache.

Setup:  pip install flask anthropic pypdf pillow
        export ANTHROPIC_API_KEY=sk-ant-...
        python analizzatore_fatture_v5.py        ->  http://localhost:5000
"""
from __future__ import annotations

import base64
import io
import json
import math
import re
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path

import anthropic
from flask import Flask, jsonify, request
from pypdf import PdfReader

try:
    from PIL import Image
except ImportError:
    Image = None

# ---------------------------------------------------------------- configurazione
FAST, SMART = "claude-haiku-4-5-20251001", "claude-sonnet-5-5"
CHUNK_CHARS, MAX_CHUNKS, WORKERS = 40_000, 80, 4    # lettura PDF a blocchi
SEARCHES_DISCOVER = 6                               # ricerche per elencare gli operatori del paese
SEARCHES_PER_OPERATOR = 4                           # ricerche per ogni operatore
SCRAPE_WORKERS = 3                                  # operatori analizzati in parallelo (attenzione ai rate limit)
CONSOLIDA_FONIA_FISSA = True                        # piu' linee fisse (ISDN/analogiche) -> un unico trunk VoIP
CAPACITA_CANALI = 1.0                               # quota dei canali attuali da mantenere (1.0 = tutti, 0.5 = meta')
MIN_RECOMMEND_RATING = 7                            # rating medio minimo per l'offerta consigliata
MIN_RATING = 6                                      # sotto questa soglia l'offerta non viene proposta
MAX_IMAGE_BYTES = 4_500_000                         # limite API 5 MB per immagine
CACHE_DIR = Path(__file__).with_name("cache_offerte")
KIND = {"voce": "mobile_voce", "dati": "mobile_dati", "fisso": "fisso"}
IMAGES = {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png"}

client = anthropic.Anthropic()                      # legge ANTHROPIC_API_KEY
app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 60 * 2**20

# ---------------------------------------------------------------- prompt
EXTRACT = """Estrai dati da una porzione di fattura di telefonia business (testo, foto o scansione, anche storta o
parziale: leggi con attenzione). Rispondi SOLO con JSON:
{"paese":str|null,"operatore":str|null,"valuta":"EUR"|"RON"|...|null,"cambio_eur":number|null,
"periodo":{"dal":"AAAA-MM-GG","al":"AAAA-MM-GG"}|null,
"linee":[{"numero":str,"tipo":"voce"|"dati"|"fisso"|"escluso","piano":str,"canone_periodo":number,
  "extra":[{"nome":str,"importo":number}],
  "uso":{"gb":number|null,"minuti":number|null,"sms":number|null,"roaming_gb":number|null},
  "canali":number|null,
  "traffico":[{"destinazione":"locale"|"nazionale"|"mobile"|"internazionale"|"speciali"|"altro",
               "chiamate":number,"minuti":number,"importo":number}]}],
"sconti":[{"nome":str,"importo":number,"numero":str|null,"piano":str|null}],
"cliente":{"residente":bool|null,"regione":str|null,"provincia":str|null,"pagamento_sdd":bool|null},
"forniture":[{"commodity":"luce"|"gas","id":str,"offerta":str|null,"tipo_prezzo":"fisso"|"indicizzato"|null,
  "periodo":{"dal":"AAAA-MM-GG","al":"AAAA-MM-GG"},
  "consumo":{"totale":number,"fasce":{"f1":number,"f2":number,"f3":number}|null},
  "consumo_annuo":{"totale":number|null,"fasce":{"f1":number,"f2":number,"f3":number}|null}|null,
  "spesa_vendita":number,"quota_fissa_mese":number|null,"prezzo":number|null,
  "prezzi_fascia":{"f1":number,"f2":number,"f3":number}|null,"potenza_kw":number|null}]}
REGOLE
- Importi IVA esclusa. "canone_periodo" = importo LORDO della linea, prima di qualunque sconto.
- Ogni riga di sconto va in "sconti". Compila "numero" o "piano" SOLO se quella riga cita esplicitamente la SIM/linea o il
  piano tariffario (mai dedurlo da categorie, importi o percentuali), altrimenti null. Usa l'importo della colonna "Sconto"
  del periodo fatturato, non il valore nominale di una promozione. Non elencare due volte lo stesso sconto.
- Ignora sconti su servizi esclusi. Usa "escluso" per IoT, M2M, cloud, ICT, rate di dispositivi e ogni servizio che non sia
  SIM voce, SIM dati standard o linea fissa.
- "uso" = TOTALI del periodo scritti in fattura (mai sommare singoli eventi). KB/1048576 o MB/1024 = GB; hh:mm:ss = minuti.
- LINEE FISSE (ISDN, analogiche, VoIP) CON TRAFFICO A CONSUMO: "canone_periodo" = solo abbonamenti/canoni/contributi
  (anche se addebitati in anticipo per un altro periodo). "traffico" = una riga per categoria di destinazione con i totali
  del periodo (chiamate, minuti da hh:mm:ss, importo IVA esclusa); mai i canoni qui. Mappatura: urbane/locali -> locale;
  interurbane/nazionali -> nazionale; verso mobili -> mobile; estero/internazionali (anche mobili internazionali) ->
  internazionale; servizi/numeri speciali -> speciali; categoria non chiara -> altro. "canali" = canali dell'accesso se
  scritto (ISDN base = 2, primario = 30), altrimenti null. "periodo" = periodo di traffico (es. "Settembre-Ottobre 2016"
  -> 2016-09-01 / 2016-10-31). Non includere spese di spedizione, indennita' di mora o altri addebiti non telefonici. Le linee analogiche
  ("telefonia di base", "linea telefonica") sono tipo "fisso". Se il traffico verso i fissi non compare perche' incluso in
  un'offerta flat/canone, NON inventarlo.
- "cambio_eur" = unita' di valuta fattura per 1 EUR, se scritto. Dato mancante = null. Non inventare.
- BOLLETTE LUCE/GAS: "linee" resta vuoto e si compila "forniture" (una per POD/PDR; in una bolletta luce+gas due).
  "id" = POD o PDR. "spesa_vendita" = SOLO la spesa per la materia energia/vendita del periodo (quota fissa + quota
  energia di vendita), IVA esclusa, senza trasporto, gestione contatore, oneri di sistema, accise, imposte, canone RAI,
  arrotondamenti. "consumo" = kWh (luce, con "fasce" F1/F2/F3 se presenti) o Smc (gas) FATTURATI nel periodo.
  "consumo_annuo" SOLO se la bolletta lo dichiara esplicitamente (mai calcolarlo); le sue "fasce" solo se dichiarate oppure
  come SOMMA ESATTA dei 12 mesi dello storico mensile per fascia che copre il periodo del consumo annuo dichiarato. "quota_fissa_mese", "prezzo" e
  "prezzi_fascia" = prezzi unitari di VENDITA IVA esclusa, solo se riportati in bolletta, altrimenti null.
  "cliente": residente/regione/provincia/pagamento_sdd (addebito diretto) solo se scritti; altrimenti null.
- Se non ci sono dati utili (solo riepilogo, testi legali, pubblicita'): "linee":[]."""

DISCOVER = """Elenca TUTTI gli operatori di telecomunicazioni del paese indicato che vendono a clienti business/partita IVA:
operatori di rete mobile, MVNO, fibra, linea fissa e fonia VoIP/SIP trunk (anche regionali o di nicchia). Usa registri del regolatore, comparatori
e articoli recenti; non fermarti ai primi risultati. Rispondi SOLO con JSON:
{"paese":str,"valuta":str (codice ISO della valuta locale),"cambio_eur":number|null (unita' di valuta locale per 1 EUR; 1 se EUR),
"operatori":[str]}"""

OFFERS_OP = """Cerca sul sito ufficiale (e, se serve, su comparatori) le offerte ATTUALI per clienti business/partita IVA
dell'operatore indicato nel paese indicato. Includi TUTTE le offerte attivabili oggi di questi tipi, max 8 per tipo:
mobile_voce (SIM voce+dati), mobile_dati (SIM solo dati), fisso (fibra/linea fissa), voce_fissa (fonia fissa business a
consumo: SIP trunk/VoIP/sostituti di ISDN e analogica). Ignora IoT, M2M, cloud, energia, dispositivi.
Prezzi IVA esclusa, nella valuta indicata. Per voce_fissa, e per ogni offerta "fisso" che include chiamate, compila
"tariffe" (euro al minuto per locale, nazionale, mobile, internazionale, speciali; 0 = incluso/illimitato; null = non
indicato; per internazionale usa i principali paesi europei e segnala nei rischi se varia molto), "scatto" (euro a
chiamata), "fatturazione" ("secondo"|"minuto"), "canone_canale" (euro/mese per canale), "canali_inclusi" (canali
compresi nel canone_mensile), "canali_max" (null = illimitati), "connettivita_inclusa" (bool). Non inventare: valore non
in fonte = null; illimitato = 99999; nessuna offerta trovata = lista vuota. Rispondi SOLO con JSON:
{"offerte":[{"rete":"propria"|"MVNO su <rete>"|null,"nome":str,
 "tipo":"mobile_voce"|"mobile_dati"|"fisso"|"voce_fissa","canone_mensile":number,"gb":number|null,
 "minuti":number|null,"sms":number|null,"roaming_gb":number|null,"extra_ue":bool|null,
 "tariffe":{"locale":number|null,"nazionale":number|null,"mobile":number|null,"internazionale":number|null,
            "speciali":number|null}|null,
 "scatto":number|null,"fatturazione":"secondo"|"minuto"|null,"canone_canale":number|null,"canali_inclusi":number|null,
 "canali_max":number|null,"connettivita_inclusa":bool|null,"attivazione":number|null,"vincolo_mesi":number|null,
 "rischi":[str],"fonte_url":str,"data":"AAAA-MM-GG"}]}"""

class NoOffers(RuntimeError):
    """Rilevamento offerte fallito o vuoto: non esiste nessuna offerta concreta da confrontare."""


ENERGY_DISCOVER = """Elenca TUTTI i fornitori di energia elettrica e gas naturale per clienti DOMESTICI del paese indicato
(mercato libero), compresi quelli regionali o di nicchia. Usa registri del regolatore (es. Portale Offerte ARERA),
comparatori e articoli recenti; non fermarti ai primi risultati. Rispondi SOLO con JSON:
{"paese":str,"valuta":str (codice ISO),"cambio_eur":number|null,"operatori":[str]}"""

ENERGY_OFFERS = """Cerca sul sito ufficiale dell'operatore (e, se serve, sul Portale Offerte ARERA o su comparatori) le
offerte ATTUALI per clienti DOMESTICI di energia elettrica (luce) e gas naturale attivabili oggi nel paese indicato
(mercato libero). Includi tutte le offerte, anche le dual (luce+gas insieme), max 8 per commodity. Tutti i prezzi sono
IVA esclusa e riferiti SOLO alla parte vendita (esclusi trasporto, oneri di sistema, accise, imposte).
- "prezzo" = prezzo energia EFFETTIVO DI OGGI in euro per kWh (luce) o per Smc (gas), come risulta dalle condizioni
  economiche. Per le offerte indicizzate usa il valore dell'indice del mese corrente + spread (le perdite di rete solo se la
  fonte le indica) e riporta in "indice" nome, valore usato e spread. Se dalle fonti non puoi determinare il prezzo
  effettivo di oggi metti null: l'offerta verra' scartata. NON STIMARE.
- Luce multioraria: "prezzi_fascia" f1/f2/f3; monoraria: "prezzo". "quota_fissa_anno" = costo fisso annuo del solo
  venditore (se mensile moltiplica per 12). "sconto_anno" = sconto/bonus garantito e incondizionato per anno (0 se nessuno).
- "requisiti": residente (true se riservata ai residenti, false se ai non residenti, null altrimenti), regioni (lista se
  limitata ad aree), sdd (true se richiesta la domiciliazione), solo_nuovi_clienti. Segnala nei rischi scadenze di sconti
  e rinnovi (es. quota fissa che sale dopo 12 mesi). Nessuna offerta trovata = lista vuota. Rispondi SOLO con JSON:
{"offerte":[{"nome":str,"commodity":"luce"|"gas"|"dual","tipo_prezzo":"fisso"|"indicizzato",
 "luce":{"prezzo":number|null,"prezzi_fascia":{"f1":number,"f2":number,"f3":number}|null,"quota_fissa_anno":number|null}|null,
 "gas":{"prezzo":number|null,"quota_fissa_anno":number|null}|null,
 "sconto_anno":number,"indice":{"nome":str,"valore":number,"spread":number}|null,"durata_prezzo_mesi":number|null,
 "vincolo_mesi":number|null,
 "requisiti":{"residente":bool|null,"regioni":[str]|null,"sdd":bool|null,"solo_nuovi_clienti":bool|null},
 "rischi":[str],"fonte_url":str,"data":"AAAA-MM-GG"}]}"""

SUMMARY = """Scrivi in italiano una sintesi (max 8 righe) per un responsabile acquisti. Se "esito.stato" non e' "ok" spiega con chiarezza perche' non c'e' un'offerta concreta con risparmio e NON
proporne una. Altrimenti parti da "raccomandazione" (l'offerta
consigliata) e spiega perche' e' in linea con il consumo rilevato; poi: piani proposti, risparmio annuo
(IVA esclusa), rischi sulla qualita' del servizio, rating medio. Usa SOLO i dati JSON forniti. Se ci sono
"sconti_non_attribuiti", "blocchi_non_letti" o "operatori_non_rilevati", segnalali come limite dell'analisi
(risparmio sovrastimato, dati o offerte incompleti). Per la fonia fissa a consumo cita canali, destinazioni principali e tariffe oggi vs nuove. Indica cosa va
verificato prima di firmare."""


# ---------------------------------------------------------------- chiamate al modello
def ask(model: str, system: str, user, tools=None, max_tokens: int = 4000) -> str:
    """Una richiesta al modello; gestisce i tool lato server (ricerca web) che possono mettere in pausa il turno."""
    messages = [{"role": "user", "content": user}]
    for _ in range(8):
        extra = {"tools": tools} if tools else {}
        r = client.messages.create(model=model, max_tokens=max_tokens, system=system, messages=messages, **extra)
        if r.stop_reason != "pause_turn":
            break
        messages.append({"role": "assistant", "content": r.content})
    return "".join(b.text for b in r.content if b.type == "text")


def parse_json(text: str) -> dict:
    return json.loads(text[text.find("{"): text.rfind("}") + 1])


def web_search(max_uses: int) -> list[dict]:
    return [{"type": "web_search_20250305", "name": "web_search", "max_uses": max_uses}]


def norm(x) -> str:
    return re.sub(r"\W+", "", str(x or "")).lower()


# ---------------------------------------------------------------- 1) lettura file
def split_pages(pages: list[str]) -> list[str]:
    blocks, cur = [], ""
    for page in pages:
        if cur and len(cur) + len(page) > CHUNK_CHARS:
            blocks.append(cur)
            cur = ""
        cur += page + "\n"
    return blocks + ([cur] if cur else [])


def image_block(raw: bytes, media_type: str) -> dict:
    if Image and len(raw) > MAX_IMAGE_BYTES:
        img = Image.open(io.BytesIO(raw)).convert("RGB")
        img.thumbnail((2400, 2400))
        buf = io.BytesIO()
        img.save(buf, "JPEG", quality=85)
        raw, media_type = buf.getvalue(), "image/jpeg"
    return {"type": "image", "source": {"type": "base64", "media_type": media_type,
                                        "data": base64.b64encode(raw).decode()}}


def file_tasks(f) -> list:
    """Trasforma un file caricato in 'compiti' di estrazione: testo (PDF) oppure blocchi immagine/documento."""
    raw, ext = f.read(), f.filename.lower().rsplit(".", 1)[-1]
    if ext in IMAGES:
        return [[image_block(raw, IMAGES[ext]), {"type": "text", "text": "Foto di una pagina di fattura."}]]
    if ext != "pdf":
        raise ValueError("formato non supportato (usa PDF, JPG o PNG)")
    pages = [p.extract_text() or "" for p in PdfReader(io.BytesIO(raw)).pages]
    if sum(map(len, pages)) >= 500:
        return split_pages(pages)[:MAX_CHUNKS]
    # PDF scansionato: lo invio intero (max 100 pagine / 32 MB per richiesta)
    return [[{"type": "document", "source": {"type": "base64", "media_type": "application/pdf",
                                             "data": base64.b64encode(raw).decode()}}]]


# ---------------------------------------------------------------- 2) estrazione e costi
def extract_chunk(content) -> dict | None:
    try:
        return parse_json(ask(FAST, EXTRACT, content, max_tokens=8000))
    except Exception:
        return None  # contato come "blocco non letto"


def num(x) -> float:
    return x if isinstance(x, (int, float)) else 0


def merge(parts: list[dict]):
    """Unisce i risultati dei blocchi: una voce per numero, sconti deduplicati."""
    lines, discounts, meta, seen = {}, [], {}, set()
    for p in parts:
        for k in ("paese", "operatore", "periodo", "valuta", "cambio_eur"):
            if p.get(k) and k not in meta:
                meta[k] = p[k]
        for s in p.get("sconti") or []:
            key = (s.get("nome"), s.get("importo"), s.get("numero"), s.get("piano"))
            if key not in seen:
                seen.add(key)
                discounts.append(s)
        for l in p.get("linee") or []:
            numero, tipo = l.get("numero"), l.get("tipo")
            if not numero or tipo not in KIND:
                continue
            d = lines.setdefault(numero, {"numero": numero, "tipo": tipo, "piano": l.get("piano"),
                                          "base": 0, "extra": {}, "uso": {}, "traffico": {}, "canali": None})
            d["base"] = max(d["base"], num(l.get("canone_periodo")))
            for e in l.get("extra") or []:
                d["extra"][e.get("nome")] = max(d["extra"].get(e.get("nome"), 0), num(e.get("importo")))
            for k, v in (l.get("uso") or {}).items():
                if isinstance(v, (int, float)):
                    d["uso"][k] = max(d["uso"].get(k, 0), v)  # max, non somma: evita doppi conteggi
            if isinstance(l.get("canali"), (int, float)):
                d["canali"] = max(d["canali"] or 0, l["canali"])
            sums = {}  # nello stesso blocco le categorie con la stessa destinazione si SOMMANO...
            for t in l.get("traffico") or []:
                acc = sums.setdefault(t.get("destinazione") or "altro", {"chiamate": 0, "minuti": 0, "importo": 0})
                for k in acc:
                    acc[k] += num(t.get(k))
            for dest, acc in sums.items():  # ...tra blocchi diversi si prende il MAX (evita doppi conteggi)
                cur = d["traffico"].setdefault(dest, {"chiamate": 0, "minuti": 0, "importo": 0})
                for k in cur:
                    cur[k] = max(cur[k], acc[k])
    return meta, list(lines.values()), discounts


def months_in(meta: dict) -> int:
    try:
        a, b = (date.fromisoformat(meta["periodo"][k]) for k in ("dal", "al"))
        return max(1, round(((b - a).days + 1) / 30.4))
    except Exception:
        return 1


def apply_discounts(lines: list[dict], discounts: list[dict], m: int) -> list[dict]:
    """Uno sconto va a una utenza SOLO se cita esplicitamente il suo numero o il suo piano.
    Gli altri NON vengono spalmati: restano elencati come 'non attribuiti'."""
    for l in lines:
        l["m"] = m
        l["lordo"] = (l["base"] + sum(l["extra"].values()) + sum(t["importo"] for t in l["traffico"].values())) / m
        l["sconto"] = 0.0
    unassigned = []
    for s in discounts:
        amount, n, p = abs(num(s.get("importo"))) / m, norm(s.get("numero")), norm(s.get("piano"))
        if n:
            targets = [l for l in lines if norm(l["numero"]) == n]
        elif p:
            targets = [l for l in lines if norm(l["piano"]) and (p in norm(l["piano"]) or norm(l["piano"]) in p)]
        else:
            targets = []
        if targets:
            total = sum(l["lordo"] for l in targets) or 1
            for l in targets:
                l["sconto"] += amount * l["lordo"] / total
        else:
            unassigned.append({"nome": s.get("nome"), "importo_anno": round(amount * 12, 2)})
    for l in lines:
        l["costo"] = max(0.0, l["lordo"] - l["sconto"])  # costo mensile IVA esclusa
    return unassigned


# ---------------------------------------------------------------- 3) offerte dei competitor (CACHE GIORNALIERA)
_locks: dict[str, threading.Lock] = defaultdict(threading.Lock)
_locks_guard = threading.Lock()


def _lock_for(key: str) -> threading.Lock:
    with _locks_guard:
        return _locks[key]


def scrape_operator(country: str, operator: str, currency: str) -> tuple[list[dict], bool]:
    """Offerte di UN operatore. Ritorna (offerte, ok). Un errore non blocca gli altri operatori."""
    try:
        r = parse_json(ask(SMART, OFFERS_OP,
                           json.dumps({"paese": country, "operatore": operator, "valuta": currency}, ensure_ascii=False),
                           tools=web_search(SEARCHES_PER_OPERATOR), max_tokens=6000))
        offers = [dict(o, operatore=operator) for o in r.get("offerte", [])
                  if o.get("tipo") in (*KIND.values(), "voce_fissa") and isinstance(o.get("canone_mensile"), (int, float))]
        return offers, True
    except Exception:
        return [], False


def scrape_country(country: str, current_operator: str | None) -> dict:
    """UNICO scraping giornaliero: 1) elenca tutti gli operatori del paese  2) un'analisi per ogni operatore."""
    found = parse_json(ask(SMART, DISCOVER, json.dumps({"paese": country}, ensure_ascii=False),
                           tools=web_search(SEARCHES_DISCOVER), max_tokens=3000))
    operators = list(dict.fromkeys([*(found.get("operatori") or []), *([current_operator] if current_operator else [])]))
    currency = found.get("valuta") or "EUR"
    with ThreadPoolExecutor(SCRAPE_WORKERS) as pool:
        results = list(pool.map(lambda op: scrape_operator(country, op, currency), operators))
    offers = [o for res, _ in results for o in res]
    if not offers:
        raise NoOffers(f"Non sono riuscito a rilevare offerte per {country}: non posso indicare un risparmio.")
    return {"data": date.today().isoformat(), "paese": country, "valuta": currency,
            "cambio_eur": found.get("cambio_eur"), "operatori": operators, "offerte": offers,
            "non_rilevati": [op for op, (_, ok) in zip(operators, results) if not ok]}


def get_offers(country: str, current_operator: str | None, domain: str = "telecom") -> tuple[dict, bool]:
    """Offerte del giorno per il paese: da cache se gia' rilevate oggi, altrimenti (una volta) dal web."""
    CACHE_DIR.mkdir(exist_ok=True)
    suffix = "v4" if domain == "telecom" else "energia_v5"  # nome file cache: paese_data_dominio
    scrape_all, scrape_one = (scrape_country, scrape_operator) if domain == "telecom" else (scrape_energy_country, scrape_energy_operator)
    key = f"{norm(country)}_{date.today().isoformat()}_{suffix}"
    path = CACHE_DIR / f"{key}.json"
    with _lock_for(key):  # evita due scraping simultanei dello stesso paese
        if not path.exists():
            data = scrape_all(country, current_operator)
            path.write_text(json.dumps(data, ensure_ascii=False, indent=1), "utf-8")
            return data, False
        data = json.loads(path.read_text("utf-8"))
        # operatore della fattura mai visto oggi: si rileva solo lui e si aggiunge alla cache
        if current_operator and norm(current_operator) not in {norm(o) for o in data["operatori"]}:
            offers, ok = scrape_one(country, current_operator, data["valuta"])
            data["operatori"].append(current_operator)
            data["offerte"] += offers
            if not ok:
                data["non_rilevati"].append(current_operator)
            path.write_text(json.dumps(data, ensure_ascii=False, indent=1), "utf-8")
        return data, True


def convert_offers(cache: dict, currency: str, invoice_rate) -> list[dict]:
    """Porta i prezzi delle offerte nella valuta della fattura."""
    src, offers = cache.get("valuta") or currency, cache["offerte"]
    if src == currency:
        return offers
    if currency == "EUR" and cache.get("cambio_eur"):
        factor = 1 / cache["cambio_eur"]
    elif src == "EUR" and invoice_rate:
        factor = invoice_rate
    else:
        raise ValueError(f"Fattura in {currency} e offerte in {src}: cambio non disponibile, importi non confrontabili.")
    return [dict(o, canone_mensile=o["canone_mensile"] * factor,
                 attivazione=None if o.get("attivazione") is None else o["attivazione"] * factor) for o in offers]


# ---------------------------------------------------------------- 4) rating e scenari
def rate(line: dict, offer: dict) -> tuple[int, list[str]]:
    """Rating 1-10: quanto l'offerta copre il comportamento attuale della linea."""
    score, notes = 10, []
    use = {k: v / line["m"] for k, v in line["uso"].items()}  # consumo mensile

    def check(label, used, allowed):
        nonlocal score
        if not used:
            return
        if allowed is None:
            score -= 1
            notes.append(f"{label} non verificabili")
        elif used > allowed:
            score -= 4
            notes.append(f"{label} insufficienti ({used:.0f} vs {allowed:.0f})")
        elif used > 0.8 * allowed:
            score -= 2
            notes.append(f"{label} al limite")

    if line["tipo"] != "fisso":
        check("GB", use.get("gb"), offer.get("gb"))
        check("GB roaming", use.get("roaming_gb"), offer.get("roaming_gb"))
        if line["tipo"] == "voce":
            check("minuti", use.get("minuti"), offer.get("minuti"))
            check("SMS", use.get("sms"), offer.get("sms"))
        if line["extra"] and not offer.get("extra_ue"):
            score -= 2
            notes.append("extra/roaming attuali non coperti")
    if (offer.get("vincolo_mesi") or 0) > 0:
        score -= 1
        notes.append(f"vincolo {offer['vincolo_mesi']} mesi")
    return max(1, score), notes


# --- fonia fissa a consumo: canali, minuti e tariffe per destinazione
FALLBACK = {"locale": "nazionale", "nazionale": "locale", "altro": "nazionale"}


VOICE_PLAN = re.compile(r"isdn|analog|\brtg\b|telefonia di base|linea telefonica", re.I)


def is_voice_line(line: dict) -> bool:
    """Linea fissa di fonia (traffico a consumo, ISDN o analogica): si confronta per canali, minuti e tariffe."""
    return line["tipo"] == "fisso" and (bool(line.get("traffico")) or bool(VOICE_PLAN.search(str(line.get("piano") or ""))))


def consolidate_voice_lines(lines: list[dict]) -> list[dict]:
    """Piu' linee fisse di fonia diventano UN trunk VoIP: traffico, costi e canali si sommano (da fare dopo gli sconti)."""
    voice = [l for l in lines if is_voice_line(l)]
    if len(voice) < 2:
        return lines
    channels = lambda l: int(l["canali"]) if l.get("canali") else (2 if "isdn" in norm(l["piano"]) else 1)
    big = {"numero": ", ".join(l["numero"] for l in voice), "tipo": "fisso", "base": 0, "extra": {}, "uso": {},
           "traffico": {}, "m": voice[0]["m"], "lordo": 0.0, "sconto": 0.0, "costo": 0.0}
    plans: dict = {}
    for l in voice:
        plans[l["piano"]] = plans.get(l["piano"], 0) + 1
        for k in ("base", "lordo", "sconto", "costo"):
            big[k] += l[k]
        for name, v in l["extra"].items():
            big["extra"][name] = big["extra"].get(name, 0) + v
        for dest, t in l["traffico"].items():
            acc = big["traffico"].setdefault(dest, {"chiamate": 0, "minuti": 0, "importo": 0})
            for k in acc:
                acc[k] += t[k]
    big["piano"] = "; ".join(f"{p} x{n}" for p, n in plans.items())
    big["canali"] = max(2, math.ceil(sum(channels(l) for l in voice) * CAPACITA_CANALI))
    return [l for l in lines if not is_voice_line(l)] + [big]


def fits(line: dict, offer: dict) -> bool:
    if is_voice_line(line):
        return offer.get("tipo") in ("voce_fissa", "fisso") and isinstance(offer.get("tariffe"), dict)
    return offer.get("tipo") == KIND[line["tipo"]] and offer.get("canone_mensile") is not None


def rate_for(tariffe: dict | None, dest: str):
    """Tariffa al minuto per una destinazione (None = non indicata nell'offerta)."""
    t = tariffe or {}
    r = t.get(dest)
    return r if r is not None else t.get(FALLBACK.get(dest, ""))


def channels_needed(line: dict, minutes_month: float) -> int:
    """Canali dichiarati in fattura; altrimenti stima: ora di punta = 3x il carico medio (22 gg x 8 h), minimo 2 (ISDN) o 1 (analogica)."""
    if line.get("canali"):
        return int(line["canali"])
    floor = 2 if "isdn" in norm(line["piano"]) else 1
    return max(floor, math.ceil(3 * minutes_month / (22 * 480)))


def dest_row(dest: str, t: dict, m: int, new_rate=None, new_cost=None) -> dict:
    return {"destinazione": dest, "chiamate_mese": round(t["chiamate"] / m, 1), "minuti_mese": round(t["minuti"] / m, 1),
            "costo_oggi_mese": round(t["importo"] / m, 2),
            "tariffa_oggi": round(t["importo"] / t["minuti"], 4) if t["minuti"] else None,
            "tariffa_nuova": new_rate, "costo_nuovo_mese": None if new_cost is None else round(new_cost, 2)}


def voice_cost(line: dict, offer: dict) -> dict:
    """Costo mensile di una linea fissa con l'offerta: canone + canali extra + minuti x tariffa per destinazione."""
    m = line["m"]
    known = [v for v in (offer.get("tariffe") or {}).values() if isinstance(v, (int, float))]
    worst, per_minute, scatto = max(known, default=0), offer.get("fatturazione") == "minuto", offer.get("scatto") or 0
    rows, traffic, penalty, notes = [], 0.0, 0, []
    for dest, t in line["traffico"].items():
        mins, calls = t["minuti"] / m, t["chiamate"] / m
        rate = rate_for(offer.get("tariffe"), dest)
        if rate is None:  # tariffa non pubblicata: prudenza = nessun miglioramento (tariffa attuale, o la piu' alta nota)
            rate = t["importo"] / t["minuti"] if t["minuti"] else worst
            penalty += 1
            notes.append(f"tariffa {dest} non indicata (assunta pari a quella attuale)")
        cost = (mins + (0.5 * calls if per_minute else 0)) * rate + calls * scatto
        traffic += cost
        rows.append(dest_row(dest, t, m, rate, cost))
    if not any(d in line["traffico"] for d in ("locale", "nazionale")):
        if max(rate_for(offer.get("tariffe"), "nazionale") or 0, rate_for(offer.get("tariffe"), "locale") or 0) > 0:
            notes.append("chiamate verso fissi non rilevabili in fattura (flat?): il costo reale puo' essere maggiore")
    channels = channels_needed(line, sum(t["minuti"] for t in line["traffico"].values()) / m)
    price, included = offer.get("canone_canale"), offer.get("canali_inclusi")
    if included is None:
        included = 1 if price else channels
    fixed = (offer.get("canone_mensile") or 0) + max(0, channels - included) * (price or 0)
    if offer.get("canali_max") is not None and channels > offer["canali_max"]:
        penalty += 4
        notes.append(f"canali insufficienti ({channels} richiesti vs {offer['canali_max']})")
    if offer.get("connettivita_inclusa") is False:
        penalty += 1
        notes.append("richiede connettivita' dati separata (non inclusa)")
    if "isdn" in norm(line["piano"]):
        penalty += 1
        notes.append("migrazione ISDN->VoIP: verificare fax, POS, allarmi e centralino")
    if (offer.get("vincolo_mesi") or 0) > 0:
        penalty += 1
        notes.append(f"vincolo {offer['vincolo_mesi']} mesi")
    return {"monthly": fixed + traffic, "fixed": fixed, "traffic": traffic, "rows": rows, "channels": channels,
            "score": max(1, 10 - penalty), "notes": notes}


def best_offer(line: dict, pool: list[dict]):
    """Offerta piu' economica (attivazione ammortizzata su 12 mesi) con rating >= MIN_RATING."""
    candidates = []
    for o in pool:
        if not fits(line, o):
            continue
        if is_voice_line(line):
            info = voice_cost(line, o)
            r, notes, monthly = info["score"], info["notes"], info["monthly"]
        else:
            info, monthly = None, o["canone_mensile"]
            r, notes = rate(line, o)
        if r >= MIN_RATING:
            candidates.append((monthly + (o.get("attivazione") or 0) / 12, o, r, notes, monthly, info))
    return min(candidates, key=lambda c: c[0], default=None)


def build_scenario(lines: list[dict], pool: list[dict], name: str) -> dict:
    rows, new_total, covered, risks = [], 0.0, 0, set()
    for l in lines:
        current, voice = l["costo"] * 12, is_voice_line(l)
        minutes = sum(t["minuti"] for t in l["traffico"].values()) / l["m"] if voice else None
        row = {"numero": l["numero"], "tipo": l["tipo"], "piano_attuale": l["piano"],
               "costo_attuale_anno": round(current, 2), "operatore": None, "piano_nuovo": None,
               "costo_nuovo_anno": None, "risparmio_anno": None, "rating": None, "fonte": None,
               "canali": channels_needed(l, minutes) if voice else None,
               "minuti_mese": round(minutes, 1) if voice else None,
               "destinazioni": [dest_row(d, t, l["m"]) for d, t in l["traffico"].items()] if voice else [],
               "nuovo_dettaglio": None, "rischi": "Nessuna offerta adeguata: resta com'e'"}
        pick = best_offer(l, pool)
        if pick:
            _, o, r, notes, monthly, info = pick
            new = monthly * 12 + (o.get("attivazione") or 0)
            line_risks = list(o.get("rischi") or []) + notes
            if o.get("rete"):
                line_risks.append(f"rete {o['rete']}")
            row.update(operatore=o["operatore"], piano_nuovo=o["nome"], costo_nuovo_anno=round(new, 2),
                       risparmio_anno=round(current - new, 2), rating=r, fonte=o.get("fonte_url"),
                       rischi="; ".join(dict.fromkeys(line_risks)) or "Nessun rischio segnalato")
            if info:
                row.update(destinazioni=info["rows"], canali=info["channels"],
                           nuovo_dettaglio=f"canone+canali {info['fixed']:.2f} + traffico {info['traffic']:.2f} al mese")
            new_total += new
            covered += 1
            risks.update(f"{o['operatore']}: {x}" for x in line_risks)
        else:
            new_total += current
        rows.append(row)
    current_total = sum(r["costo_attuale_anno"] for r in rows)
    rated = [r["rating"] for r in rows if r["rating"]]
    return {"nome": name, "completo": covered == len(lines), "coperte": covered,
            "costo_anno": round(new_total, 2), "risparmio_anno": round(current_total - new_total, 2),
            "rating_medio": round(sum(rated) / len(rated), 1) if rated else None,
            "rischi": sorted(risks), "linee": rows, "piani": group_plans(rows)}


def group_plans(rows: list[dict]) -> list[dict]:
    """Tabella 'nuovi piani': una riga per (operatore, piano) con totale utenze e risparmio."""
    groups: dict = {}
    for r in rows:
        if r["operatore"]:
            groups.setdefault((r["operatore"], r["piano_nuovo"], r["fonte"]), []).append(r)
    return [{"operatore": op, "piano": plan, "fonte": src, "utenze": len(rs),
             "costo_nuovo_anno": round(sum(r["costo_nuovo_anno"] for r in rs), 2),
             "risparmio_anno": round(sum(r["risparmio_anno"] for r in rs), 2),
             "rating_medio": round(sum(r["rating"] for r in rs) / len(rs), 1)}
            for (op, plan, src), rs in sorted(groups.items(), key=lambda kv: -sum(r["risparmio_anno"] for r in kv[1]))]


# ---------------------------------------------------------------- offerta consigliata
def usage_summary(lines: list[dict]) -> str:
    """Comportamento rilevato in fattura, in una riga leggibile."""
    parts = []
    for l in (x for x in lines if is_voice_line(x)):
        mins = {d: t["minuti"] / l["m"] for d, t in l["traffico"].items()}
        top = ", ".join(f"{d} {v:,.0f}" for d, v in sorted(mins.items(), key=lambda kv: -kv[1])[:3])
        parts.append(f"linea fissa {l['numero']}: {sum(mins.values()):,.0f} min/mese" + (f" ({top})" if top else "") + ", "
                     f"{channels_needed(l, sum(mins.values()))} canali")
    for tipo, label in (("voce", "SIM voce"), ("dati", "SIM dati")):
        group = [l for l in lines if l["tipo"] == tipo]
        if group:
            avg = lambda k: sum(l["uso"].get(k, 0) / l["m"] for l in group) / len(group)
            parts.append(f"{len(group)} {label} (media {avg('gb'):.1f} GB"
                         + (f", {avg('minuti'):,.0f} min" if tipo == "voce" else "") + " al mese)")
    return "; ".join(parts)


def recommend(scenarios: list[dict], lines: list[dict], current_year: float) -> dict | None:
    """Sceglie UNA proposta: scenario con un solo operatore (o mix) piu' conveniente tra quelli che coprono
    tutte le utenze con rating medio >= MIN_RECOMMEND_RATING. None = conviene restare dove si e'."""
    def good(sc):
        return sc["completo"] and (sc["rating_medio"] or 0) >= MIN_RECOMMEND_RATING and sc["risparmio_anno"] > 0
    singles = [sc for sc in scenarios[:-1] if good(sc)]
    note = ""
    if singles:
        best = max(singles, key=lambda sc: sc["risparmio_anno"])
    elif good(scenarios[-1]):
        best, note = scenarios[-1], "Nessun singolo operatore copre tutte le utenze: la proposta richiede piu' contratti."
    else:
        return None
    return {"nome": best["nome"], "costo_anno": best["costo_anno"], "costo_attuale_anno": round(current_year, 2),
            "risparmio_anno": best["risparmio_anno"], "rating_medio": best["rating_medio"], "rischi": best["rischi"],
            "piani": best["piani"], "consumo": usage_summary(lines), "nota": note}


# ---------------------------------------------------------------- esito: messaggio quando NON c'e' un'offerta concreta
STEPS = ["Rinegozia con l'operatore attuale usando i prezzi dei concorrenti come leva.",
         "Chiedi un preventivo diretto a 2-3 operatori business: le offerte su misura non sono pubbliche.",
         "Riprova con una fattura piu' recente o completa (tutte le pagine)."]


def money_it(x: float, currency: str) -> str:
    return f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") + f" {currency}"


def build_outcome(rec, scenarios: list[dict], offers: list[dict], currency: str, country: str) -> dict:
    """{'stato': 'ok'} se c'e' una proposta; altrimenti titolo/messaggio/dettagli/passi per il pop-up del sito."""
    if rec:
        return {"stato": "ok"}
    m = lambda x: money_it(x, currency)

    def outcome(stato, titolo, messaggio, dettagli=(), passi=STEPS):
        return {"stato": stato, "titolo": titolo, "messaggio": messaggio, "dettagli": list(dettagli), "passi": list(passi)}

    candidates = [sc for sc in scenarios if sc["coperte"] > 0]
    if not offers:
        return outcome("nessuna_offerta", "Nessuna offerta concreta trovata",
                       f"Non ho trovato offerte attuali per {country}: non posso indicare un risparmio.",
                       passi=["Riprova domani (le offerte si rilevano una volta al giorno) o cancella il file del giorno "
                              "in cache_offerte/ per forzare un nuovo rilevamento.", *STEPS])
    if not candidates:
        return outcome("nessuna_adeguata", "Nessuna offerta adatta al tuo consumo",
                       "Ho trovato offerte, ma nessuna e' adeguata al comportamento rilevato in fattura (minuti, GB, canali "
                       f"o roaming insufficienti: rating sotto {MIN_RATING}/10). Non posso indicare un risparmio.")
    best = max(candidates, key=lambda sc: sc["risparmio_anno"])
    if best["risparmio_anno"] <= 0:
        return outcome("non_conveniente", "Nessun risparmio possibile",
                       f"Le offerte trovate non fanno risparmiare: la migliore ({best['nome']}) costa "
                       f"{m(-best['risparmio_anno'])} in piu' all'anno rispetto a oggi.",
                       dettagli=[f"Costo con l'alternativa migliore: {m(best['costo_anno'])}/anno"])
    if not best["completo"]:
        missing = [r["numero"] for r in best["linee"] if not r["operatore"]]
        return outcome("copertura_parziale", "Offerta solo per alcune utenze",
                       f"Ho trovato un'offerta concreta solo per {best['coperte']} {'utenza' if best['coperte'] == 1 else 'utenze'} su {len(best['linee'])} "
                       f"(risparmio parziale: {m(best['risparmio_anno'])}/anno). Non posso proporre una soluzione completa.",
                       dettagli=["Utenze senza offerta adeguata: " + ", ".join(missing[:8]) + ("..." if len(missing) > 8 else "")])
    return outcome("rating_basso", "Offerte poco adatte al tuo consumo",
                   f"Ho trovato offerte piu' economiche (migliore: {best['nome']}, risparmio {m(best['risparmio_anno'])}/anno) "
                   f"ma il rating medio e' {best['rating_medio']}/10, sotto la soglia di {MIN_RECOMMEND_RATING}: non le consiglio.",
                   dettagli=best["rischi"][:5])


# ---------------------------------------------------------------- ENERGIA: bollette luce e gas
def scrape_energy_operator(country: str, operator: str, currency: str) -> tuple[list[dict], bool]:
    """Offerte luce/gas/dual di UN fornitore (prezzi effettivi di oggi, solo parte vendita)."""
    try:
        r = parse_json(ask(SMART, ENERGY_OFFERS,
                           json.dumps({"paese": country, "operatore": operator, "valuta": currency}, ensure_ascii=False),
                           tools=web_search(SEARCHES_PER_OPERATOR), max_tokens=7000))
        offers = [dict(o, operatore=operator) for o in r.get("offerte", [])
                  if o.get("commodity") in ("luce", "gas", "dual") and (o.get("luce") or o.get("gas"))]
        return offers, True
    except Exception:
        return [], False


def scrape_energy_country(country: str, current_operator: str | None) -> dict:
    """UNICO scraping giornaliero delle offerte energia: elenca i fornitori, poi un'analisi per ciascuno."""
    found = parse_json(ask(SMART, ENERGY_DISCOVER, json.dumps({"paese": country}, ensure_ascii=False),
                           tools=web_search(SEARCHES_DISCOVER), max_tokens=3000))
    operators = list(dict.fromkeys([*(found.get("operatori") or []), *([current_operator] if current_operator else [])]))
    currency = found.get("valuta") or "EUR"
    with ThreadPoolExecutor(SCRAPE_WORKERS) as pool:
        results = list(pool.map(lambda op: scrape_energy_operator(country, op, currency), operators))
    offers = [o for res, _ in results for o in res]
    if not offers:
        raise NoOffers(f"Non sono riuscito a rilevare offerte luce/gas per {country}: non posso indicare un risparmio.")
    return {"data": date.today().isoformat(), "paese": country, "valuta": currency,
            "cambio_eur": found.get("cambio_eur"), "operatori": operators, "offerte": offers,
            "non_rilevati": [op for op, (_, ok) in zip(operators, results) if not ok]}


def merge_energy(parts: list[dict]):
    """Unisce le forniture lette dai blocchi (una per POD/PDR) e i dati del cliente."""
    forn, cliente = {}, {}
    for p in parts:
        for k, v in (p.get("cliente") or {}).items():
            if v is not None and k not in cliente:
                cliente[k] = v
        for f in p.get("forniture") or []:
            key = f.get("id")
            if not key or f.get("commodity") not in ("luce", "gas"):
                continue
            d = forn.setdefault(key, {"id": key, "commodity": f["commodity"], "offerta": None, "tipo_prezzo": None,
                                      "periodo": None, "consumo": {"totale": 0, "fasce": None}, "consumo_annuo": None,
                                      "spesa_vendita": 0, "quota_fissa_mese": None, "prezzo": None,
                                      "prezzi_fascia": None, "potenza_kw": None})
            for k in ("offerta", "tipo_prezzo", "periodo", "quota_fissa_mese", "prezzo", "prezzi_fascia", "potenza_kw"):
                if d[k] is None and f.get(k) is not None:
                    d[k] = f[k]
            c = f.get("consumo") or {}
            d["consumo"]["totale"] = max(d["consumo"]["totale"], num(c.get("totale")))
            if c.get("fasce") and not d["consumo"]["fasce"]:
                d["consumo"]["fasce"] = c["fasce"]
            ca = f.get("consumo_annuo")
            if ca and ca.get("totale") and not d["consumo_annuo"]:
                d["consumo_annuo"] = ca
            d["spesa_vendita"] = max(d["spesa_vendita"], num(f.get("spesa_vendita")))
    return list(forn.values()), cliente


BANDS = ("f1", "f2", "f3")


def days_in(periodo) -> int | None:
    try:
        a, b = (date.fromisoformat(periodo[k]) for k in ("dal", "al"))
        return (b - a).days + 1
    except Exception:
        return None


def scale_cons(c: dict, k: float) -> dict:
    return {"totale": c["totale"] * k, "fasce": {b: v * k for b, v in c["fasce"].items()} if c.get("fasce") else None}


def part_cost(cons: dict, part) -> float | None:
    """Costo annuo di vendita di una commodity: quota fissa + prezzo x consumo. None = non calcolabile (mai stimato)."""
    if not isinstance(part, dict) or not isinstance(part.get("quota_fissa_anno"), (int, float)):
        return None
    pf, p, fasce = part.get("prezzi_fascia"), part.get("prezzo"), cons.get("fasce")
    if isinstance(pf, dict) and fasce and all(isinstance(pf.get(b), (int, float)) and b in fasce for b in BANDS):
        energy = sum(pf[b] * fasce[b] for b in BANDS)
    elif isinstance(p, (int, float)):
        energy = p * cons["totale"]
    else:
        return None
    return part["quota_fissa_anno"] + energy


def energy_base(f: dict) -> dict | None:
    """Costo annuo di vendita CON LE CONDIZIONI DELLA BOLLETTA, a parita' di consumi. Nessuna stima oltre alla
    proiezione del periodo fatturato a 12 mesi (365/giorni) quando la bolletta non dichiara il consumo annuo."""
    days = days_in(f.get("periodo"))
    if not days or f["consumo"]["totale"] <= 0:
        return None
    k = 365 / days
    ann = f.get("consumo_annuo")
    if ann and ann.get("fasce") and ann.get("totale") and abs(sum(ann["fasce"].values()) - ann["totale"]) > 0.01 * ann["totale"]:
        ann = {"totale": ann["totale"], "fasce": None}  # fasce incoerenti con il totale dichiarato: non usate
    fee, price, bands = f.get("quota_fissa_mese"), f.get("prezzo"), f.get("prezzi_fascia")
    if fee is not None and (isinstance(price, (int, float)) or isinstance(bands, dict)):
        cons = {"totale": ann["totale"], "fasce": ann.get("fasce")} if ann and ann.get("totale") else scale_cons(f["consumo"], k)
        cost = part_cost(cons, {"quota_fissa_anno": fee * 12, "prezzo": price, "prezzi_fascia": bands})
        if cost is not None:
            how = "consumo annuo dichiarato in bolletta" if ann and ann.get("totale") else "consumo del periodo proiettato a 12 mesi"
            return {"cons": cons, "costo": cost, "metodo": f"quota fissa e prezzi della bolletta x {how}"}
    return {"cons": scale_cons(f["consumo"], k), "costo": f["spesa_vendita"] * k,
            "metodo": "spesa di vendita del periodo e consumo del periodo, entrambi proiettati a 12 mesi (prezzi non dettagliati in bolletta)"}


def eligible(o: dict, cliente: dict, current_op: str | None) -> tuple[bool, list[str]]:
    """Esclude solo se un requisito e' VERIFICATAMENTE incompatibile; se non verificabile lo segnala."""
    r, notes = o.get("requisiti") or {}, []
    if r.get("residente") is not None:
        if cliente.get("residente") is None:
            notes.append("riservata a " + ("residenti" if r["residente"] else "non residenti") + ": da verificare")
        elif r["residente"] != cliente["residente"]:
            return False, []
    if r.get("regioni"):
        areas = {norm(x) for x in r["regioni"]}
        known = [norm(cliente[k]) for k in ("regione", "provincia") if cliente.get(k)]
        if any(x in areas for x in known):
            pass  # area confermata
        elif cliente.get("regione"):
            return False, []  # regione nota e non compresa: esclusa
        else:
            notes.append("limitata ad alcune aree: da verificare")  # solo provincia/nulla: non si puo' escludere
    if r.get("sdd"):
        if cliente.get("pagamento_sdd") is False:
            return False, []
        if cliente.get("pagamento_sdd") is None:
            notes.append("richiede domiciliazione bancaria")
    if r.get("solo_nuovi_clienti") and current_op and norm(o.get("operatore")) == norm(current_op):
        return False, []
    return True, notes


def _row(f: dict, operatore, offerta, costo: float, o=None, notes=(), keep=False) -> dict:
    risks = list(notes)
    if o:
        risks += list(o.get("rischi") or [])
        if o.get("durata_prezzo_mesi"):
            risks.append(f"prezzo valido {o['durata_prezzo_mesi']} mesi")
        if o.get("vincolo_mesi"):
            risks.append(f"vincolo {o['vincolo_mesi']} mesi")
    note = "; ".join(dict.fromkeys(risks)) or ("Nessuna offerta piu' conveniente: si mantiene quella attuale" if keep else "Nessun rischio segnalato")
    return {"id": f["id"], "commodity": f["commodity"], "offerta_attuale": f.get("offerta"),
            "costo_attuale_anno": round(f["base"]["costo"], 2), "operatore": operatore, "offerta": offerta,
            "costo_anno": round(costo, 2), "risparmio_anno": round(f["base"]["costo"] - costo, 2),
            "fonte": (o or {}).get("fonte_url"), "mantieni": keep, "note": note}


def _solution(nome: str, tipo: str, rows: list[dict], base_total: float) -> dict:
    cost = sum(r["costo_anno"] for r in rows)
    return {"nome": nome, "tipo": tipo, "costo_anno": round(cost, 2), "risparmio_anno": round(base_total - cost, 2) + 0.0,
            "cambia": any(not r["mantieni"] for r in rows), "consigliata": False, "righe": rows}


def energy_solutions(forn: list[dict], offers: list[dict], cliente: dict, current_op: str | None) -> dict:
    """Separata (migliore offerta per ogni commodity, o si resta dov'e') e Insieme (stesso fornitore / dual)."""
    base_total = sum(f["base"]["costo"] for f in forn)
    singles = {f["id"]: [] for f in forn}  # (costo, offerta, note)
    duals = []
    both = {f["commodity"] for f in forn} == {"luce", "gas"}
    for o in offers:
        ok, notes = eligible(o, cliente, current_op)
        if not ok:
            continue
        disc = o.get("sconto_anno") or 0
        if o.get("commodity") in ("luce", "gas"):
            for f in forn:
                if f["commodity"] == o["commodity"]:
                    c = part_cost(f["base"]["cons"], o.get(o["commodity"]))
                    if c is not None:
                        singles[f["id"]].append((max(0.0, c - disc), o, notes))
        elif o.get("commodity") == "dual" and both:
            parts = [(f, part_cost(f["base"]["cons"], o.get(f["commodity"]))) for f in forn]
            if all(c is not None for _, c in parts):
                gross = sum(c for _, c in parts)
                duals.append((max(0.0, gross - disc), gross, o, notes, parts))
    sols = []
    # separata: per ogni fornitura la migliore offerta, oppure si mantiene l'attuale
    rows = []
    for f in forn:
        best = min(singles[f["id"]], key=lambda x: x[0], default=None)
        if best and best[0] < f["base"]["costo"]:
            rows.append(_row(f, best[1]["operatore"], best[1]["nome"], best[0], best[1], best[2]))
        else:
            rows.append(_row(f, f.get("operatore_attuale"), f.get("offerta"), f["base"]["costo"], keep=True))
    sols.append(_solution("Separata (luce e gas dai fornitori piu' convenienti)" if both else "Offerta singola piu' conveniente",
                          "separata", rows, base_total))
    # insieme: stesso fornitore (offerte singole) o offerta dual; richiede di cambiare tutte le forniture
    if both:
        cands = []
        for op in {norm(o["operatore"]): o["operatore"] for lst in singles.values() for _, o, _ in lst}.values():
            rr = []
            for f in forn:
                best = min((x for x in singles[f["id"]] if norm(x[1]["operatore"]) == norm(op)), key=lambda x: x[0], default=None)
                if not best:
                    break
                rr.append(_row(f, op, best[1]["nome"], best[0], best[1], best[2]))
            else:
                cands.append((f"Insieme: tutto da {op}", rr))
        for total, gross, o, notes, parts in duals:
            factor = total / gross if gross else 1
            cands.append((f"Insieme: {o['operatore']} - {o['nome']} (offerta dual)",
                          [_row(f, o["operatore"], o["nome"], c * factor, o, notes) for f, c in parts]))
        if cands:
            nome, rr = min(cands, key=lambda c: sum(r["costo_anno"] for r in c[1]))
            sols.append(_solution(nome, "insieme", rr, base_total))
    best = min(sols, key=lambda x: x["costo_anno"])
    if best["cambia"] and best["risparmio_anno"] > 0:
        best["consigliata"] = True
    return {"base_total": base_total, "soluzioni": sols, "singles": singles, "n_duals": len(duals)}


def build_outcome_energy(res: dict, forn: list[dict], currency: str) -> dict:
    """{'stato': 'ok'} oppure il motivo per cui non c'e' un risparmio (pop-up del sito)."""
    if any(s["consigliata"] for s in res["soluzioni"]):
        return {"stato": "ok"}
    m = lambda x: money_it(x, currency)
    n_cand = sum(len(v) for v in res["singles"].values()) + res["n_duals"]
    if n_cand == 0:
        return {"stato": "nessuna_offerta", "titolo": "Nessuna offerta con prezzo verificabile",
                "messaggio": "Non ho trovato offerte attuali di cui sia determinabile il prezzo di oggi per le tue forniture: "
                             "non posso indicare un risparmio (non stimo prezzi).", "dettagli": [], "passi": STEPS}
    det = []
    for f in forn:
        best = min(res["singles"][f["id"]], key=lambda x: x[0], default=None)
        if best:
            det.append(f"{f['commodity'].capitalize()}: la migliore alternativa ({best[1]['operatore']} - {best[1]['nome']}) "
                       f"costa {m(best[0])}/anno contro {m(f['base']['costo'])} della tua bolletta")
    return {"stato": "non_conveniente", "titolo": "Nessun risparmio possibile",
            "messaggio": "A parita' di consumi, le condizioni della tua bolletta sono ancora piu' convenienti di tutte le "
                         "offerte di oggi che ho potuto verificare.", "dettagli": det, "passi": STEPS}


def analyze_energy(meta: dict, forn: list[dict], cliente: dict, failed: int):
    country = (request.form.get("paese") or meta.get("paese") or "").strip()
    if not country:
        return jsonify(errore="Paese non rilevato dalla bolletta: indicalo nel campo 'Paese' e riprova.")
    currency, current_op = meta.get("valuta") or "EUR", meta.get("operatore")
    warn, righe = [], []
    for f in forn:
        base = energy_base(f)
        if not base:
            warn.append(f"{f['id']}: periodo o consumo non leggibili, fornitura esclusa dal confronto.")
            continue
        f["base"], f["operatore_attuale"] = base, current_op
        righe.append(f)
        al = (f.get("periodo") or {}).get("al")
        if al and (date.today() - date.fromisoformat(al)).days > 180:
            warn.append(f"La bolletta {f['id']} e' del {al}: il confronto usa le condizioni di quella bolletta, che "
                        "potrebbero non essere piu' le tue.")
    if not righe:
        return jsonify(errore="Dati insufficienti per confrontare la bolletta.")
    if failed:
        warn.append(f"{failed} blocchi della bolletta non sono stati letti: i dati potrebbero essere incompleti.")
    cache, from_cache = get_offers(country, current_op, "energia")
    if (cache.get("valuta") or currency) != currency:
        raise ValueError(f"Bolletta in {currency} e offerte in {cache.get('valuta')}: importi non confrontabili.")
    res = energy_solutions(righe, cache["offerte"], cliente, current_op)
    if cache["non_rilevati"]:
        warn.append("Offerte non rilevate per: " + ", ".join(cache["non_rilevati"]) + ". Il confronto potrebbe essere incompleto.")
    esito = build_outcome_energy(res, righe, currency)
    rec = next((s for s in res["soluzioni"] if s["consigliata"]), None)
    m = lambda x: money_it(x, currency)
    text = [f"A parita' di consumi, con le condizioni della tua bolletta la spesa di vendita e' {m(res['base_total'])}/anno "
            "(IVA esclusa; trasporto, oneri e imposte sono uguali per tutti i fornitori)."]
    text += [f"{s['nome']}: {m(s['costo_anno'])}/anno, risparmio {m(s['risparmio_anno'])}/anno." for s in res["soluzioni"]]
    text.append(f"Consigliata: {rec['nome']}." if rec else esito.get("messaggio", ""))
    return jsonify({"tipo": "energia", "valuta": currency, "operatore_attuale": current_op, "attuale_anno": round(res["base_total"], 2),
                    "forniture": [{"id": f["id"], "commodity": f["commodity"], "offerta_attuale": f.get("offerta"),
                                   "periodo": f.get("periodo"), "consumo_annuo": round(f["base"]["cons"]["totale"], 2),
                                   "unita": "kWh" if f["commodity"] == "luce" else "Smc",
                                   "costo_attuale_anno": round(f["base"]["costo"], 2), "metodo": f["base"]["metodo"]} for f in righe],
                    "soluzioni": res["soluzioni"], "raccomandazione": rec, "esito": esito, "avvisi": warn,
                    "offerte_info": {"paese": country, "data": cache["data"], "da_cache": from_cache,
                                     "operatori": cache["operatori"], "non_rilevati": cache["non_rilevati"]},
                    "sintesi": "\n".join(text)})


# ---------------------------------------------------------------- 5) endpoint
@app.post("/analyze")
def analyze():
    try:
        tasks = [t for f in request.files.getlist("f") for t in file_tasks(f)]
        if not tasks:
            return jsonify(errore="Nessun file caricato.")
        with ThreadPoolExecutor(WORKERS) as pool:
            results = list(pool.map(extract_chunk, tasks))
        failed = results.count(None)
        good = [r for r in results if r]
        meta, lines, discounts = merge(good)
        forn, cliente = merge_energy(good)
        if forn and not lines:
            return analyze_energy(meta, forn, cliente, failed)
        if not lines:
            return jsonify(errore="Nessuna linea voce/dati/fisso trovata nella fattura.")
        country = (request.form.get("paese") or meta.get("paese") or "").strip()
        if not country:
            return jsonify(errore="Paese non rilevato dalla fattura: indicalo nel campo 'Paese' e riprova.")
        unassigned = apply_discounts(lines, discounts, months_in(meta))
        if CONSOLIDA_FONIA_FISSA:
            lines = consolidate_voice_lines(lines)
        currency = meta.get("valuta") or "EUR"

        cache, from_cache = get_offers(country, meta.get("operatore"))
        offers = convert_offers(cache, currency, meta.get("cambio_eur"))

        scenarios = [build_scenario(lines, [o for o in offers if o["operatore"] == op], op)
                     for op in sorted({o["operatore"] for o in offers})]
        scenarios.sort(key=lambda s: (not s["completo"], -s["risparmio_anno"]))
        scenarios = scenarios[:6] + [build_scenario(lines, offers, "Mix migliore (piu' contratti)")]

        out = {"valuta": currency, "operatore_attuale": meta.get("operatore"),
               "attuale_anno": round(sum(l["costo"] * 12 for l in lines), 2),
               "sconti_non_attribuiti": unassigned, "blocchi_non_letti": failed, "scenari": scenarios,
               "offerte_info": {"paese": country, "data": cache["data"], "da_cache": from_cache,
                                "operatori": cache["operatori"], "non_rilevati": cache["non_rilevati"]}}
        out["raccomandazione"] = recommend(scenarios, lines, out["attuale_anno"])
        out["esito"] = build_outcome(out["raccomandazione"], scenarios, offers, currency, country)
        brief = {k: out[k] for k in ("valuta", "attuale_anno", "sconti_non_attribuiti", "blocchi_non_letti")}
        brief["operatori_non_rilevati"] = cache["non_rilevati"]
        brief["raccomandazione"], brief["esito"] = out["raccomandazione"], out["esito"]
        brief["scenari"] = [{k: s[k] for k in ("nome", "completo", "risparmio_anno", "rating_medio", "rischi", "piani")}
                            for s in scenarios]
        brief["fonia_fissa"] = [
            {"numero": r["numero"], "canali": r["canali"], "oggi_anno": r["costo_attuale_anno"],
             "nuovo_anno": r["costo_nuovo_anno"], "piano_nuovo": r["piano_nuovo"],
             "destinazioni": [{k: d[k] for k in ("destinazione", "minuti_mese", "tariffa_oggi", "tariffa_nuova")}
                              for d in r["destinazioni"]]}
            for r in scenarios[-1]["linee"] if r["destinazioni"]][:10]
        out["sintesi"] = ask(SMART, SUMMARY, json.dumps(brief, ensure_ascii=False)[:60000], max_tokens=1000)
        return jsonify(out)
    except NoOffers as e:  # rilevamento offerte fallito: nessuna offerta concreta -> pop-up sul sito
        return jsonify(esito={"stato": "nessuna_offerta", "titolo": "Nessuna offerta concreta trovata",
                              "messaggio": str(e), "dettagli": [], "passi": [
                                  "Riprova tra qualche minuto (puo' essere un problema temporaneo di ricerca o di limiti API).", *STEPS]})
    except Exception as e:
        return jsonify(errore=f"{type(e).__name__}: {e}")


# ---------------------------------------------------------------- 6) interfaccia web
PAGE = r"""<!doctype html><html lang="it"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Analizzatore fatture telecom</title>
<style>
:root{--bg:#f6f7f5;--fg:#16201c;--mut:#5d6b64;--ac:#0b6e4f;--ln:#d5dcd7;--bad:#b3261e}
@media(prefers-color-scheme:dark){:root{--bg:#121714;--fg:#e8efe9;--mut:#9aa9a0;--ac:#4fc79a;--ln:#2a352f;--bad:#ff8a80}}
body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.5 system-ui,sans-serif}
main{max-width:1200px;margin:auto;padding:24px 16px}h1{font-size:1.5rem;margin:0 0 4px}h2{font-size:1.1rem;margin:28px 0 4px}
.sub,.info{color:var(--mut);margin:0 0 20px}.info{margin:8px 0}button,select,input{font:inherit}
input[type=text]{width:12em;padding:6px 8px;border:1px solid var(--ln);border-radius:6px;background:var(--bg);color:var(--fg)}
button{background:var(--ac);color:var(--bg);border:0;padding:8px 16px;border-radius:6px;cursor:pointer}
.box{overflow-x:auto}table{border-collapse:collapse;width:100%}
th,td{border-bottom:1px solid var(--ln);padding:6px 8px;text-align:left;vertical-align:top;white-space:nowrap}
td.w{white-space:normal;min-width:240px}dialog{max-width:560px;width:calc(100% - 32px);border:2px solid var(--ac);border-radius:10px;background:var(--bg);color:var(--fg);padding:20px 24px}
dialog::backdrop{background:rgba(0,0,0,.45)}dialog h2{margin-top:0}dialog ul{padding-left:20px}
.rec{border:2px solid var(--ac);border-radius:8px;padding:12px 16px;margin:12px 0}.bad{color:var(--bad)}.warn{color:var(--bad);margin:6px 0}
pre{white-space:pre-wrap;font:inherit;background:var(--ln);padding:12px;border-radius:6px}a{color:var(--ac)}
</style>
<main><h1>Analizzatore fatture telecom</h1>
<p class="sub">Fatture di telefonia (SIM voce/dati, fisso, fonia fissa a consumo) oppure bollette di luce e gas. Importi IVA esclusa. Carica PDF o foto (JPG/PNG), anche piu' file della stessa fattura.</p>
<form id="f"><input type="file" name="f" accept=".pdf,.jpg,.jpeg,.png" multiple required>
<input type="text" name="paese" placeholder="Paese (se non rilevato)"> <button>Analizza</button></form>
<p id="st"></p><div id="out"></div>
<dialog id="dlg"><h2 id="dlg-t"></h2><p id="dlg-m"></p><ul id="dlg-d"></ul><p><b>Cosa puoi fare</b></p><ul id="dlg-p"></ul>
<form method="dialog"><button>Ho capito</button></form></dialog>
<script>
const $ = s => document.querySelector(s);
const esc = s => String(s ?? "").replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
let D, S = 0, SE = 0;
const money = x => x == null ? "—" : x.toLocaleString("it-IT", {style: "currency", currency: D.valuta});
const table = (head, rows) => `<div class="box"><table><tr>${head.map(h => `<th>${h}</th>`).join("")}</tr>` +
  rows.map(r => `<tr>${r.map(c => `<td${/^<span class="w">/.test(c) ? ' class="w"' : ""}>${c}</td>`).join("")}</tr>`).join("") + "</table></div>";
const wide = t => `<span class="w">${esc(t)}</span>`;
const link = u => u ? `<a href="${esc(u)}" target="_blank" rel="noopener">fonte</a>` : "—";
const rating = r => r == null ? "—" : r <= 5 ? `<b class="bad">${r}/10</b>` : `<b>${r}/10</b>`;

$("#f").onsubmit = async ev => {
  ev.preventDefault();
  $("#st").textContent = "Elaborazione in corso (la prima fattura del giorno rileva anche le offerte: alcuni minuti in piu')...";
  $("#out").innerHTML = "";
  let d;
  try { d = await (await fetch("/analyze", {method: "POST", body: new FormData(ev.target)})).json(); }
  catch (e) { $("#st").textContent = "Errore di connessione con lo script: verifica che sia in esecuzione."; return; }
  if (d.errore) { $("#st").textContent = d.errore; return; }
  $("#st").textContent = "";
  if (d.tipo === "energia") { D = d; SE = 0; drawEnergy(); }
  else if (d.scenari) { D = d; S = 0; draw(); }
  if (d.esito && d.esito.stato !== "ok") showOutcome(d.esito);   // nessuna offerta concreta con risparmio: pop-up
};

function showOutcome(e) {
  const list = a => (a || []).map(x => `<li>${esc(x)}</li>`).join("");
  $("#dlg-t").textContent = e.titolo;
  $("#dlg-m").textContent = e.messaggio;
  $("#dlg-d").innerHTML = list(e.dettagli);
  $("#dlg-p").innerHTML = list(e.passi);
  $("#dlg").showModal();
}

function draw() {
  const s = D.scenari[S], i = D.offerte_info, warn = [];
  const lost = (D.sconti_non_attribuiti || []).reduce((a, x) => a + x.importo_anno, 0);
  if (lost) warn.push(`Sconti non legati a una utenza o a un piano (non applicati): ${money(lost)}/anno. Il risparmio potrebbe essere sovrastimato fino a questa cifra.`);
  if (D.blocchi_non_letti) warn.push(`${D.blocchi_non_letti} blocchi della fattura non sono stati letti: i dati potrebbero essere incompleti.`);
  if (i.non_rilevati.length) warn.push(`Offerte non rilevate per: ${i.non_rilevati.join(", ")}. Il confronto potrebbe essere incompleto.`);
  $("#out").innerHTML =
    recCard() + warn.map(w => `<p class="warn">${esc(w)}</p>`).join("") + `<pre>${esc(D.sintesi)}</pre>` +
    `<p class="info">Offerte del ${esc(i.data)} (${i.da_cache ? "da cache" : "rilevate ora"}) - ${esc(i.paese)}: ${i.operatori.length} operatori analizzati.</p>` +
    `<h2>Scenari</h2><p>Costo attuale: <b>${money(D.attuale_anno)}</b>/anno. Scenario mostrato: <select onchange="S=+this.value;draw()">` +
    D.scenari.map((x, k) => `<option value="${k}" ${k == S ? "selected" : ""}>${esc(x.nome)}</option>`).join("") + `</select> ` +
    `<button type="button" onclick="csv()">Scarica CSV</button> ` +
    (destRows(s).length ? `<button type="button" onclick="csvDest()">CSV tariffe per destinazione</button>` : "") + `</p>` +
    table(["Scenario", "Utenze coperte", "Costo nuovo/anno", "Risparmio/anno", "Rating medio"],
      D.scenari.map(x => [esc(x.nome) + (x.completo ? "" : " (parziale)"), `${x.coperte}/${x.linee.length}`,
        money(x.costo_anno), money(x.risparmio_anno), rating(x.rating_medio)])) +
    `<h2>Nuovi piani proposti - ${esc(s.nome)}</h2>` +
    table(["Operatore", "Piano", "Utenze", "Costo nuovo/anno", "Risparmio/anno", "Rating medio", "Fonte"],
      s.piani.map(p => [esc(p.operatore), esc(p.piano), p.utenze, money(p.costo_nuovo_anno), money(p.risparmio_anno), rating(p.rating_medio), link(p.fonte)])) +
    voiceTable(s) + `<h2>Rischi sulla qualita' del servizio</h2><p>${s.rischi.length ? esc(s.rischi.join(" | ")) : "Nessuno segnalato: verifica comunque copertura e SLA."}</p>` +
    `<h2>Dettaglio per utenza</h2>` +
    table(["Utenza", "Tipo", "Piano attuale", "Fonia fissa", "Costo attuale/anno", "Nuovo operatore", "Nuovo piano", "Costo nuovo/anno", "Risparmio/anno", "Rating", "Rischi qualita'"],
      s.linee.map(l => [esc(l.numero), l.tipo, esc(l.piano), l.canali ? `${l.canali} canali · ${l.minuti_mese} min/mese` : "—", money(l.costo_attuale_anno), esc(l.operatore) || "—", esc(l.piano_nuovo) || "—",
        money(l.costo_nuovo_anno), `<span class="${l.risparmio_anno < 0 ? "bad" : ""}">${money(l.risparmio_anno)}</span>`, rating(l.rating), wide(l.rischi)]));
}

function recCard() {
  const r = D.raccomandazione;
  if (!r) return `<div class="rec"><b>Nessuna offerta piu' conveniente e adeguata al consumo rilevato: conviene restare con l'operatore attuale o rinegoziare.</b></div>`;
  return `<div class="rec"><h2 style="margin-top:0">Offerta consigliata: ${esc(r.nome)}</h2>` +
    `<p>Costo <b>${money(r.costo_anno)}</b>/anno invece di ${money(r.costo_attuale_anno)}: risparmio <b>${money(r.risparmio_anno)}</b>/anno - rating medio <b>${r.rating_medio}/10</b></p>` +
    `<p class="info">Consumo rilevato in fattura: ${esc(r.consumo)}</p>` + (r.nota ? `<p class="warn">${esc(r.nota)}</p>` : "") +
    table(["Operatore", "Piano", "Utenze", "Costo nuovo/anno", "Risparmio/anno", "Rating medio", "Fonte"],
      r.piani.map(p => [esc(p.operatore), esc(p.piano), p.utenze, money(p.costo_nuovo_anno), money(p.risparmio_anno), rating(p.rating_medio), link(p.fonte)])) +
    `<p><b>Rischi sulla qualita':</b> ${r.rischi.length ? esc(r.rischi.join(" | ")) : "nessuno segnalato (verifica copertura e SLA)"}</p></div>`;
}

const enTable = rows => table(["Fornitura", "Oggi", "Costo oggi/anno", "Nuova offerta", "Costo nuovo/anno", "Risparmio/anno", "Note e rischi", "Fonte"],
  rows.map(r => [esc(r.commodity) + " " + esc(r.id), esc(r.offerta_attuale) || "—", money(r.costo_attuale_anno),
    r.mantieni ? "<i>resta com'e'</i>" : esc(r.operatore) + " - " + esc(r.offerta), money(r.costo_anno),
    `<span class="${r.risparmio_anno < 0 ? "bad" : ""}">${money(r.risparmio_anno)}</span>`, `<span class="w">${esc(r.note)}</span>`, link(r.fonte)]));

function recCardEn() {
  const r = D.raccomandazione;
  if (!r) return `<div class="rec"><b>Nessuna offerta piu' conveniente delle condizioni della tua bolletta, a parita' di consumi.</b></div>`;
  return `<div class="rec"><h2 style="margin-top:0">Soluzione consigliata: ${esc(r.nome)}</h2>` +
    `<p>Costo <b>${money(r.costo_anno)}</b>/anno invece di ${money(D.attuale_anno)}: risparmio <b>${money(r.risparmio_anno)}</b>/anno (IVA esclusa, solo parte vendita)</p>` +
    enTable(r.righe) + `</div>`;
}

function drawEnergy() {
  const i = D.offerte_info, s = D.soluzioni[SE];
  $("#out").innerHTML = recCardEn() + (D.avvisi || []).map(w => `<p class="warn">${esc(w)}</p>`).join("") + `<pre>${esc(D.sintesi)}</pre>` +
    `<p class="info">Offerte del ${esc(i.data)} (${i.da_cache ? "da cache" : "rilevate ora"}) - ${esc(i.paese)}: ${i.operatori.length} fornitori analizzati.</p>` +
    `<h2>Soluzioni a confronto (stessi consumi della bolletta)</h2>` +
    table(["Soluzione", "Costo/anno", "Risparmio/anno", ""], D.soluzioni.map(x => [esc(x.nome), money(x.costo_anno), money(x.risparmio_anno), x.consigliata ? "<b>consigliata</b>" : ""])) +
    `<p>Dettaglio: <select onchange="SE=+this.value;drawEnergy()">${D.soluzioni.map((x, k) => `<option value="${k}" ${k == SE ? "selected" : ""}>${esc(x.nome)}</option>`).join("")}</select> ` +
    `<button type="button" onclick="csvEn()">Scarica CSV</button></p>` + enTable(s.righe) +
    `<h2>Base di calcolo (dalla bolletta)</h2>` +
    table(["Fornitura", "Offerta in bolletta", "Periodo", "Consumo annuo usato", "Costo vendita/anno", "Metodo"],
      D.forniture.map(f => [esc(f.commodity) + " " + esc(f.id), esc(f.offerta_attuale) || "—", f.periodo ? esc(f.periodo.dal + " / " + f.periodo.al) : "—",
        f.consumo_annuo.toLocaleString("it-IT") + " " + f.unita, money(f.costo_attuale_anno), `<span class="w">${esc(f.metodo)}</span>`]));
}

function csvEn() {
  const H = ["Fornitura", "Tipo", "Offerta attuale", "Costo attuale/anno", "Nuovo operatore", "Nuova offerta", "Costo nuovo/anno", "Risparmio/anno", "Note", "Fonte"];
  download("confronto_energia.csv", H, D.soluzioni[SE].righe.map(r => [r.id, r.commodity, r.offerta_attuale, r.costo_attuale_anno, r.mantieni ? "" : r.operatore, r.mantieni ? "" : r.offerta, r.costo_anno, r.risparmio_anno, r.note, r.fonte]));
}

const rate3 = x => x == null ? "—" : x.toLocaleString("it-IT", {style: "currency", currency: D.valuta, minimumFractionDigits: 3, maximumFractionDigits: 4});
const destRows = sc => sc.linee.flatMap(l => l.destinazioni.map(d => [l.numero, d]));

function voiceTable(sc) {
  const rows = destRows(sc);
  if (!rows.length) return "";
  return `<h2>Fonia fissa a consumo: tariffe per destinazione - ${esc(sc.nome)}</h2>` +
    table(["Utenza", "Destinazione", "Chiamate/mese", "Minuti/mese", "Costo oggi/mese", "Tariffa oggi/min", "Tariffa nuova/min", "Costo nuovo/mese"],
      rows.map(([n, d]) => [esc(n), esc(d.destinazione), d.chiamate_mese, d.minuti_mese, money(d.costo_oggi_mese),
        rate3(d.tariffa_oggi), rate3(d.tariffa_nuova), money(d.costo_nuovo_mese)]));
}

function download(name, head, rows) {
  const text = "\ufeff" + [head, ...rows].map(r => r.map(c => '"' + String(c ?? "").replace(/"/g, '""') + '"').join(";")).join("\n");
  const a = document.createElement("a");
  a.href = URL.createObjectURL(new Blob([text], {type: "text/csv"}));
  a.download = name; a.click();
}

function csv() {
  const H = ["Utenza", "Tipo", "Piano attuale", "Canali", "Minuti/mese", "Costo attuale/anno", "Nuovo operatore", "Nuovo piano", "Costo nuovo/anno", "Risparmio/anno", "Rating", "Rischi", "Fonte"];
  download("proposta_telecom.csv", H, D.scenari[S].linee.map(l => [l.numero, l.tipo, l.piano_attuale, l.canali, l.minuti_mese, l.costo_attuale_anno, l.operatore, l.piano_nuovo, l.costo_nuovo_anno, l.risparmio_anno, l.rating, l.rischi, l.fonte]));
}

function csvDest() {
  const H = ["Utenza", "Destinazione", "Chiamate/mese", "Minuti/mese", "Costo oggi/mese", "Tariffa oggi/min", "Tariffa nuova/min", "Costo nuovo/mese"];
  download("tariffe_per_destinazione.csv", H, destRows(D.scenari[S]).map(([n, d]) => [n, d.destinazione, d.chiamate_mese, d.minuti_mese, d.costo_oggi_mese, d.tariffa_oggi, d.tariffa_nuova, d.costo_nuovo_mese]));
}
</script></main>"""


@app.get("/")
def index():
    return PAGE


if __name__ == "__main__":
    app.run(port=5000, debug=False)
